-module(nodo).
-include_lib("stdlib/include/qlc.hrl").

-export([start_system/1, new_erlang_node/1, node_behavior/1, print_all/0, all/0]).

% Gestione dei record per la tabella di bootstrap
-record(bootstrap_table, {id, pid, last_ping}).
% Record per la cache dei risultati
-record(result_cache, {target_id, found_id, found_pid, timestamp}).

% Costanti
-define(MAX_HOPS, 10).        % Massimo numero di hop per ricerca
-define(CACHE_TIMEOUT, 300).  % Timeout cache in secondi (5 minuti)
-define(K_BUCKET_SIZE, 4).    % Dimensione massima dei k-bucket

% Creazione di un nuovo nodo
new_erlang_node(P) ->
    Storage = generate_random_string(16),
    Key = crypto:hash(sha256, Storage),
    InitialValues = {"idTMP", [[Key, Storage]], [], 0, [], erlang:system_time(second)},
    Pid = spawn(fun() -> node_behavior(InitialValues) end),
    P ! {ok, Pid}.

% Funzione per generare una stringa casuale di lunghezza N
generate_random_string(N) ->
    lists:map(fun(_) -> rand:uniform(26) + $a - 1 end, lists:seq(1, N)).

% Definizione del comportamento di un nodo kademlia generico
node_behavior({Id, Storage, K_buckets, Timer, Cache, LastCleanup}) ->
    % Pulizia della cache se necessario
    CurrentTime = erlang:system_time(second),
    {NewCache, NewLastCleanup} = case CurrentTime - LastCleanup > ?CACHE_TIMEOUT of
        true -> {clean_cache(Cache, CurrentTime), CurrentTime};
        false -> {Cache, LastCleanup}
    end,

    receive
        {getInfo} ->
            io:format("Id: ~p~nStorage: ~p~nK_buckets: ~p~nTimer: ~p~nCache size: ~p~n", [
                Id, Storage, K_buckets, Timer, length(NewCache)
            ]),
            node_behavior({Id, Storage, K_buckets, Timer, NewCache, NewLastCleanup});

        % Gestione del ping
        {ping, TargetId, From, HopCount} when HopCount =< ?MAX_HOPS ->
            % Controlla prima nella cache
            case check_cache(TargetId, NewCache, CurrentTime) of
                {found, FoundId, FoundPid} ->
                    From ! {ping_result, FoundId, FoundPid, cached},
                    node_behavior({Id, Storage, K_buckets, Timer, NewCache, NewLastCleanup});
                not_found ->
                    case lists:keyfind(TargetId, 2, K_buckets) of
                        false ->
                            ClosestNode = find_closest(TargetId, K_buckets),
                            case ClosestNode of
                                undefined ->
                                    From ! {ping_result, not_found, HopCount};
                                {_, _, ClosestPid, _} ->
                                    ClosestPid ! {ping, TargetId, From, HopCount + 1}
                            end;
                        {_, FoundId, FoundPid, _} ->
                            % Aggiorna la cache con il risultato
                            UpdatedCache = update_cache(TargetId, FoundId, FoundPid, CurrentTime, NewCache),
                            From ! {ping_result, FoundId, FoundPid, HopCount},
                            node_behavior({Id, Storage, K_buckets, Timer, UpdatedCache, NewLastCleanup})
                    end
            end,
            node_behavior({Id, Storage, K_buckets, Timer, NewCache, NewLastCleanup});

        {ping, _, From, HopCount} ->
            From ! {ping_result, not_found, HopCount},
            node_behavior({Id, Storage, K_buckets, Timer, NewCache, NewLastCleanup});

        % Gestione del find_node
        {find_node, TargetId, From, HopCount} when HopCount =< ?MAX_HOPS ->
            % Controlla prima nella cache
            case check_cache(TargetId, NewCache, CurrentTime) of
                {found, FoundId, FoundPid} ->
                    From ! {find_result, FoundId, FoundPid, cached},
                    node_behavior({Id, Storage, K_buckets, Timer, NewCache, NewLastCleanup});
                not_found ->
                    case lists:keyfind(TargetId, 2, K_buckets) of
                        false ->
                            ClosestNode = find_closest(TargetId, K_buckets),
                            case ClosestNode of
                                undefined ->
                                    From ! {find_result, not_found, HopCount};
                                {_, _, ClosestPid, _} ->
                                    ClosestPid ! {find_node, TargetId, From, HopCount + 1}
                            end;
                        {_, FoundId, FoundPid, _} ->
                            % Aggiorna la cache con il risultato
                            UpdatedCache = update_cache(TargetId, FoundId, FoundPid, CurrentTime, NewCache),
                            From ! {find_result, FoundId, FoundPid, HopCount},
                            node_behavior({Id, Storage, K_buckets, Timer, UpdatedCache, NewLastCleanup})
                    end
            end,
            node_behavior({Id, Storage, K_buckets, Timer, NewCache, NewLastCleanup});

        {find_node, _, From, HopCount} ->
            From ! {find_result, not_found, HopCount},
            node_behavior({Id, Storage, K_buckets, Timer, NewCache, NewLastCleanup});

        % Gestione dello storage
        {store, Value} ->
            Key = crypto:hash(sha256, Value),
            NewStorage = [[Key, Value] | Storage],
            node_behavior({Id, NewStorage, K_buckets, Timer, NewCache, NewLastCleanup});

        % Aggiornamento buckets
        {refresh, {Idx, Lista}} ->
            UpdatedBuckets = update_k_buckets(Lista, K_buckets, ?K_BUCKET_SIZE),
            node_behavior({Idx, Storage, UpdatedBuckets, Timer, NewCache, NewLastCleanup});

        {newBuckets, Lista} ->
            UpdatedBuckets = update_k_buckets(Lista, K_buckets, ?K_BUCKET_SIZE),
            node_behavior({Id, Storage, UpdatedBuckets, Timer, NewCache, NewLastCleanup});

        % Messaggio generico
        _ ->
            node_behavior({Id, Storage, K_buckets, Timer, NewCache, NewLastCleanup})
    end.

% Funzione per aggiornare i k-buckets mantenendo la dimensione massima
update_k_buckets(NewNodes, ExistingBuckets, MaxSize) ->
    % Unisce i nuovi nodi con quelli esistenti
    Combined = NewNodes ++ ExistingBuckets,
    % Rimuove i duplicati basandosi sull'ID del nodo
    Unique = lists:usort(fun({_, Id1, _, _}, {_, Id2, _, _}) -> Id1 =< Id2 end, Combined),
    % Prende solo i primi MaxSize elementi
    lists:sublist(Unique, MaxSize).

% Funzioni per la gestione della cache
clean_cache(Cache, CurrentTime) ->
    lists:filter(
        fun({_, _, _, Timestamp}) ->
            CurrentTime - Timestamp =< ?CACHE_TIMEOUT
        end,
        Cache
    ).

check_cache(TargetId, Cache, CurrentTime) ->
    case lists:keyfind(TargetId, 1, Cache) of
        {_, FoundId, FoundPid, Timestamp} when CurrentTime - Timestamp =< ?CACHE_TIMEOUT ->
            {found, FoundId, FoundPid};
        _ ->
            not_found
    end.

update_cache(TargetId, FoundId, FoundPid, CurrentTime, Cache) ->
    % Rimuove eventuale entry vecchia
    CleanedCache = lists:keydelete(TargetId, 1, Cache),
    % Aggiunge la nuova entry
    [{TargetId, FoundId, FoundPid, CurrentTime} | CleanedCache].

% Funzione per trovare il nodo più vicino
find_closest(TargetId, K_buckets) ->
    case K_buckets of
        [] -> undefined;
        _ ->
            Sorted = lists:sort(
                fun({Distance1, _, _, _}, {Distance2, _, _, _}) ->
                    Distance1 < Distance2
                end,
                [{TargetId bxor Id, Id, Pid, LastPing} || {_, Id, Pid, LastPing} <- K_buckets]
            ),
            hd(Sorted)
    end.

% Inizializzazione della rete Kademlia
start_system(P) ->
    mnesia:create_schema([node()]),
    mnesia:start(),
    mnesia:create_table(bootstrap_table, [
        {attributes, record_info(fields, bootstrap_table)},
        {type, set},
        {disc_copies, [node()]}
    ]),
    NodeId = rand:uniform(1 bsl 160 - 1),
    Pid = spawn(fun() -> bootstrap_node_loop(NodeId) end),
    P ! {ok, Pid}.

% Comportamento del nodo bootstrap
bootstrap_node_loop(Id) ->
    io:format("Nodo bootstrap pronto con ID: ~p~n", [Id]),
    receive
        {ping, From} ->
            io:format("[[--BOOTSTRAP--]] --> Ping ricevuto da: ~p~n", [From]),
            bootstrap_node_loop(Id);

        {enter, From} ->
            io:format("[[--BOOTSTRAP--]] --> Richiesta di entrare nella rete da: ~p~n", [From]),
            NodeId = rand:uniform(1 bsl 160 - 1),
            case mnesia:transaction(fun() ->
                mnesia:write(#bootstrap_table{id = NodeId, pid = From, last_ping = 0})
            end) of
                {atomic, ok} ->
                    io:format("[[--BOOTSTRAP--]] Nodo aggiunto con successo: ~p~n", [NodeId]),
                    Buckets = get_4_buckets(NodeId),
                    From ! {refresh, {NodeId, Buckets}},
                    AllNodes = all(),
                    case length(AllNodes) of
                        1 ->
                            io:format("Lista contiene un solo elemento~n");
                        _ ->
                            lists:foreach(
                                fun({Id_tmp, Pid, _}) ->
                                    Buckets_tmp = get_4_buckets(Id_tmp),
                                    Pid ! {newBuckets, Buckets_tmp}
                                end,
                                AllNodes
                            )
                    end;
                {aborted, Reason} ->
                    io:format("[[--BOOTSTRAP--]] Errore durante l'aggiunta del nodo: ~p~n", [Reason])
            end,
            bootstrap_node_loop(Id);

        {print} ->
            print_all(),
            bootstrap_node_loop(Id);

        _ ->
            io:format("[[--BOOTSTRAP--]] --> Messaggio sconosciuto."),
            bootstrap_node_loop(Id)
    end.

% Funzioni di utility per la gestione della tabella bootstrap
print_all() ->
    Print = fun(#bootstrap_table{id = Id, pid = Pid, last_ping = L}, Acc) ->
        Acc ++ [{Id, Pid, L}]
    end,
    Tran = fun() -> mnesia:foldr(Print, [], bootstrap_table) end,
    {atomic, Res} = mnesia:transaction(Tran),
    lists:foreach(
        fun({Id, Pid, L}) ->
            io:format(" id: ~p, pid: ~p, last ping: ~p~n", [Id, Pid, L])
        end,
        Res
    ).

all() ->
    Print = fun(#bootstrap_table{id = Id, pid = Pid, last_ping = L}, Acc) ->
        Acc ++ [{Id, Pid, L}]
    end,
    Tran = fun() -> mnesia:foldr(Print, [], bootstrap_table) end,
    {atomic, Res} = mnesia:transaction(Tran),
    Res.

% Funzione per ottenere i k-bucket più vicini
get_4_buckets(NodeId) ->
    Tran = fun() ->
        mnesia:foldr(
            fun(#bootstrap_table{id = Id, pid = Pid, last_ping = L}, Acc) ->
                case Id == NodeId of
                    true -> Acc;
                    false ->
                        Distance = NodeId bxor Id,
                        Acc ++ [{Distance, Id, Pid, L}]
                end
            end,
            [],
            bootstrap_table
        )
    end,
    {atomic, AllRecords} = mnesia:transaction(Tran),
    SortedRecords = lists:sort(fun({D1, _, _, _}, {D2, _, _, _}) -> D1 < D2 end, AllRecords),
    lists:sublist(SortedRecords, ?K_BUCKET_SIZE).