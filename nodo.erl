% interfaccia per gestire i nodi kademlia

-module(nodo).
-include_lib("stdlib/include/qlc.hrl").

% Costanti

% Massimo numero di hop per ricerca
-define(MAX_HOPS, 10).

-export([start_system/1, new_erlang_node/1, node_behavior/1, print_all/0, all/0]).
% gestione dei record per la tebella di bostrap
-record(bootstrap_table, {id, pid, last_ping}).

% creazione di un nuovo nodo
new_erlang_node(P) ->
    % valore casuale (stringa)
    Storage = generate_random_string(5),
    %Key = crypto:hash(sha256, Storage), NON SI USA SOLO PER IL TEST
    Key = generate_random_string(3),
    InitialValues = {"idTMP", [[Key, Storage]], [], 0},
    Pid = spawn(fun() -> node_behavior(InitialValues) end),
    P ! {ok, Pid}.

% Funzione per generare una stringa casuale di lunghezza N
generate_random_string(N) ->
    lists:map(fun(_) -> rand:uniform(26) + $a - 1 end, lists:seq(1, N)).

% definizione del comportamento di un nodo kademlia generico
node_behavior({Id, Storage, K_buckets, Timer}) ->
    receive
        {getInfo} ->
            io:format("Id: ~p - Storage: ~p - K_buckets: ~p - Timer: ~p. ", [
                Id, Storage, K_buckets, Timer
            ]),
            node_behavior({Id, Storage, K_buckets, Timer});
        {pingTo, To} ->
            io:format("Ping ricevuto da: ~p", [To]),
            node_behavior({Id, Storage, K_buckets, Timer});
        {ping, TargetId, FromTo} ->
            % cerco se è gia' presente nei propri k_buckets
            case lists:keyfind(TargetId, 2, K_buckets) of
                % se non è presente:
                false ->
                    ClosestNode = find_closest(TargetId, K_buckets),
                    case ClosestNode of
                        undefined ->
                            FromTo ! {ping_result, not_found};
                        {_, _, ClosestPid, _} ->
                            send_ping_node(ClosestPid, TargetId, FromTo, 30)
                    end;
                % se è presente:
                _ ->
                    FromTo ! {ping_result, trovato}
            end,
            node_behavior({Id, Storage, K_buckets, Timer});
        {ping_result, trovato} ->
            io:format("PONG", []),
            node_behavior({Id, Storage, K_buckets, Timer});
        {ping_result, not_found} ->
            io:format("PANG", []),
            node_behavior({Id, Storage, K_buckets, Timer});
        % modifica del tabella dei k_buckets
        {refresh, {Idx, Lista}} ->
            node_behavior({Idx, Storage, Lista, Timer});
        % aggiorna i suoi buckets
        {newBuckets, Lista} ->
            node_behavior({Id, Storage, Lista, Timer});
        {store, Value} ->
            Key = crypto:hash(sha256, Value),
            NewStorage = [[Key, Value] | Storage],
            node_behavior({Id, NewStorage, K_buckets, Timer});
        % messaggio per trovare un nodo
        {find_node, TargetId, From} ->
            case lists:keyfind(TargetId, 2, K_buckets) of
                false ->
                    % Nodo non trovato localmente, inoltra al nodo più vicino
                    ClosestNode = find_closest(TargetId, K_buckets),
                    case ClosestNode of
                        undefined ->
                            % Nessun nodo disponibile nei k-buckets
                            From ! {find_result, not_found};
                        {_, _, ClosestPid, _} ->
                            % Inoltra la richiesta al nodo più vicino
                            io:format("Nodo più vicino trovato: ~p. Inoltro la richiesta...~n", [
                                ClosestPid
                            ]),
                            % Invio della richiesta al nodo più vicino (bloccante per 30 secondi)
                            send_find_node(ClosestPid, TargetId, From, 30)
                    end,
                    node_behavior({Id, Storage, K_buckets, Timer});
                {_, FoundId, FoundPid, _} ->
                    % Nodo trovato, restituisci il risultato all'indietro
                    From ! {find_result, FoundId, FoundPid},
                    node_behavior({Id, Storage, K_buckets, Timer})
            end;
        % Quando il nodo corrente riceve il risultato della ricerca
        {find_result, FoundId, FoundPid} ->
            io:format("Nodo trovato: ID ~p, PID ~p~n", [FoundId, FoundPid]),
            node_behavior({Id, Storage, K_buckets, Timer});
        % ricerca di un valore nello storage data la key associata al valore
        {find_value, Key, From} ->
            % Controlla prima nel proprio storage
            MyStorage = lists:foldl(
                fun
                    ([K, V], _) when K =:= Key -> [K, V];
                    % Prosegui con il valore accumulato
                    (_, Acc) -> Acc
                end,
                % Valore di default se nessun match
                [],
                Storage
            ),
            case MyStorage of
                [Key, Value] ->
                    % Trovato nel proprio storage, restituisci il valore
                    From ! {value_found, Key, Value};
                [] ->
                    % Non trovato, inoltra al primo nodo nei k_buckets
                    case K_buckets of
                        [] ->
                            % Nessun nodo disponibile, restituisci not_found
                            From ! {value_not_found, Key};
                        [{_, _, ClosestPid, _} | _] ->
                            % Inoltra al nodo più vicino
                            ClosestPid ! {find_value, Key, From}
                    end
            end,
            node_behavior({Id, Storage, K_buckets, Timer});
        % Quando riceve un valore trovato
        {value_found, Key, Value} ->
            io:format("Valore trovato: ~p -> ~p~n", [Key, Value]),
            node_behavior({Id, Storage, K_buckets, Timer});
        % Quando il valore non è stato trovato
        {value_not_found, Key} ->
            io:format("Valore non trovato per chiave: ~p~n", [Key]),
            node_behavior({Id, Storage, K_buckets, Timer});
        % comportamento generico
        _ ->
            node_behavior({Id, Storage, K_buckets, Timer})
    end.

% Funzione invocata per inviare find_node in modo bloccante
send_find_node(Pid, TargetId, From, Timeout) ->
    % Invia il messaggio find_node al nodo destinazione
    Pid ! {find_node, TargetId, self()},
    % Imposta il timeout
    receive
        {find_result, FoundId, FoundPid} ->
            %  io:format("Risultato trovato dopo ~p secondi: Nodo ID ~p, PID ~p~n", [
            %     Timeout, FoundId, FoundPid
            % ]),
            From ! {find_result, FoundId, FoundPid}
    after Timeout * 1000 ->
        io:format("Timeout dopo ~p secondi: Nessun risultato trovato~n", [Timeout]),
        From ! {find_result, not_found}
    end.

% funzione per propagare il ping
send_ping_node(Pid, TargetId, From, Timeout) ->
    Pid ! {ping, TargetId, self()},
    receive
        {ping_result, Result} ->
            From ! {ping_result, Result}
    after Timeout * 1000 ->
        io:format("Timeout dopo ~p secondi: Nessuna risposta dal nodo ~p~n", [Timeout, Pid]),
        From ! {ping_result, not_found}
    end.

% funzione per propagare il find_value
send_find_value(Pid, Key, From, Timeout) ->
    Pid ! {find_value, Key, self()},
    receive
        {value_found, Key, Value} ->
            From ! {value_found, Key, Value};
        {value_not_found, Key} ->
            From ! {value_not_found, Key}
    after Timeout * 1000 ->
        io:format("Timeout dopo ~p secondi: Nessun valore trovato per Key=~p~n", [Timeout, Key]),
        From ! {value_not_found, Key}
    end.

% inizializzazione della rete di kademlia con la
% creazione del nodo boostrapt
start_system(P) ->
    mnesia:create_schema([node()]),
    mnesia:start(),
    mnesia:create_table(bootstrap_table, [
        {attributes, record_info(fields, bootstrap_table)},
        {type, set},
        {disc_copies, [node()]}
    ]),
    % generazione di un Id casuale
    NodeId = rand:uniform(1 bsl 160 - 1),
    Pid = spawn(fun() -> bootstrap_node_loop(NodeId) end),
    P ! {ok, Pid}.
%   Verifica se ciò può andare bene.
%     % aggiunta del nodo bootstrap alla tabella mnesia
%    mnesia:transaction(fun() ->
%        mnesia:write(#bootstrap_table{id = NodeId, pid = Pid, last_ping = 0})
%    end),
%    P ! {ok, Pid}.

% il nodo boostrapt e' considerato come una sorta di server
bootstrap_node_loop(Id) ->
    % segnalo che è stato avviato con successo
    io:format("Nodo bootstrap pronto con ID: ~p~n", [Id]),
    receive
        % messaggio che definisce il ping al boostrap
        {ping, From} ->
            io:format("[[--BOOSTRAP--]] --> Ping ricevuto da: ~p~n .", [From]),
            bootstrap_node_loop(Id);
        % richiesta di un nodo di entrare nella rete
        {enter, From} ->
            io:format("[[--BOOSTRAP--]] --> Richiesta di entrare nella rete da da: ~p~n .", [From]),
            % creazione id del nuovo nodo
            NodeId = rand:uniform(1 bsl 160 - 1),
            % transazione per aggiungere un nodo alla tabella mnesia
            %Tran = fun() ->
            %   mnesia:write(#boostrap_table{
            %      id = NodeId, pid = From, last_ping = 0
            % })
            %end,
            % esecuzione della transazione, inserimento del nodo
            % nella tabella mnesia del boostrap
            %mnesia:transaction(Tran),
            % Transazione per aggiungere il nodo nella tabella Mnesia
            case
                mnesia:transaction(fun() ->
                    mnesia:write(#bootstrap_table{id = NodeId, pid = From, last_ping = 0})
                end)
            of
                {_, ok} ->
                    io:format("[[--BOOSTRAP--]] Nodo aggiunto con successo: ~p~n", [NodeId]),
                    % assegnazione dell'Id e della K_buckets al nodo che entra
                    Buckets = get_4_buckets(NodeId),
                    From ! {refresh, {NodeId, Buckets}},
                    % serve il codice per aggiornare tutti i nodi vecchi
                    % Recupera tutti i nodi dalla tabella mnesia, escluso il nodo appena aggiunto
                    % solo se ci sono > 1 nodi
                    AllNodes = all(),
                    lists:foreach(
                        fun({_, _, _}) ->
                            case length(AllNodes) of
                                1 ->
                                    % Se la lista ha un solo elemento
                                    io:format("Lista contiene un solo elemento~n", []);
                                _ ->
                                    % Per ogni nodo, ricalcola i 4 nodi più vicini
                                    lists:foreach(
                                        fun({Id_tmp, Pid, _}) ->
                                            % Calcola i 4 nodi più vicini per questo nodo
                                            Buckets_tmp = get_4_buckets(Id_tmp),

                                            % Aggiorna i k-buckets di questo nodo
                                            Pid ! {newBuckets, Buckets_tmp}
                                        end,
                                        AllNodes
                                    )
                            end
                        end,
                        AllNodes
                    );
                {_, Reason} ->
                    io:format("[[--BOOSTRAP--]] Errore durante l'aggiunta del nodo: ~p~n", [Reason])
            end,
            bootstrap_node_loop(Id);
        % stampa di tutta la sua tabella di mnesia
        {print} ->
            print_all(),
            bootstrap_node_loop(Id);
        % messaggio generico
        _ ->
            io:format("[[--BOOSTRAP--]] --> Messaggio sconosciuto."),
            bootstrap_node_loop(Id)
    end.

% funzione invocata per visualizzare il contenuto dello schema di mnesia,
% solo il nodo Bootstrap può invocarla
print_all() ->
    Print = fun(#bootstrap_table{id = Id, pid = Pid, last_ping = L}, Acc) ->
        Acc ++ [{Id, Pid, L}]
    end,
    Tran = fun() -> mnesia:foldr(Print, [], bootstrap_table) end,
    {_, Res} = mnesia:transaction(Tran),
    lists:foreach(
        fun({Id, Pid, L}) ->
            io:format(" id:  ~p , pid:  ~p, last ping:  ~p \n", [Id, Pid, L])
        end,
        Res
    ).

% uguale a print_all() ma restituisce una lista
all() ->
    Print = fun(#bootstrap_table{id = Id, pid = Pid, last_ping = L}, Acc) ->
        Acc ++ [{Id, Pid, L}]
    end,
    Tran = fun() -> mnesia:foldr(Print, [], bootstrap_table) end,
    {_, Res} = mnesia:transaction(Tran),
    Res.

% funzione invocata per fornire ad un nodo, la sua lista k_buckets
% Nota Arlind: rinominiamo la funzione in get_buckets.
get_4_buckets(NodeId) ->
    Tran = fun() ->
        % Ottieni tutti i record nella tabella
        mnesia:foldr(
            fun(#bootstrap_table{id = Id, pid = Pid, last_ping = L}, Acc) ->
                % Escludi il nodo chiamante
                case Id == NodeId of
                    % Non aggiungere il nodo chiamante
                    true ->
                        Acc;
                    false ->
                        % Calcola la distanza XOR e aggiungi alla lista
                        Distance = NodeId bxor Id,
                        Acc ++ [{Distance, Id, Pid, L}]
                end
            end,
            [],
            bootstrap_table
        )
    end,
    % Esegui la transazione
    {atomic, AllRecords} = mnesia:transaction(Tran),
    % Ordina per distanza XOR
    SortedRecords = lists:sort(fun({D1, _, _, _}, {D2, _, _, _}) -> D1 < D2 end, AllRecords),
    % Prendi i primi 4
    Top4 = lists:sublist(SortedRecords, 4),
    % Restituisci la lista
    Top4.

% funzione per trovare il nodo più vicino
find_closest(TargetId, K_buckets) ->
    case K_buckets of
        [] ->
            % Nessun nodo nei k-buckets
            undefined;
        _ ->
            % Ordina i nodi nei k-buckets per distanza XOR
            Sorted = lists:sort(
                fun({Distance1, _, _, _}, {Distance2, _, _, _}) ->
                    Distance1 < Distance2
                end,
                [{TargetId bxor Id, Id, Pid, LastPing} || {_, Id, Pid, LastPing} <- K_buckets]
            ),
            % Restituisce il nodo più vicino
            hd(Sorted)
    end.
