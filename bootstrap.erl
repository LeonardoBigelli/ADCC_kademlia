-module(bootstrap).
-include_lib("stdlib/include/qlc.hrl").

% interfaccie da esportare
-export([start_system/1, print_all/0, all/0]).
% gestione dei record per la tebella di bostrap
-record(bootstrap_table, {id, pid, last_ping}).

% inizializzazione della rete di kademlia con la
% creazione del nodo boostrapt
start_system(P) ->
    mnesia:create_schema([node()]),
    mnesia:start(),
    mnesia:create_table(bootstrap_table, [
        {attributes, record_info(fields, bootstrap_table)},
        {type, set},
        {ram_copies, [node()]}
    ]),
    % generazione di un Id casuale per il 'principale'
    NodeId = rand:uniform(1 bsl 160 - 1),
    % generazione di un Id casuale per il 'backup'
    BackupNodeId = rand:uniform(1 bsl 160 - 1),
    Pid = spawn(node(), fun() -> init_bootstrap(NodeId, primary) end),
    % registrazione del Bootstrap 'backup'
    global:register_name(bootstrap, Pid),
    % inoltro del messaggio per creare il nodo di backup
    % del bootstrap, al bootstrap principale
    Pid ! {createBackup, BackupNodeId},
    global:sync(),
    P ! {ok}.

% utilizzato per impostare il 'trap_exit' a true.
init_bootstrap(Id, Role) ->
    erlang:process_flag(trap_exit, true),
    bootstrap_node_loop(Id, Role).

% comportamento del nodo di bootstrap, principale e backup
bootstrap_node_loop(Id, Role) ->
    % segnalo che è stato avviato con successo
    io:format("Nodo bootstrap pronto con ID: ~p~n", [Id]),
    receive
        % messaggio che definisce il ping al boostrap
        {ping, From} ->
            io:format("[[--BOOSTRAP--]] --> Ping ricevuto da: ~p~n .", [From]),
            bootstrap_node_loop(Id, Role);
        % messaggio per creare il backup
        {createBackup, BackupId} ->
            % effettuo la spawn per creare il backup
            BackupPid = spawn_link(node(), fun() -> init_bootstrap(BackupId, backup) end),
            % registro il backup
            global:register_name(
                backup_bootstrap, BackupPid
            ),
            io:format("Nodo backup del bootstrap creato.", []),
            bootstrap_node_loop(Id, Role);
        % richiesta di un nodo di entrare nella rete
        {enter, From} ->
            Time = erlang:system_time(microsecond),
            io:format("[[--BOOSTRAP--]] --> Richiesta di entrare nella rete da da: ~p~n .", [From]),
            % creazione id del nuovo nodo
            NodeId = crypto:hash(
                sha, lists:map(fun(_) -> rand:uniform(26) + $a - 1 end, lists:seq(1, 5))
            ),
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
                                    % Per ogni nodo vicino a quello appena inserito,
                                    % ricalcola i 4 nodi più vicini
                                    lists:foreach(
                                        fun({_, Id_tmp, Pid, _}) ->
                                            % Calcola i 4 nodi più vicini per questo nodo
                                            Buckets_tmp = get_4_buckets(Id_tmp),

                                            % Aggiorna i k-buckets di questo nodo
                                            Pid ! {newBuckets, Buckets_tmp}
                                        end,
                                        Buckets
                                    )
                            end
                        end,
                        AllNodes
                    ),
                    CurrentTime = erlang:system_time(microsecond),
                    io:format("Tempo necessario per l'aggiunta: ~ps\n", [CurrentTime - Time]);
                {_, Reason} ->
                    io:format("[[--BOOSTRAP--]] Errore durante l'aggiunta del nodo: ~p~n", [Reason])
            end,
            % avvio del sistema per inviare periodicamente i valori
            % dello Storage del nodo ai suoi k_buckets
            From ! {send_periodic},
            bootstrap_node_loop(Id, Role);
        % stampa di tutta la sua tabella di mnesia
        {print} ->
            print_all(),
            bootstrap_node_loop(Id, Role);
        % messaggio per far morirre il nodo
        {crash} ->
            exit(errore);
        % Fallimento di uno dei nodi (NON FUNZIONA, STESSO PRINCIPIO DI QUANDO LI LINKO LA PRIMA VOLTA...)
        {'EXIT', _, _Reason} ->
            io:format("ricevuto exit....", []),
            case Role of
                primary ->
                    % il backup è morto
                    io:format(
                        "Il nodo di backup è morto. Creazione di un nuovo nodo di backup.~n"
                    ),
                    NewBackupPid = spawn_link(node(), fun() ->
                        bootstrap_node_loop(rand:uniform(1 bsl 160 - 1), backup)
                    end),
                    global:register_name(backup_bootstrap, NewBackupPid),
                    bootstrap_node_loop(Id, primary);
                backup ->
                    % il principale è morto
                    io:format(
                        "Il nodo principale è morto. Divento il nuovo principale e creo un nuovo backup.~n"
                    ),
                    global:unregister_name(backup_bootstrap),
                    global:register_name(bootstrap, self()),

                    % Creazione del nuovo nodo di backup
                    NewBackupPid = spawn_link(node(), fun() ->
                        bootstrap_node_loop(rand:uniform(1 bsl 160 - 1), backup)
                    end),
                    global:register_name(backup_bootstrap, NewBackupPid),
                    bootstrap_node_loop(Id, primary)
            end;
        % messaggio generico
        _ ->
            io:format("[[--BOOSTRAP--]] --> Messaggio sconosciuto."),
            bootstrap_node_loop(Id, Role)
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
                        %Distance = NodeId bxor Id,
                        Distance = calculate_distance(NodeId, Id),
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

    case SortedRecords of
        [] ->
            % Nessun nodo nella rete, restituisce una lista vuota
            [];
        [_] ->
            % Un solo nodo nella rete, restituisci quel nodo
            SortedRecords;
        _ ->
            % Almeno due nodi nella rete
            ClosestTwo = lists:sublist(SortedRecords, 2),
            TotalNodes = length(SortedRecords),
            MiddleNodeIndex =
                case TotalNodes > 2 of
                    true -> round(TotalNodes / 2);
                    false -> 1
                end,
            MiddleNode = lists:nth(MiddleNodeIndex, SortedRecords),
            FarthestNode = lists:last(SortedRecords),
            UniqueNodes = lists:usort(ClosestTwo ++ [MiddleNode, FarthestNode]),
            lists:sublist(UniqueNodes, 4)
    end.

%%% Funzione per calcolare la distanza tra due binari
calculate_distance(Bin1, Bin2) when is_binary(Bin1), is_binary(Bin2) ->
    calculate_distance(binary:bin_to_list(Bin1), binary:bin_to_list(Bin2));
calculate_distance([Byte1 | Rest1], [Byte2 | Rest2]) ->
    abs(Byte1 - Byte2) + calculate_distance(Rest1, Rest2);
calculate_distance([], []) ->
    0;
calculate_distance(_, _) ->
    erlang:error({badarg, "Binary lengths must match"}).
