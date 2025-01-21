% interfaccia per gestire i nodi kademlia

-module(nodo).
-include_lib("stdlib/include/qlc.hrl").

% interfaccie da esportare
-export([start_system/1, new_kademlia_node/1, print_all/0, all/0]).
% gestione dei record per la tebella di bostrap
-record(bootstrap_table, {id, pid, last_ping}).

% creazione di un nuovo nodo
new_kademlia_node(P) ->
    % valore casuale (stringa)
    Storage = generate_random_string(5),
    %Key = crypto:hash(sha256, Storage), NON SI USA SOLO PER IL TEST
    Key = generate_random_string(3),
    % inizializzazione temporanea dei valori di un nodo
    InitialValues = {"idTMP", [[Key, Storage]], [], 0},
    % effettuo la spawn del nodo vero e proprio con i parametri
    % temporanei iniziali
    Pid = spawn(fun() -> node_behavior(InitialValues) end),
    % restituisco il Pid del nodo di cui ho effettuato la spawn
    % al nodo invocante (i.e. shell)
    P ! {ok, Pid}.

% Funzione per generare una stringa casuale di lunghezza N
generate_random_string(N) ->
    lists:map(fun(_) -> rand:uniform(26) + $a - 1 end, lists:seq(1, N)).

% definizione del comportamento di un nodo kademlia generico
% definizione dei parametri:
% -Id: identificativo univoco rispetto alla rete kademlia;
% -Storage: lista di [Key, Value];
% -K_buckets: i primi K nodi della rete vicini;
% Timer: ultima volta che è stato contattato (FORSE DA ELIMINARE)
node_behavior({Id, Storage, K_buckets, Timer}) ->
    receive
        % messaggio per visualizzare lo stato del nodo corrente
        {getInfo} ->
            io:format("Id: ~p - Storage: ~p - K_buckets: ~p - Timer: ~p. ", [
                Id, Storage, K_buckets, Timer
            ]),
            node_behavior({Id, Storage, K_buckets, Timer});
        % messaggio per l'invio periodico dello Storage ai suoi nodi della k_buckets
        {send_periodic} ->
            % Invia un messaggio a tutti i nodi nei K_buckets
            lists:foreach(
                fun({_, _, Pid, _}) ->
                    %io:format("Inoltro dello Storage...", []),
                    Pid ! {addToStore, Storage}
                end,
                K_buckets
            ),
            % Avvia il prossimo ciclo dopo 30 secondi
            timer:send_after(30000, self(), {send_periodic}),
            node_behavior({Id, Storage, K_buckets, Timer});
        % messaggio per  ricevere un ping, solo io:format() su shell
        {pingTo, To} ->
            io:format("Ping ricevuto da: ~p", [To]),
            node_behavior({Id, Storage, K_buckets, Timer});
        % messaggio per cambiare il last_ping del nodo
        {changeTimer, T, FromTo} ->
            % il nodo che deve aggioranre il timer, vuol dire che ha ricevuto
            % un ping, quindi deve aggiornare la sua K_buckets,
            % modificando il campo last_ping in corrispondenza
            % di FromTo
            UpdatedBuckets = lists:map(
                fun
                    ({Distance, Id_tmp, Pid, _}) when Pid =:= FromTo ->
                        % Aggiorna last_ping con T
                        {Distance, Id_tmp, Pid, T};
                    (Node) ->
                        % Lascia inalterati gli altri nodi
                        Node
                end,
                K_buckets
            ),
            node_behavior({Id, Storage, UpdatedBuckets, T});
        % messaggio per effettuare un ping tramite l'identificativo della
        % rete kademlia
        {ping, TargetId, FromTo} ->
            % cerco se è gia' presente nei propri k_buckets
            List = lists:keyfind(TargetId, 2, K_buckets),
            case List of
                % se non è presente, contatto il nodo più vicino:
                false ->
                    ClosestNode = find_closest(TargetId, K_buckets),
                    case ClosestNode of
                        undefined ->
                            FromTo ! {ping_result, not_found};
                        {_, _, ClosestPid, _} ->
                            send_ping_node(ClosestPid, TargetId, FromTo, 30)
                    end;
                % se è presente, contatto il nodo direttamente:
                {_, _, Pid, _} ->
                    % aggiorno il campo Timer del nodo che ha ricevuto il ping
                    CurrentTime = erlang:system_time(second),
                    Pid ! {changeTimer, CurrentTime, FromTo},
                    % devo aggiornare anche il mio campo della K_bucket
                    % perchè ho verificato che un nodo sia raggiungibile
                    UpdatedBuckets = lists:map(
                        fun
                            ({Distance, Id_tmp, OtherPid, _}) when OtherPid =:= Pid ->
                                % Aggiorna last_ping con T
                                {Distance, Id_tmp, Pid, CurrentTime};
                            (Node) ->
                                % Lascia inalterati gli altri nodi
                                Node
                        end,
                        K_buckets
                    ),
                    FromTo ! {newBuckets, UpdatedBuckets},
                    FromTo ! {ping_result, trovato}
            end,
            node_behavior({Id, Storage, K_buckets, Timer});
        % messaggio per segnalare che il ping è avvenuto con successo
        {ping_result, trovato} ->
            io:format("PONG", []),
            node_behavior({Id, Storage, K_buckets, Timer});
        % messaggio per segnalare che il ping è fallito
        {ping_result, not_found} ->
            io:format("PANG", []),
            node_behavior({Id, Storage, K_buckets, Timer});
        % modifica del tabella dei k_buckets e dell'id.
        % il messaggio viene ricevuto quando il nodo entra con successo
        % nella rete di kademlia
        {refresh, {Idx, Lista}} ->
            node_behavior({Idx, Storage, Lista, Timer});
        % aggiorna i buckets del nodo corrente
        {newBuckets, Lista} ->
            node_behavior({Id, Storage, Lista, Timer});
        {store, Value} ->
            Key = crypto:hash(sha256, Value),
            NewStorage = [[Key, Value] | Storage],
            node_behavior({Id, NewStorage, K_buckets, Timer});
        % messaggio per aggiungere elementi al mio store se non
        % sono gia' presenti
        {addToStore, MoreStore} ->
            % Filtra gli elementi di MoreStore che non sono già presenti in Storage
            FilteredStore = lists:filter(
                fun(Element) -> not lists:member(Element, Storage) end,
                MoreStore
            ),
            % Aggiungi solo gli elementi filtrati a Storage
            NewStorage = FilteredStore ++ Storage,
            node_behavior({Id, NewStorage, K_buckets, Timer});
        % messaggio per trovare un nodo, dato il suo Id
        {find_node, TargetId, From} ->
            StartTime = erlang:system_time(second),
            %% check 2
            case lists:keyfind(TargetId, 2, K_buckets) of
                false ->
                    % Nodo non trovato localmente, inoltra al nodo più vicino
                    ClosestNode = find_closest(TargetId, K_buckets),
                    case ClosestNode of
                        undefined ->
                            % Nessun nodo disponibile nei k-buckets

                            % not matched
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
                    EndTime = erlang:system_time(second),
                    io:format("Tempo impiegato per trovare un nodo: ~ps\n", [EndTime - StartTime]),
                    From ! {find_result, FoundId, FoundPid},
                    node_behavior({Id, Storage, K_buckets, Timer})
            end;
        % Quando il nodo corrente riceve il risultato della ricerca
        {find_result, FoundId, FoundPid} ->
            io:format("Nodo trovato: ID ~p, PID ~p~n", [FoundId, FoundPid]),
            node_behavior({Id, Storage, K_buckets, Timer});
        {find_result, not_found} ->
            io:format("Nodo non trovato\n", []),
            % ricerca di un valore nello storage data la key associata al valore
            node_behavior({Id, Storage, K_buckets, Timer});
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
                            % ClosestPid ! {find_value, Key, From} FUNZIONA MA NON è BLOCCANTE
                            send_find_value(ClosestPid, Key, From, 30)
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

% Funzione invocata per inviare find_node (bloccante)
send_find_node(Pid, TargetId, From, Timeout) ->
    % Invia il messaggio find_node al nodo destinazione
    Pid ! {find_node, TargetId, self()},
    % Imposta il timeout
    receive
        {find_result, FoundId, FoundPid} ->
            %  io:format("Risultato trovato dopo ~p secondi: Nodo ID ~p, PID ~p~n", [
            %     Timeout, FoundId, FoundPid
            % ]),
            From ! {find_result, FoundId, FoundPid};
        % se non lo trova
        {find_result, not_found} ->
            From ! {find_result, not_found}
    after Timeout * 1000 ->
        io:format("Timeout dopo ~p secondi: Nessun risultato trovato~n", [Timeout]),
        From ! {find_result, not_found}
    end.

% funzione per propagare il ping (bloccante)
send_ping_node(Pid, TargetId, From, Timeout) ->
    Pid ! {ping, TargetId, self()},
    receive
        {ping_result, Result} ->
            From ! {ping_result, Result}
    after Timeout * 1000 ->
        io:format("Timeout dopo ~p secondi: Nessuna risposta dal nodo ~p~n", [Timeout, Pid]),
        From ! {ping_result, not_found}
    end.

% funzione per propagare il find_value (bloccante)
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
    % generazione di un Id casuale per il 'principale'
    NodeId = rand:uniform(1 bsl 160 - 1),
    % generazione di un Id casuale per il 'backup'
    BackupNodeId = rand:uniform(1 bsl 160 - 1),
    Pid = spawn(fun() -> init_bootstrap(NodeId, primary) end),
    % registrazione del Bootstrap 'backup'
    register(bootstrap, Pid),
    % inoltro del messaggio per creare il nodo di backup
    % del bootstrap, al bootstrap principale
    Pid ! {createBackup, BackupNodeId},
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
            BackupPid = spawn_link(fun() -> init_bootstrap(BackupId, backup) end),
            % registro il backup
            register(
                backup_bootstrap, BackupPid
            ),
            io:format("Nodo backup del bootstrap creato.", []),
            bootstrap_node_loop(Id, Role);
        % richiesta di un nodo di entrare nella rete
        {enter, From} ->
            Time = erlang:system_time(second),
            io:format("[[--BOOSTRAP--]] --> Richiesta di entrare nella rete da da: ~p~n .", [From]),
            % creazione id del nuovo nodo
            NodeId = rand:uniform(1 bsl 160 - 1),
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
                    CurrentTime = erlang:system_time(second),
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
                    NewBackupPid = spawn_link(fun() ->
                        bootstrap_node_loop(rand:uniform(1 bsl 160 - 1), backup)
                    end),
                    register(backup_bootstrap, NewBackupPid),
                    bootstrap_node_loop(Id, primary);
                backup ->
                    % il principale è morto
                    io:format(
                        "Il nodo principale è morto. Divento il nuovo principale e creo un nuovo backup.~n"
                    ),
                    unregister(backup_bootstrap),
                    register(bootstrap, self()),

                    % Creazione del nuovo nodo di backup
                    NewBackupPid = spawn_link(fun() ->
                        bootstrap_node_loop(rand:uniform(1 bsl 160 - 1), backup)
                    end),
                    register(backup_bootstrap, NewBackupPid),
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
            lists:sublist(UniqueNodes, 4),
            io:format("K_buckets: ~p\n", [lists:sublist(UniqueNodes, 4)])
    end.

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
