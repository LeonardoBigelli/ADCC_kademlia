% interfaccia per gestire i nodi kademlia
-module(nodo).
-include_lib("stdlib/include/qlc.hrl").

% interfaccia da esportare
-export([new_kademlia_node/1]).

% creazione di un nuovo nodo
new_kademlia_node(P) ->
    % valore casuale (stringa)
    Storage = generate_random_string(5),
    Key = crypto:hash(sha, Storage),
    %Key = generate_random_string(3), SOLO PER TESTING!!!
    % inizializzazione temporanea dei valori di un nodo
    InitialValues = {"idTMP", [[Key, Storage]], []},
    % effettuo la spawn del nodo vero e proprio con i parametri
    % temporanei iniziali
    Pid = spawn(node(), fun() -> node_behavior(InitialValues) end),
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
node_behavior({Id, Storage, K_buckets}) ->
    receive
        % messaggio per visualizzare lo stato del nodo corrente
        {getInfo} ->
            io:format("Id: ~p - Storage: ~p - K_buckets: ~p\n. ", [
                Id, Storage, K_buckets
            ]),
            node_behavior({Id, Storage, K_buckets});
        % messaggio utilizzato per la gestione della K_buckets dinamica
        {isAlive, Pid, OtherId} ->
            % controllo se è possibile inserire elementi
            % nella propria K_buckets
            case length(K_buckets) < 4 of
                true ->
                    NewKB_tmp = {binary_xor(Id, OtherId), OtherId, Pid},
                    % controllo se quel Pid è già presente nella mia K_buckets
                    case lists:any(fun({_, _, X}) -> X == Pid end, K_buckets) of
                        false -> NewKB = [NewKB_tmp] ++ K_buckets;
                        true -> NewKB = K_buckets
                    end;
                false ->
                    NewKB = K_buckets
            end,
            Pid ! {alive, self()},
            node_behavior({Id, Storage, NewKB});
        % messaggio utilizzato per la gestione della K_buckets dinamica
        % rimozione dei nodi morti
        {updateB, Pid} ->
            % Controllo tutta la K_buckets e rimuovo il nodo con il Pid in
            % questione
            NewKB = lists:foldr(
                fun({A, B, C}, Acc) ->
                    case C of
                        Pid -> NewAcc = Acc;
                        _ -> NewAcc = [{A, B, C}] ++ Acc
                    end,
                    NewAcc
                end,
                [],
                K_buckets
            ),
            node_behavior({Id, Storage, NewKB});
        % messaggio per l'invio periodico dello Storage ai suoi nodi della k_buckets
        {send_periodic} ->
            % salvo chi ha invocato il messaggio
            Self = self(),
            % effettuo una spawn, per evitare di bloccare il nodo,
            % con lo scopo di controllare se i nodi presenti nella
            % propria K_buckets sono ancora raggiungibili
            lists:foreach(
                fun({_, OtherId, Pid}) ->
                    spawn(
                        fun() ->
                            Pid ! {isAlive, self(), OtherId},
                            receive
                                {alive, Pid} -> ok
                                % se non è raggiungibile,
                                % viene rimosso
                            after 5000 -> Self ! {updateB, Pid}
                            end
                        end
                    )
                end,
                K_buckets
            ),
            % Invia un messaggio a tutti i nodi nei K_buckets
            lists:foreach(
                fun({_, _, Pid}) ->
                    Pid ! {addToStore, Storage}
                end,
                K_buckets
            ),
            % Avvia il prossimo ciclo dopo 30 secondi
            timer:send_after(30000, self(), {send_periodic}),
            node_behavior({Id, Storage, K_buckets});
        % Ping che sfrutta il pattern matching, se l'Id da cercare è il nodo stesso
        % restituisce che è stato trovato
        {ping, Id, From, StartTime} ->
            io:format("[PING] PONG\n", []),
            [H | _] = From,
            H ! {ping_result, trovato, StartTime, self()},
            node_behavior({Id, Storage, K_buckets});
        % messaggio per effettuare un ping tramite l'identificativo della
        % rete kademlia
        {ping, TargetId, From, T} ->
            %StartTime = erlang:system_time(microsecond),
            % cerco se è gia' presente nei propri k_buckets
            [H | _] = From,
            case lists:keyfind(TargetId, 2, K_buckets) of
                false ->
                    % elimino i nodi a cui inoltrare il messaggio
                    % che sono già stati visitati
                    Bucktes_noLoop = lists:filter(
                        fun({_, _, X}) -> not lists:member(X, From) end, K_buckets
                    ),
                    % inoltro al nodo più vicino
                    ClosestNode = find_closest(TargetId, Bucktes_noLoop),
                    case ClosestNode of
                        undefined ->
                            % Nessun nodo disponibile nei k-buckets
                            H ! {ping_result, not_found};
                        {_, _, ClosestPid} ->
                            io:format(
                                "[PING] Nodo più vicino trovato: ~p. Inoltro la richiesta...~n\n",
                                [
                                    ClosestPid
                                ]
                            ),
                            % Invio della richiesta al nodo più vicino (bloccante per 5 secondi)
                            send_ping_node(ClosestPid, TargetId, From, 5, T)
                    end,
                    node_behavior({Id, Storage, K_buckets});
                {_, _, ClosestPidInit} ->
                    % Nodo trovato, restituisci il risultato al nodo precedente
                    ClosestPidInit ! {ping, TargetId, [self()] ++ From, T},
                    node_behavior({Id, Storage, K_buckets})
            end;
        % messaggio per segnalare che il ping è avvenuto con successo
        {ping_result, trovato, StartTime, Pid} ->
            EndTime = erlang:system_time(microsecond),
            io:format("PONG a ~p !!! in ~p [microsenci]\n", [Pid, EndTime - StartTime]),
            node_behavior({Id, Storage, K_buckets});
        % messaggio per segnalare che il ping è fallito
        {ping_result, not_found} ->
            %  io:format("PANG\n", []),
            node_behavior({Id, Storage, K_buckets});
        % modifica del tabella dei k_buckets e dell'id.
        % il messaggio viene ricevuto quando il nodo entra con successo
        % nella rete di kademlia
        {refresh, {Idx, Lista}} ->
            node_behavior({Idx, Storage, Lista});
        % aggiorna i buckets del nodo corrente
        {newBuckets, Lista} ->
            node_behavior({Id, Storage, Lista});
        % messaggio per insserire un nuovo elemento nello store
        {store, Value} ->
            %   StartTime = erlang:system_time(microsecond),
            % calcolo della chiave sul valore da inserire
            Key = crypto:hash(sha, Value),
            NewStorage = [[Key, Value] | Storage],
            %  EndTime = erlang:system_time(microsecond),
            % io:format("Valore aggiunto in ~p [microsecondi]", [EndTime - StartTime]),
            node_behavior({Id, NewStorage, K_buckets});
        % messaggio per aggiungere elementi al mio store (ricevuti da altri nodi) se non
        % sono gia' presenti
        {addToStore, MoreStore} ->
            %  StartTime = erlang:system_time(microsecond),
            % Filtra gli elementi di MoreStore che non sono già presenti in Storage
            FilteredStore = lists:filter(
                fun(Element) -> not lists:member(Element, Storage) end,
                MoreStore
            ),
            % Aggiungi solo gli elementi filtrati a Storage
            NewStorage = FilteredStore ++ Storage,
            %  EndTime = erlang:system_time(microsecond),
            node_behavior({Id, NewStorage, K_buckets});
        % messaggio per trovare un nodo, dato il suo Id
        % ultimo paramentro è il tempo
        {find_node, TargetId, From, T} ->
            % StartTime = erlang:system_time(microsecond),
            [H | _] = From,
            % controllo se il nodo che sto cercando è presente
            % o meno nella mia K_buckets
            case lists:keyfind(TargetId, 2, K_buckets) of
                false ->
                    % elimino i nodi a cui inoltrare il messaggio
                    % che sono già stati visitati
                    Bucktes_noLoop = lists:filter(
                        fun({_, _, X}) -> not lists:member(X, From) end, K_buckets
                    ),
                    % inoltro al nodo più vicino
                    ClosestNode = find_closest(TargetId, Bucktes_noLoop),
                    case ClosestNode of
                        undefined ->
                            % Nessun nodo disponibile nei k-buckets
                            H ! {find_result, not_found};
                        {_, _, ClosestPid} ->
                            io:format(
                                "Nodo più vicino trovato: ~p. Inoltro la richiesta...~n", [
                                    ClosestPid
                                ]
                            ),
                            % Invio della richiesta al nodo più vicino (bloccante per 5 secondi)
                            send_find_node(ClosestPid, TargetId, From, 5, T)
                    end,
                    node_behavior({Id, Storage, K_buckets});
                {_, FoundId, FoundPid} ->
                    % Nodo trovato, restituisci il risultato al nodo chiamante
                    %EndTime = erlang:system_time(microsecond),
                    H ! {find_result, FoundId, FoundPid, T},
                    node_behavior({Id, Storage, K_buckets})
            end;
        % messaggio per segnalare che non è stato trovato il nodo
        {find_result, not_found} ->
            io:format("Nodo non trovato\n", []),
            node_behavior({Id, Storage, K_buckets});
        % Quando il nodo corrente riceve il risultato della ricerca
        {find_result, FoundId, FoundPid, StartTime} ->
            EndTime = erlang:system_time(microsecond),
            io:format("Nodo trovato: ID ~p, PID ~p, Tempo necessario: ~p micros\n", [
                FoundId, FoundPid, EndTime - StartTime
            ]),
            node_behavior({Id, Storage, K_buckets});
        % messaggio per cercare un valore data la chiave
        {find_value, Key, From, T} ->
            % From è una lista
            [H | _] = From,
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
                    H ! {value_found, Key, Value, T};
                [] ->
                    % Valore non on trovato, inoltra al primo nodo nei k_buckets più vicino alla chiave
                    case K_buckets of
                        [] ->
                            % Nessun nodo disponibile, restituisci not_found
                            H ! {value_not_found, Key};
                        % la chiave è data da un hash a 160 bit
                        _ ->
                            {_, _, ClosestPid} = find_closest(Key, K_buckets),
                            % Inoltra al nodo più vicino, rispetto alla chiave
                            send_find_value(ClosestPid, Key, From, 5, T)
                    end
            end,
            node_behavior({Id, Storage, K_buckets});
        % messaggio scambiato segnalando che il valore da cercare
        % è stato trovato
        {value_found, Key, Value, StartTime} ->
            EndTime = erlang:system_time(microsecond),
            io:format("Valore trovato: ~p -> ~p in ~p [microsecondi] ~n\n", [
                Key, Value, EndTime - StartTime
            ]),
            node_behavior({Id, Storage, K_buckets});
        % messaggio scambiato quando il valore non è stato trovato
        {value_not_found, Key} ->
            io:format("Valore non trovato per chiave: ~p~n", [Key]),
            node_behavior({Id, Storage, K_buckets});
        % messaggio per far morire un nodo, genera una 'exit'
        {crash} ->
            exit(errore);
        % comportamento generico
        _ ->
            node_behavior({Id, Storage, K_buckets})
    end.

% Funzione invocata per inviare find_node (bloccante)
send_find_node(Pid, TargetId, From, Timeout, StartTime) ->
    [H | _] = From,
    % Invia il messaggio find_node al nodo destinazione
    Pid ! {find_node, TargetId, [self()] ++ From, StartTime},
    receive
        {find_result, FoundId, FoundPid, StartTime} ->
            H ! {find_result, FoundId, FoundPid, StartTime};
        % nel caso del nodo non trovato
        {find_result, not_found} ->
            H ! {find_result, not_found}
    after Timeout * 1000 ->
        io:format("Timeout dopo ~p secondi: Nessun risultato trovato~n", [Timeout]),
        H ! {find_result, not_found}
    end.

% funzione per propagare il ping (bloccante)
send_ping_node(Pid, TargetId, From, Timeout, StartTime) ->
    [H | _] = From,
    Pid ! {ping, TargetId, [self()] ++ From, StartTime},
    receive
        {ping_result, trovato, CurrentTime, RPid, StartTime} ->
            H ! {ping_result, trovato, CurrentTime, RPid, StartTime};
        {ping_result, not_found} ->
            H ! {ping_result, not_found}
    after Timeout * 1000 ->
        %io:format("Timeout dopo ~p secondi: Nessuna risposta dal nodo ~p~n", [Timeout, Pid]),
        H ! {ping_result, not_found}
    end.

% funzione per propagare il find_value (bloccante)
send_find_value(Pid, Key, From, Timeout, StartTime) ->
    [H | _] = From,
    Pid ! {find_value, Key, [self()] ++ From, StartTime},
    receive
        {value_found, Key, Value, StartTime} ->
            H ! {value_found, Key, Value, StartTime};
        {value_not_found, Key} ->
            H ! {value_not_found, Key}
    after Timeout * 1000 ->
        io:format("Timeout dopo ~p secondi: Nessun valore trovato per Key=~p~n", [Timeout, Key]),
        H ! {value_not_found, Key}
    end.

% funzione per trovare il nodo più vicino
find_closest(TargetId, K_bucketsTMP) ->
    case K_bucketsTMP of
        [] ->
            % Nessun nodo nei k-buckets
            undefined;
        _ ->
            % Ordina i nodi nei k-buckets per distanza XOR
            Sorted = lists:sort(
                fun({Distance1, _, _}, {Distance2, _, _}) ->
                    Distance1 < Distance2
                end,
                % calcolo della distanza
                lists:map(
                    fun({_, Id, Pid}) ->
                        {binary_xor(TargetId, Id), Id, Pid}
                    end,
                    K_bucketsTMP
                )
            ),
            % Restituisce il nodo più vicino
            hd(Sorted)
        % check se Sorted == FROM
    end.

% funzione re-implementata per il calcolo della Binary-XOR (la distanza)
binary_xor(Bin1, Bin2) when is_binary(Bin1), is_binary(Bin2) ->
    case byte_size(Bin1) == byte_size(Bin2) of
        true ->
            lists:foldl(
                fun({Byte1, Byte2}, Acc) ->
                    Acc bsl 8 bor (Byte1 bxor Byte2)
                end,
                0,
                lists:zip(binary:bin_to_list(Bin1), binary:bin_to_list(Bin2))
            );
        false ->
            erlang:error(badarith)
    end.
