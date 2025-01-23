% interfaccia per gestire i nodi kademlia

-module(nodo).
-include_lib("stdlib/include/qlc.hrl").

% interfaccie da esportare
-export([new_kademlia_node/1]).
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
                    end,
                    node_behavior({Id, Storage, K_buckets, Timer});
                % se è presente, contatto il nodo direttamente:
                {_, _, Pid, _} ->
                    io:format("Trovato......\n"),
                    % aggiorno il campo Timer del nodo che ha ricevuto il ping
                    CurrentTime = erlang:system_time(second),
                    % Pid ! {changeTimer, CurrentTime, FromTo},

                    % contatto il nodo mittente per aggiornare il suo last_timer
                    % riguardo al nodo destinatario
                    %FromTo ! {newBuckets, ping, Pid, UpdatedBuckets},
                    % contatto il nodo mittente per segnalare che il risultato positivo
                    FromTo ! {ping_result, trovato, CurrentTime, Pid},
                    % controllo se il destinatorio (nodo corrente)
                    % ha la k_bucket completa, se così fosse sostituisco il nodo piu'
                    % vecchio con il mittente del ping
                    % D = TargetId bxor Id,
                    %NewElement = {D, TargetId, Pid, CurrentTime},
                    %  update_buckets_after_ping(K_buckets, NewElement),
                    node_behavior({Id, Storage, K_buckets, Timer})
            end;
        % messaggio per segnalare che il ping è avvenuto con successo
        {ping_result, trovato, CurrentTime, Pid} ->
            io:format("PONG", []),
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
            node_behavior({Id, Storage, UpdatedBuckets, Timer});
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
        % aggiorna i buckets durante il ping
        {newBuckets, ping, Pid, Lista} ->
            % Verifica se esiste una tupla con OtherPid == Pid
            Exists = lists:any(
                fun({_, _, OtherPid, _}) ->
                    OtherPid == Pid
                end,
                K_buckets
            ),
            case Exists of
                true -> node_behavior({Id, Storage, Lista, Timer});
                false -> node_behavior({Id, Storage, K_buckets, Timer})
            end;
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
        {find_result, not_found} ->
            io:format("Nodo non trovato\n", []),
            % ricerca di un valore nello storage data la key associata al valore
            node_behavior({Id, Storage, K_buckets, Timer});
        % Quando il nodo corrente riceve il risultato della ricerca
        {find_result, FoundId, FoundPid} ->
            io:format("Nodo trovato: ID ~p, PID ~p~n", [FoundId, FoundPid]),
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
                        % ERRORE IL 'CLOSERPID' DEVE ESSERE DATO DAL HASH_CHIAVE XOR IDS DELLA K_BUCKETS (PRENDO IL PIù VICINO)
                        _ ->
                            ClosestPid = find_closest(Key, K_buckets),
                            % Inoltra al nodo più vicino, rispetto alla chiave
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
    Pid ! {ping, TargetId, From},
    receive
        {ping_result, trovato, CurrentTime, Pid} ->
            io:format("La funzione ha ricevuto con successo e non è stata bloccata\n", []),
            From ! {ping_result, trovato, CurrentTime, Pid};
        {ping_result, not_found} ->
            io:format("Nodo PINGATO non trovato!", [])
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
