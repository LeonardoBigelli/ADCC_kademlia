% interfaccia per gestire i nodi kademlia

-module(nodo).
-include_lib("stdlib/include/qlc.hrl").

-export([start_system/1, new_erlang_node/1, node_behavior/1, print_all/0]).
% gestione dei record per la tebella di bostrap
-record(bootstrap_table, {id, pid, last_ping}).

% creazione di un nuovo nodo
new_erlang_node(P) ->
    % valore casuale (stringa)
    Storage = generate_random_string(16),
    Key = crypto:hash(sha256, Storage),
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
        % modifica del tabella dei k_buckets
        {refresh, {Idx, Lista}} ->
            node_behavior({Idx, Storage, Lista, Timer});
        {stora, Value} ->
            Key = crypto:hash(sha256, Value),
            NewStorage = [[Key, Value] | Storage],
            node_behavior({Id, NewStorage, K_buckets, Timer});
        % comportamento generico
        _ ->
            node_behavior({Id, Storage, K_buckets, Timer})
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
                    From ! {refresh, {NodeId, Buckets}};
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
