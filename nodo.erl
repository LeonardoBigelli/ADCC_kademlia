% interfaccia per gestire i nodi kademlia

-module(nodo).
-include_lib("stdlib/include/qlc.hrl").

-export([start_system/1, new_erlang_node/1, node_behavior/0]).
% gestione dei record per la tebella di bostrap
-record(bootstrap_table, {id, pid, last_ping}).

% creazione di un nuovo nodo
new_erlang_node(P) ->
    % TODO: inizializzare lo Storage
    Pid = spawn(nodo, node_behavior, []),
    P ! {ok, Pid}.

% definizione del comportamento di un nodo kademlia generico
node_behavior() ->
    receive
        {pingTO, To} ->
            io:format("TODO ~p", [To]),
            node_behavior()
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
                    io:format("[[--BOOSTRAP--]] Nodo aggiunto con successo: ~p~n", [NodeId]);
                {_, Reason} ->
                    io:format("[[--BOOSTRAP--]] Errore durante l'aggiunta del nodo: ~p~n", [Reason])
            end,
            bootstrap_node_loop(Id);
        % messaggio generico
        _ ->
            io:format("[[--BOOSTRAP--]] --> Messaggio sconosciuto."),
            bootstrap_node_loop(Id)
    end.
