% interfaccia per gestire i nodi kademlia

-module(nodo).
%-behaviour(gen_server).

-export([init/1, start_link/2, ping/2, start_system/1]).
-export([handle_call/3]).

% un nodo kademlia ha un proprio stato,
% ovvero un insieme di informazioni,
% quali id, k_buckets, storage, timer
-record(state, {id, k_buckets, storage, timer}).

% inizializzazione
init(Id) ->
    {ok, #state{
        id = Id,
        k_buckets = ets:new(buckets, [ordered_set, named_table]),
        storage = vuoto,
        timer = 0
    }}.

% avvio del nodo (CAPIRE DOVE USARE LA SPAWN)
start_link(Id, Options) -> gen_server:start_link({local, Id}, ?MODULE, Id, Options).

% implementazione delle interfacce di base

% ping
ping(NodeId, FromId) -> gen_server:call(NodeId, {ping, FromId}).

handle_call({ping, FromId}, _From, State) ->
    io:format("PING received from ~p~n", [FromId]),
    {reply, pong, State}.

% inizializzazione della rete di kademlia con la
% creazione del nodo boostrapt
start_system(P) ->
    mnesia:start(),
    mnesia:create_table(boostrapt_table, [
        {attributes, record_info(fields, state)},
        {type, set}
    ]),
    NodeId = rand:uniform(1 bsl 160 - 1),
    Pid = spawn(fun() -> bootstrap_node_loop(NodeId) end),
    P ! {ok, Pid}.

% il nodo boostrapt e' considerato come una sorta di server
bootstrap_node_loop(Id) ->
    io:format("Nodo bootstrap avviato con ID: ~p~n", [Id]),
    receive
        {ping, From} ->
            io:format("[[--BOOSTRAP--]]Ping ricevuto da: ~p~n", [From]),
            bootstrap_node_loop(Id);
        _ ->
            io:format("[[--BOOSTRAP--]] Messaggio sconosciuto"),
            bootstrap_node_loop(Id)
    end.
