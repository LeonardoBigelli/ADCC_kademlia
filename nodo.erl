% interfaccia per gestire i nodi kademlia

-module(nodo).
%-behaviour(gen_server).

-export([init/1, start_link/2, ping/2]).
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
