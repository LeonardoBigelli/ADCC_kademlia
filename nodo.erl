% interfaccia per gestire i nodi kademlia

-module(node).
-behaviour(gen_server).

-export([init/1, start_link/2]).

% un nodo kademlia ha un proprio stato,
% ovvero un insieme di informazioni,
% quali id, k_buckets, storage, timer
-record(state, {id, k_buckets, storage, timer}).

% inizializzazione
init(Id) -> {ok, #state{id = Id, k_buckets = vuoto, storage = vuoto, timer = 0}}.

% avvio del nodo (CAPIRE DOVE USARE LA SPAWN)
start_link(Id, Options) -> gen_server:start_link({local, Id}, ?MODULE, Id, Options).
