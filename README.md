# ADCC_kademlia

## Comandi iniziali

#### Compilazione del file sorgente
c(nodo).
#### Inizializzazione del sistema con creazione del nodo di Bootstrap (Boot)
{_, Boot} = nodo:start_system(self()).
#### Verifica della corretta raggiungibilità del nodo di Boot
Boot ! {ping, self()}.
#### Creazione di un attore che rappresenta un nodo
{_, Nodo\_1} = new\_erlang\_node(self()).
#### Inserimento del nuovo nodo nella rete kademlia
Boot ! {enter, Nodo\_1}. 
