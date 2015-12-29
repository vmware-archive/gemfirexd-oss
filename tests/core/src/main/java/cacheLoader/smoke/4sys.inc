hydra.ClientPrms-names           = client1a     client1b     client2a    client2b    client3a    client3b    client4a    client4b;
hydra.ClientPrms-gemfireNames    = gemfire1     gemfire1     gemfire2    gemfire2    gemfire3    gemfire3    gemfire4    gemfire4;
hydra.ClientPrms-vmNames         = vm1          vm1          vm2         vm2         vm3         vm3         vm4         vm4;

hydra.ClientPrms-vmQuantities    = 1 1 1 1 1 1 1 1;
hydra.ClientPrms-vmThreads       = 5 5 5 5 5 5 5 5;

THREADGROUP cacheLoaderAndUpdater
  totalThreads = 1  totalVMs = 1  clientNames = client1a
  ;

THREADGROUP otherInitializers
  totalThreads = 1  totalVMs = 1  clientNames = client1b
  totalThreads = 1  totalVMs = 1  clientNames = client2a
  totalThreads = 1  totalVMs = 1  clientNames = client2b
  totalThreads = 1  totalVMs = 1  clientNames = client3a
  totalThreads = 1  totalVMs = 1  clientNames = client3b
  totalThreads = 1  totalVMs = 1  clientNames = client4a
  totalThreads = 1  totalVMs = 1  clientNames = client4b
  ;
