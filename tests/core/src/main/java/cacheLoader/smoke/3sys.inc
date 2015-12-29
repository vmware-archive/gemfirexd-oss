// hydra.HostPrms-names          = host1    host2    host3;

// --------------------------------------------------------
// For remote tests, set the following, e.g. in local.conf:
// hydra.HostPrms-hostNames         = duck     stealth  biker;
// --------------------------------------------------------

// hydra.VmPrms-names            = vm1      vm2      vm3;
// hydra.VmPrms-hostNames        = host1    host2    host3;

// hydra.GemFirePrms-names       = gemfire1 gemfire2 gemfire3;
// hydra.GemFirePrms-hostNames   = host1    host2    host3;

hydra.ClientPrms-names           = client1a     client1b     client2a    client2b    client3a    client3b;
hydra.ClientPrms-gemfireNames    = gemfire1     gemfire1     gemfire2    gemfire2    gemfire3    gemfire3;
hydra.ClientPrms-vmNames         = vm1          vm1          vm2         vm2         vm3         vm3;

hydra.ClientPrms-vmQuantities    = 1 1 1 1 1 1;
hydra.ClientPrms-vmThreads       = 5 5 5 5 5 5;

THREADGROUP cacheLoaderAndUpdater
  totalThreads = 1  totalVMs = 1  clientNames = client1a
  ;

THREADGROUP otherInitializers
  totalThreads = 1  totalVMs = 1  clientNames = client1b
  totalThreads = 1  totalVMs = 1  clientNames = client2a
  totalThreads = 1  totalVMs = 1  clientNames = client2b
  totalThreads = 1  totalVMs = 1  clientNames = client3a
  totalThreads = 1  totalVMs = 1  clientNames = client3b
  ;
