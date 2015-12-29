// hydra.HostPrms-names          = host1    host2;

// --------------------------------------------------------
// For remote tests, set something like the following in local.conf:
// hydra.HostPrms-hostNames         = duck     stealth;
// --------------------------------------------------------

// hydra.VmPrms-names            = vm1      vm2;
// hydra.VmPrms-hostNames        = host1    host2;

// hydra.GemFirePrms-names       = gemfire1 gemfire2;
// hydra.GemFirePrms-hostNames   = host1    host2;

hydra.ClientPrms-names           = client1a     client1b     client2a     client2b  client2c;
hydra.ClientPrms-gemfireNames    = gemfire1     gemfire1     gemfire2     gemfire2  gemfire2;
hydra.ClientPrms-vmNames         = vm1          vm1          vm2          vm2       vm2;

hydra.ClientPrms-vmQuantities    = 1 1 1 1 1;
hydra.ClientPrms-vmThreads       = 5 5 5 5 1;

THREADGROUP cacheLoaderAndUpdater
  totalThreads = 1  totalVMs = 1  clientNames = client1a
  ;

THREADGROUP otherInitializers
  totalThreads = 1  totalVMs = 1  clientNames = client1b
  totalThreads = 1  totalVMs = 1  clientNames = client2a
  totalThreads = 1  totalVMs = 1  clientNames = client2b
  ;

THREADGROUP admin
  totalThreads = 1  totalVMs = 1  clientNames = client2c
  ;
