//// systemparams4.inc: four gemfire systems on localhost
INCLUDE $JTESTS/hydraconfig/gemfireprms.inc;

hydra.HostPrms-names          = host1      host2      host3      host4;

hydra.VmPrms-names            = vm1        vm2        vm3        vm4;
hydra.VmPrms-hostNames        = host1      host2      host3      host4;

hydra.GemFirePrms-names       = gemfire1   gemfire2   gemfire3   gemfire4;
hydra.GemFirePrms-hostNames   = host1      host2      host3      host4;

hydra.ClientPrms-names        = client1    client2    client3    client4;
hydra.ClientPrms-vmNames      = vm1        vm2        vm3        vm4;
hydra.ClientPrms-gemfireNames = gemfire1   gemfire2   gemfire3   gemfire4;

hydra.ClientPrms-vmQuantities = 1 1 1 1;
hydra.ClientPrms-vmThreads    = 4 4 4 4;
