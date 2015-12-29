//// systemparams3.inc: three gemfire systems on localhost
INCLUDE $JTESTS/hydraconfig/gemfireprms.inc;

hydra.HostPrms-names          = host1      host2      host3;

hydra.VmPrms-names            = vm1        vm2        vm3;
hydra.VmPrms-hostNames        = host1      host2      host3;

hydra.GemFirePrms-names       = gemfire1   gemfire2   gemfire3;
hydra.GemFirePrms-hostNames   = host1      host2      host3;

hydra.ClientPrms-names        = client1    client2    client3;
hydra.ClientPrms-vmNames      = vm1        vm2        vm3;
hydra.ClientPrms-gemfireNames = gemfire1   gemfire2   gemfire3;

hydra.ClientPrms-vmQuantities = 1 1 1;
hydra.ClientPrms-vmThreads    = 4 4 4;
