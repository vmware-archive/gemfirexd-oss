//// systemparams2.inc: two gemfire systems on localhost
INCLUDE $JTESTS/hydraconfig/gemfireprms.inc;

hydra.HostPrms-names          = host1      host2;

hydra.VmPrms-names            = vm1        vm2;
hydra.VmPrms-hostNames        = host1      host2;

hydra.GemFirePrms-names       = gemfire1   gemfire2;
hydra.GemFirePrms-hostNames   = host1      host2;

hydra.ClientPrms-names        = client1    client2;
hydra.ClientPrms-vmNames      = vm1        vm2;
hydra.ClientPrms-gemfireNames = gemfire1   gemfire2;

hydra.ClientPrms-vmQuantities = 1 1;
hydra.ClientPrms-vmThreads    = 4 4;
