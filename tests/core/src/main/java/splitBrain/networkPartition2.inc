// Tests including this file are expected to be run with MasterController
// on one host and two separate test systems (host1 and host2) for the VMs
// under test (locators and clients).
// This include provides clients and locators on each host
INCLUDE $JTESTS/hydraconfig/gemfireprms.inc;

hydra.HostPrms-names          = host1      host2;

hydra.VmPrms-names            = vm1        vm2       vm3        vm4;
hydra.VmPrms-hostNames        = host1      host1     host2      host2;

hydra.GemFirePrms-names       = gemfire1   gemfire2  gemfire3   gemfire4;
hydra.GemFirePrms-hostNames   = host1      host1     host2      host2;

hydra.ClientPrms-names        = client1    locator1  client2    locator2;
hydra.ClientPrms-vmNames      = vm1        vm2       vm3        vm4;
hydra.ClientPrms-gemfireNames = gemfire1   gemfire2  gemfire3   gemfire4;

hydra.ClientPrms-vmQuantities = 2          1         2          1;
hydra.ClientPrms-vmThreads    = 2          1         2          1;
