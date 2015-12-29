//// systemparams1.inc: one gemfire system on localhost
INCLUDE $JTESTS/hydraconfig/gemfireprms.inc;

hydra.HostPrms-names          = host;

hydra.VmPrms-names            = vm;
hydra.VmPrms-hostNames        = host;

hydra.GemFirePrms-names       = gemfire;
hydra.GemFirePrms-hostNames   = host;

hydra.ClientPrms-names        = client;
hydra.ClientPrms-vmNames      = vm;
hydra.ClientPrms-gemfireNames = gemfire;

hydra.ClientPrms-vmQuantities = 2;
hydra.ClientPrms-vmThreads    = 4;
