hydra.GatewaySenderPrms-names           = sender;
hydra.GatewaySenderPrms-parallel        = false;
hydra.GatewaySenderPrms-remoteDistributedSystemsAlgorithm =  hydra.GatewaySenderHelper getRemoteDistributedSystems;

hydra.GatewaySenderPrms-diskStoreName   = senderDisk;
hydra.DiskStorePrms-names               = disk senderDisk;

hydra.GatewayReceiverPrms-names     	= receiver;
