//----  INCLUDE FILE TO USE IN ALL WAN TESTS

hydra.ConfigPrms-gatewaySenderConfig 		= sender;
hydra.GatewaySenderPrms-names 				= sender;
hydra.GatewaySenderPrms-parallel 			= false;
hydra.GatewaySenderPrms-remoteDistributedSystemsAlgorithm =  hydra.GatewaySenderHelper getRemoteDistributedSystems;
hydra.GatewaySenderPrms-batchSize			= ONEOF 100 200 500 FOENO;
hydra.GatewaySenderPrms-batchTimeInterval	= ONEOF 500 1000 2000 1500 2000 FOENO;
hydra.GatewaySenderPrms-maximumQueueMemory	= ONEOF 50 100 150 FOENO;
hydra.GatewaySenderPrms-socketBufferSize	= default;
hydra.GatewaySenderPrms-manualStart		    = false;

hydra.GatewaySenderPrms-diskStoreName 	= senderDisk;
hydra.DiskStorePrms-names 				= disk senderDisk;
hydra.DiskStorePrms-queueSize 			= default;
hydra.DiskStorePrms-timeInterval 		= default;

hydra.ConfigPrms-gatewayReceiverConfig 			= receiver;
hydra.GatewayReceiverPrms-names 				= receiver;
hydra.GatewayReceiverPrms-maximumTimeBetweenPings = RANGE 15000 60000 EGNAR;
hydra.GatewayReceiverPrms-socketBufferSize 		  = default;

hydra.RegionPrms-gatewaySenderNames = sender;

hydra.GemFirePrms-remoteDistributedSystems =
     fcn "newWan.WANTest.generateRingRemoteDSName
          (\"ds_\", ${wanSites}, ${locatorHostsPerSite})" ncf         
     ,
     none
     ;  
