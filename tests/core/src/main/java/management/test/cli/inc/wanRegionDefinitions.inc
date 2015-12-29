//--- Pool Configurations -----------------------------------------------------------

hydra.PoolPrms-names            = poolSubscriptionEnabled;
hydra.PoolPrms-minConnections   = 2;
hydra.PoolPrms-subscriptionEnabled = true;
hydra.PoolPrms-subscriptionRedundancy = -1;

//--- WAN configuration --------------------------------------------------------------

hydra.GatewaySenderPrms-diskStoreName 	= senderDisk;

hydra.ConfigPrms-gatewaySenderConfig 		= sender1;
hydra.GatewaySenderPrms-names 				= sender1 parallelSender1
											  sender2 parallelSender2
											  sender3 parallelSender3
											  sender4 parallelSender4
											  sender5 parallelSender5; 

hydra.GatewaySenderPrms-parallel 			= false true
											  false true
											  false true
											  false true
											  false true;
											  
hydra.GatewaySenderPrms-remoteDistributedSystemsAlgorithm =  hydra.GatewaySenderHelper getRemoteDistributedSystems;
hydra.GatewaySenderPrms-batchSize			= ONEOF 100 200 500 FOENO;
hydra.GatewaySenderPrms-batchTimeInterval	= ONEOF 500 1000 2000 1500 2000 FOENO;
hydra.GatewaySenderPrms-maximumQueueMemory	= ONEOF 50 100 150 FOENO;
hydra.GatewaySenderPrms-socketBufferSize	= default;
hydra.GatewaySenderPrms-manualStart		    = false;


hydra.ConfigPrms-gatewayReceiverConfig 			= receiver;
hydra.GatewayReceiverPrms-names 				= receiver;
hydra.GatewayReceiverPrms-maximumTimeBetweenPings = RANGE 15000 60000 EGNAR;
hydra.GatewayReceiverPrms-socketBufferSize 		  = default;

//hydra.RegionPrms-gatewaySenderNames = sender;

hydra.GemFirePrms-remoteDistributedSystems =
     fcn "newWan.WANTest.generateRingRemoteDSName
          (\"ds_\", ${wanSites}, 1)" ncf         
     ,
     none
     ;

//--- Disk-store Configurations ------------------------------------------------------


hydra.DiskStorePrms-names = disk1 disk2 disk3 disk4 senderDisk;
hydra.DiskStorePrms-diskDirNum = 4;
hydra.DiskStorePrms-allowForceCompaction = ONEOF true false FOENO; 
hydra.DiskStorePrms-autoCompact = ONEOF true false FOENO;
hydra.DiskStorePrms-compactionThreshold = RANGE 40 100 EGNAR;
hydra.DiskStorePrms-queueSize = ONEOF 1 5 10 20 FOENO;
hydra.DiskStorePrms-timeInterval = oneof 1 10 50 500 1000 2000 foeno;


//--- WAN Replicated Regions -------------------------------------------------
hydra.RegionPrms-names          = WANReplicatedAckBridge;
hydra.RegionPrms-regionName     = TestWANReplicatedAckRegion;
hydra.RegionPrms-poolName       = none;
hydra.RegionPrms-dataPolicy     = replicate;
hydra.RegionPrms-scope          = ack;
hydra.RegionPrms-cacheListeners = management.operations.OperationsCacheListener;
hydra.RegionPrms-partitionName  = none;
hydra.RegionPrms-diskStoreName  = none;
//hydra.RegionPrms-gatewaySenderNames = 	ONEOF 
//											sender1 
//											sender2 
//											sender3 
//											sender4 
//											sender5
//										FOENO;


management.operations.OperationPrms-regionList = WANReplicatedAckBridge;

//--- PartitionRegion Partition Configurations -------------------------------------- 

hydra.PartitionPrms-names           = prRedundancy_1;
hydra.PartitionPrms-localMaxMemory  = default;
hydra.PartitionPrms-totalNumBuckets = 20;
hydra.PartitionPrms-redundantCopies = 1;
hydra.PartitionPrms-colocatedWith   = none;

hydra.PartitionPrms-names           += prRedundancy_1_c;
hydra.PartitionPrms-localMaxMemory  += default;
hydra.PartitionPrms-totalNumBuckets += 20;
hydra.PartitionPrms-redundantCopies += 1;
hydra.PartitionPrms-colocatedWith   += TestWANPartition_2;

hydra.PartitionPrms-names           += prRedundancy_2;
hydra.PartitionPrms-localMaxMemory  += default;
hydra.PartitionPrms-totalNumBuckets += 20;
hydra.PartitionPrms-redundantCopies += 2;
hydra.PartitionPrms-colocatedWith   += none;

//hydra.PartitionPrms-names           += prRedundancy_N;
//hydra.PartitionPrms-localMaxMemory  += default;
//hydra.PartitionPrms-totalNumBuckets += 20;
//hydra.PartitionPrms-redundantCopies += ${redundantCopies};


//--- WAN Partition Regions  ----------------------------------------

hydra.RegionPrms-names          += WANPartitionBridge;
hydra.RegionPrms-regionName     += TestWANPartitionRegion;
hydra.RegionPrms-poolName       += none;
hydra.RegionPrms-dataPolicy     += partition;
hydra.RegionPrms-scope          += default;
hydra.RegionPrms-partitionName  += prRedundancy_1;
//hydra.RegionPrms-cacheListeners += management.operations.OperationsCacheListener;
hydra.RegionPrms-diskStoreName  += none;
//hydra.RegionPrms-gatewaySenderNames += ONEOF 
//											parallelSender1 
//											parallelSender2 
//											parallelSender3 
//											parallelSender4 
//											parallelSender5 
//									FOENO;

management.operations.OperationPrms-regionList += WANPartitionBridge;


//Need to be colocated in order to have same sender linked
hydra.RegionPrms-names          += WANPartitionCBridge;
hydra.RegionPrms-regionName     += TestWANPartitionRegion;
hydra.RegionPrms-poolName       += none;
hydra.RegionPrms-dataPolicy     += partition;
hydra.RegionPrms-scope          += default;
hydra.RegionPrms-partitionName  += prRedundancy_1_c;
//hydra.RegionPrms-cacheListeners += management.operations.OperationsCacheListener;

//--- Persistent Co-located Partition Regions  -----------------------------


//--- START Tasks for reading descriptors --------------------------------------------

STARTTASK
  taskClass = management.operations.ops.RegionOperations 
  taskMethod = HydraStartTask_ReadRegionSpecs
  ;