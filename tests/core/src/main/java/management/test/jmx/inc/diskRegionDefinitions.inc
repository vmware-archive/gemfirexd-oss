//--- Pool Configurations -----------------------------------------------------------

hydra.PoolPrms-names            = poolSubscriptionEnabled;
hydra.PoolPrms-minConnections   = 2;
hydra.PoolPrms-subscriptionEnabled = true;
hydra.PoolPrms-subscriptionRedundancy = -1;

//--- Disk-store Configurations ------------------------------------------------------

hydra.DiskStorePrms-names = disk1 disk2 disk3 disk4;
hydra.DiskStorePrms-diskDirNum = 4;
hydra.DiskStorePrms-allowForceCompaction = ONEOF true false FOENO; 
hydra.DiskStorePrms-autoCompact = ONEOF true false FOENO;
hydra.DiskStorePrms-compactionThreshold = RANGE 40 100 EGNAR;
hydra.DiskStorePrms-queueSize = ONEOF 1 5 10 20 FOENO;
hydra.DiskStorePrms-timeInterval = oneof 1 10 50 500 1000 2000 foeno;


//--- Persistent Replicated Regions -------------------------------------------------

hydra.RegionPrms-names           += PersistentReplicatedBridge;
hydra.RegionPrms-regionName      += TestPersistentReplicatedRegion;
hydra.RegionPrms-poolName        += none;
hydra.RegionPrms-dataPolicy      += persistentReplicate;
hydra.RegionPrms-scope           += default;
hydra.RegionPrms-cacheListeners  += management.operations.OperationsCacheListener;
hydra.RegionPrms-partitionName   += none;
hydra.RegionPrms-diskStoreName   += ONEOF disk1 disk2 disk3 disk4 FOENO;
hydra.RegionPrms-diskSynchronous += ONEOF true false FOENO;

//hydra.RegionPrms-names          += PersistentReplicatedEdge;
//hydra.RegionPrms-regionName     += TestPersistentReplicatedRegion;
//hydra.RegionPrms-poolName       += poolSubscriptionEnabled;
//hydra.RegionPrms-dataPolicy     += default;
//hydra.RegionPrms-scope          += local;
//hydra.RegionPrms-cacheListeners += management.operations.OperationsCacheListener;
//hydra.RegionPrms-partitionName  += none;
//hydra.RegionPrms-diskStoreName  += ONEOF disk1 disk2 disk3 disk4 FOENO;
//hydra.RegionPrms-diskSynchronous += ONEOF true false FOENO;

management.operations.OperationPrms-regionList = PersistentReplicatedRegion;

//--- PartitionRegion Partition Configurations -------------------------------------- 

hydra.PartitionPrms-names           = prRedundancy_1;
hydra.PartitionPrms-localMaxMemory  = default;
hydra.PartitionPrms-totalNumBuckets = 20;
hydra.PartitionPrms-redundantCopies = 1;

hydra.PartitionPrms-names           += prRedundancy_2;
hydra.PartitionPrms-localMaxMemory  += default;
hydra.PartitionPrms-totalNumBuckets += 20;
hydra.PartitionPrms-redundantCopies += 2;

//hydra.PartitionPrms-names           += prRedundancy_N;
//hydra.PartitionPrms-localMaxMemory  += default;
//hydra.PartitionPrms-totalNumBuckets += 20;
//hydra.PartitionPrms-redundantCopies += ${redundantCopies};


//--- Persistent Partition Regions  ----------------------------------------

hydra.RegionPrms-names           += PersistentPartitionBridge;
hydra.RegionPrms-regionName      += TestPersistentPartitionRegion;
hydra.RegionPrms-poolName        += none;
hydra.RegionPrms-dataPolicy      += persistentPartition;
hydra.RegionPrms-scope           += default;
hydra.RegionPrms-partitionName   += prRedundancy_1;
//hydra.RegionPrms-cacheListeners  += management.operations.OperationsCacheListener;
hydra.RegionPrms-diskStoreName   += ONEOF disk1 disk2 disk3 disk4 FOENO;
hydra.RegionPrms-diskSynchronous += ONEOF true false FOENO;

//hydra.RegionPrms-names           += PersistentPartitionEdge;
//hydra.RegionPrms-regionName      += TestPersistentPartitionRegion;
//hydra.RegionPrms-poolName        += poolSubscriptionEnabled;
//hydra.RegionPrms-dataPolicy      += default;
//hydra.RegionPrms-scope           += local;
//hydra.RegionPrms-partitionName   += none;
//hydra.RegionPrms-cacheListeners  += management.operations.OperationsCacheListener;
//hydra.RegionPrms-diskStoreName   += ONEOF disk1 disk2 disk3 disk4 FOENO;
//hydra.RegionPrms-diskSynchronous += ONEOF true false FOENO;

management.operations.OperationPrms-regionList += PersistentPartitionBridge;

//--- Persistent Co-located Partition Regions  -----------------------------


//--- START Tasks for reading descriptors --------------------------------------------

STARTTASK
  taskClass = management.operations.ops.RegionOperations 
  taskMethod = HydraStartTask_ReadRegionSpecs
  ;