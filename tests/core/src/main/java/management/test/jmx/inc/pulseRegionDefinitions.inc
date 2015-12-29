

//--- Pool Configurations -----------------------------------------------------------

hydra.PoolPrms-names            = poolSubscriptionEnabled;
hydra.PoolPrms-minConnections   = 2;
hydra.PoolPrms-subscriptionEnabled = true;
hydra.PoolPrms-subscriptionRedundancy = -1;
hydra.PoolPrms-statisticInterval = 500;

//--- Disk-store Configurations ------------------------------------------------------
hydra.DiskStorePrms-names = disk1 disk2 disk3 disk4 default;
hydra.DiskStorePrms-diskDirNum = 4;
hydra.DiskStorePrms-allowForceCompaction = ONEOF true false FOENO; 
hydra.DiskStorePrms-autoCompact = ONEOF true false FOENO;
hydra.DiskStorePrms-compactionThreshold = RANGE 40 100 EGNAR;
hydra.DiskStorePrms-queueSize = ONEOF 1 5 10 20 FOENO;
hydra.DiskStorePrms-timeInterval = oneof 1 10 50 500 1000 2000 foeno;

//--- Replicated Regions with Scope : ACK --------------------------------------------

hydra.RegionPrms-names          = ReplicatedAckBridge;
hydra.RegionPrms-regionName     = TestReplicatedAckRegion;
hydra.RegionPrms-poolName       = none;
hydra.RegionPrms-dataPolicy     = replicate;
hydra.RegionPrms-scope          = ack;
hydra.RegionPrms-cacheListeners = management.operations.OperationsCacheListener;
hydra.RegionPrms-partitionName  = none;
hydra.RegionPrms-diskStoreName  = none;

hydra.RegionPrms-names          += ReplicatedAckEdge;
hydra.RegionPrms-regionName     += TestReplicatedAckRegion;
hydra.RegionPrms-poolName       += poolSubscriptionEnabled;
hydra.RegionPrms-dataPolicy     += default;
hydra.RegionPrms-scope          += local;
//hydra.RegionPrms-cacheListeners += management.operations.OperationsCacheListener;
hydra.RegionPrms-partitionName  += none;
hydra.RegionPrms-diskStoreName  += none;

//--- Replicated Regions with Scope : NO ACK ----------------------------------------

hydra.RegionPrms-names          += ReplicatedNoAckBridge;
hydra.RegionPrms-regionName     += TestReplicatedNoAckRegion;
hydra.RegionPrms-poolName       += none;
hydra.RegionPrms-dataPolicy     += replicate;
hydra.RegionPrms-scope          += noAck;
//hydra.RegionPrms-cacheListeners += management.operations.OperationsCacheListener;
hydra.RegionPrms-partitionName  += none;
hydra.RegionPrms-diskStoreName  += none;

hydra.RegionPrms-names          += ReplicatedNoAckEdge;
hydra.RegionPrms-regionName     += TestReplicatedNoAckRegion;
hydra.RegionPrms-poolName       += poolSubscriptionEnabled;
hydra.RegionPrms-dataPolicy     += default;
hydra.RegionPrms-scope          += local;
//hydra.RegionPrms-cacheListeners += management.operations.OperationsCacheListener;
hydra.RegionPrms-partitionName  += none;
hydra.RegionPrms-diskStoreName  += none;

management.operations.OperationPrms-regionList = ReplicatedAckRegion ReplicatedNoAckRegion;
 
//--- Persistent Replicated Regions -------------------------------------------------

hydra.RegionPrms-names          += PersistentReplicatedBridge;
hydra.RegionPrms-regionName     += TestPersistentReplicatedRegion;
hydra.RegionPrms-poolName       += none;
hydra.RegionPrms-dataPolicy     += persistentReplicate;
hydra.RegionPrms-scope          += default;
//hydra.RegionPrms-cacheListeners += management.operations.OperationsCacheListener;
hydra.RegionPrms-partitionName  += none;
hydra.RegionPrms-diskStoreName  += ONEOF disk1 disk2 disk3 disk4 FOENO;

hydra.RegionPrms-names          += PersistentReplicatedEdge;
hydra.RegionPrms-regionName     += TestPersistentReplicatedRegion;
hydra.RegionPrms-poolName       += poolSubscriptionEnabled;
hydra.RegionPrms-dataPolicy     += default;
hydra.RegionPrms-scope          += local;
//hydra.RegionPrms-cacheListeners += management.operations.OperationsCacheListener;
hydra.RegionPrms-partitionName  += none;
hydra.RegionPrms-diskStoreName  += none;

management.operations.OperationPrms-regionList += PersistentReplicatedRegion;

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



//--- Partition Regions  ----------------------------------------

hydra.RegionPrms-names          += PartitionBridge;
hydra.RegionPrms-regionName     += TestPartitionRegion;
hydra.RegionPrms-poolName       += none;
hydra.RegionPrms-dataPolicy     += partition;
hydra.RegionPrms-scope          += default;
hydra.RegionPrms-partitionName  += prRedundancy_1;
//hydra.RegionPrms-cacheListeners += management.operations.OperationsCacheListener;
hydra.RegionPrms-diskStoreName  += none;

hydra.RegionPrms-names          += PartitionEdge;
hydra.RegionPrms-regionName     += TestPartitionRegion;
hydra.RegionPrms-poolName       += poolSubscriptionEnabled;
hydra.RegionPrms-dataPolicy     += default;
hydra.RegionPrms-scope          += local;
hydra.RegionPrms-partitionName  += none;
//hydra.RegionPrms-cacheListeners += management.operations.OperationsCacheListener;
hydra.RegionPrms-diskStoreName  += none;

management.operations.OperationPrms-regionList += PartitionRegion;

//--- Persistent Partition Regions  ----------------------------------------

hydra.RegionPrms-names           += PersistentPartitionBridge;
hydra.RegionPrms-regionName      += TestPersistentPartitionRegion;
hydra.RegionPrms-poolName        += none;
hydra.RegionPrms-dataPolicy      += persistentPartition;
hydra.RegionPrms-scope           += default;
hydra.RegionPrms-partitionName   += prRedundancy_1;
//hydra.RegionPrms-cacheListeners  += management.operations.OperationsCacheListener;
hydra.RegionPrms-diskStoreName   += ONEOF disk1 disk2 disk3 disk4 FOENO;

hydra.RegionPrms-names          += PersistentPartitionEdge;
hydra.RegionPrms-regionName     += TestPersistentPartitionRegion;
hydra.RegionPrms-poolName       += poolSubscriptionEnabled;
hydra.RegionPrms-dataPolicy     += default;
hydra.RegionPrms-scope          += local;
hydra.RegionPrms-partitionName  += none;
//hydra.RegionPrms-cacheListeners += management.operations.OperationsCacheListener;
hydra.RegionPrms-diskStoreName  += none;


//--- Co-located Partition Regions  -----------------------------

//--- Persistent Co-located Partition Regions  -----------------------------


//--- Default Operations Region ----------------------------------------------------

hydra.RegionPrms-names          += OperationsRegion;
hydra.RegionPrms-regionName     += OperationsRegion;
hydra.RegionPrms-poolName       += none;
hydra.RegionPrms-dataPolicy     += replicate;
hydra.RegionPrms-scope          += ack;
//hydra.RegionPrms-cacheListeners += management.operations.OperationsCacheListener;
hydra.RegionPrms-partitionName  += none;
hydra.RegionPrms-diskStoreName  += none;


//--- START Tasks for reading descriptors --------------------------------------------

STARTTASK
  taskClass = management.operations.ops.RegionOperations 
  taskMethod = HydraStartTask_ReadRegionSpecs
  ;