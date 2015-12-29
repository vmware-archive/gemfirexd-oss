hydra.PartitionPrms-names           += prRedundancy_2;
hydra.PartitionPrms-localMaxMemory  += default;
hydra.PartitionPrms-totalNumBuckets += RANGE 20 59 EGNAR;
hydra.PartitionPrms-redundantCopies += 2;


hydra.RegionPrms-names          += PartitionBridge;
hydra.RegionPrms-regionName     += TestPartitionStringRegion;
hydra.RegionPrms-poolName       += none;
hydra.RegionPrms-dataPolicy     += partition;
hydra.RegionPrms-scope          += default;
hydra.RegionPrms-partitionName  += prRedundancy_2;
hydra.RegionPrms-cacheListeners += none management.operations.OperationsCacheListener;
hydra.RegionPrms-diskStoreName  += none;

hydra.RegionPrms-names          += PartitionJSONBridge;
hydra.RegionPrms-regionName     += TestPartitionJSONRegion;
hydra.RegionPrms-poolName       += none;
hydra.RegionPrms-dataPolicy     += partition;
hydra.RegionPrms-scope          += default;
hydra.RegionPrms-partitionName  += prRedundancy_2;
//hydra.RegionPrms-cacheListeners += none management.operations.OperationsCacheListener;
hydra.RegionPrms-diskStoreName  += none;


hydra.RegionPrms-names          += ReplicatedNoAckBridge;
hydra.RegionPrms-regionName     += TestReplicatedNoAckChildRegion;
hydra.RegionPrms-poolName       += none;
hydra.RegionPrms-dataPolicy     += replicate;
hydra.RegionPrms-scope          += noAck;
//hydra.RegionPrms-cacheListeners += none management.operations.OperationsCacheListener;
hydra.RegionPrms-partitionName  += none;
hydra.RegionPrms-diskStoreName  += none;

management.operations.OperationPrms-regionList += PartitionRegion PartitionRegion ReplicatedNoAckBridge;