include $JTESTS/hydraconfig/topology_1_locator.inc;
include $JTESTS/hydraconfig/performance.inc;

hydra.ConfigPrms-cacheConfig = cache;

hydra.GemFirePrms-conserveSockets = ${conserveSockets};

hydra.CachePrms-names = cache;

hydra.RegionPrms-names         = LARGEOBJECT;
hydra.RegionPrms-regionName    = LARGEOBJECT;
hydra.RegionPrms-dataPolicy    = partitioned ;
hydra.RegionPrms-partitionName = partition;
//hydra.RegionPrms-valueConstraint = objects.query.largeobject.LargeObject;

hydra.PartitionPrms-names = partition;
hydra.PartitionPrms-localMaxMemory = default;
hydra.PartitionPrms-redundantCopies = ${redundantCopies};
hydra.PartitionPrms-totalNumBuckets = fcn ${bucketsPerServer} * ${clientserverHosts} * ${clientserverVMsPerHost} ncf;
