include $JTESTS/hydraconfig/topology_1_locator.inc;
include $JTESTS/hydraconfig/performance.inc;

hydra.ConfigPrms-cacheConfig = cache;

hydra.CachePrms-names = cache;

hydra.RegionPrms-names         = TINYOBJECT;
hydra.RegionPrms-regionName    = TINYOBJECT;
hydra.RegionPrms-dataPolicy    = partitioned ;
hydra.RegionPrms-partitionName = partition;
//hydra.RegionPrms-valueConstraint = objects.query.tinyobject.TinyObject;

hydra.PartitionPrms-names = partition;
hydra.PartitionPrms-localMaxMemory = default;
hydra.PartitionPrms-redundantCopies = ${redundantCopies};
hydra.PartitionPrms-totalNumBuckets = fcn ${bucketsPerServer} * ${clientserverHosts} * ${clientserverVMsPerHost} ncf;
