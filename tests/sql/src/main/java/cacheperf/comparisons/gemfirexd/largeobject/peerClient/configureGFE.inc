include $JTESTS/hydraconfig/topology_2_locator.inc;
include $JTESTS/hydraconfig/performance.inc;

hydra.GemFirePrms-conserveSockets =
  fcn "hydra.TestConfigFcns.duplicate(\"false\", ${locatorHosts})" ncf
  fcn "hydra.TestConfigFcns.duplicate(\"${conserveSockets}\", ${clientHosts})" ncf
  fcn "hydra.TestConfigFcns.duplicate(\"false\", ${serverHosts})" ncf
  ;
hydra.GemFirePrms-distributedSystem = ds;

hydra.ConfigPrms-cacheConfig = cache;

hydra.CachePrms-names = cache;

hydra.RegionPrms-names         = LARGEOBJECT  LARGEOBJECT_CLIENT;
hydra.RegionPrms-regionName    = LARGEOBJECT;
hydra.RegionPrms-dataPolicy    = partitioned;
hydra.RegionPrms-partitionName = server       client;
//hydra.RegionPrms-valueConstraint = objects.query.largeobject.LargeObject;

hydra.PartitionPrms-names           = server  client;
hydra.PartitionPrms-localMaxMemory  = default 0;
hydra.PartitionPrms-redundantCopies = ${redundantCopies};
hydra.PartitionPrms-totalNumBuckets = fcn ${bucketsPerServer} * ${serverHosts} * ${serverVMsPerHost} ncf;
