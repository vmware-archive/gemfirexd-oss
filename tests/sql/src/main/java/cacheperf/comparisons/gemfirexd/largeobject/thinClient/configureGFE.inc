include $JTESTS/hydraconfig/topology_2_locator.inc;
include $JTESTS/hydraconfig/performance.inc;

hydra.GemFirePrms-conserveSockets = ${conserveSockets};
hydra.GemFirePrms-distributedSystem =
  fcn "hydra.TestConfigFcns.duplicate(\"ds\", ${locatorHosts})" ncf
  fcn "hydra.TestConfigFcns.duplicate(\"loner\", ${clientHosts})" ncf
  fcn "hydra.TestConfigFcns.duplicate(\"ds\", ${serverHosts})" ncf
  ;

hydra.ConfigPrms-cacheConfig = cache;
hydra.ConfigPrms-bridgeConfig = bridge;

hydra.CachePrms-names = cache;

hydra.RegionPrms-names         = LARGEOBJECT  LARGEOBJECT_CLIENT;
hydra.RegionPrms-regionName    = LARGEOBJECT;
hydra.RegionPrms-dataPolicy    = partitioned  empty;
hydra.RegionPrms-partitionName = server       none;
hydra.RegionPrms-poolName      = none         pool;
hydra.RegionPrms-scope         = default      local;
//hydra.RegionPrms-valueConstraint = objects.query.largeobject.LargeObject;

hydra.PartitionPrms-names           = server;
hydra.PartitionPrms-localMaxMemory  = default;
hydra.PartitionPrms-redundantCopies = ${redundantCopies};
hydra.PartitionPrms-totalNumBuckets = fcn ${bucketsPerServer} * ${serverHosts} * ${serverVMsPerHost} ncf;

hydra.BridgePrms-names = bridge;
hydra.BridgePrms-maxThreads = ${maxThreads};

hydra.PoolPrms-names = pool;
hydra.PoolPrms-readTimeout = 10000000;
hydra.PoolPrms-subscriptionEnabled = false;
hydra.PoolPrms-threadLocalConnections = true;
