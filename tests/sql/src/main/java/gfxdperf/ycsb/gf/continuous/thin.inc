hydra.Prms-testDescription += "Uses thin clients.";

hydra.GemFirePrms-distributedSystem =
  fcn "hydra.TestConfigFcns.duplicate(\"ds\", ${locatorHosts})" ncf
  fcn "hydra.TestConfigFcns.duplicate(\"loner\",${clientHosts})" ncf
  fcn "hydra.TestConfigFcns.duplicate(\"ds\", ${serverHosts})" ncf;

hydra.RegionPrms-dataPolicy    = partition empty;
hydra.RegionPrms-partitionName = server    none;
hydra.RegionPrms-scope         = default   local;

hydra.PartitionPrms-names           = server;
hydra.PartitionPrms-localMaxMemory  = default;
hydra.PartitionPrms-redundantCopies = 1;
hydra.PartitionPrms-totalNumBuckets = fcn ${bucketsPerServer} * ${serverHosts} * ${serverVMsPerHost} ncf;

hydra.ConfigPrms-bridgeConfig = bridge;
hydra.BridgePrms-names = bridge;

hydra.PoolPrms-names = pool;
