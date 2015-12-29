hydra.Prms-testDescription += "Uses peer clients.";

hydra.GemFirePrms-distributedSystem = ds;

hydra.RegionPrms-dataPolicy    = partition partition;
hydra.RegionPrms-partitionName = server    client;
hydra.RegionPrms-scope         = default   default;

hydra.PartitionPrms-names           = server    client;
hydra.PartitionPrms-localMaxMemory  = default   0;
hydra.PartitionPrms-redundantCopies = 1;
hydra.PartitionPrms-totalNumBuckets = fcn ${bucketsPerServer} * ${serverHosts} * ${serverVMsPerHost} ncf;
