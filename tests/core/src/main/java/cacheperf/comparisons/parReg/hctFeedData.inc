include $JTESTS/cacheperf/comparisons/replicated/hctFeedData.inc;

hydra.RegionPrms-dataPolicy     = partition partition empty;
hydra.RegionPrms-partitionName  = feed      bridge    none;
hydra.RegionPrms-scope          = default   default   local;

hydra.PartitionPrms-names           = feed bridge;
hydra.PartitionPrms-localMaxMemory  = 0    default;
hydra.PartitionPrms-redundantCopies = ${redundantCopies};
hydra.PartitionPrms-totalNumBuckets = fcn ${bucketsPerDatahost} * ${bridgeHosts} * ${bridgeVMsPerHost} ncf;

cacheperf.CachePerfPrms-keyAllocationChunkSize = fcn ${bucketsPerDatahost} * ${bridgeHosts} * ${bridgeVMsPerHost} ncf;
