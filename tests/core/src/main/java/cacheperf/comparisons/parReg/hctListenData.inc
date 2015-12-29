include $JTESTS/cacheperf/comparisons/replicated/hctListenData.inc;

hydra.RegionPrms-dataPolicy     = partition partition empty;
hydra.RegionPrms-partitionName  = listen    bridge    none;
hydra.RegionPrms-scope          = default   default   local;

hydra.PartitionPrms-names           = listen bridge;
hydra.PartitionPrms-localMaxMemory  = 0      default;
hydra.PartitionPrms-redundantCopies = ${redundantCopies};
hydra.PartitionPrms-totalNumBuckets = fcn ${bucketsPerDatahost} * ${bridgeHosts} * ${bridgeVMsPerHost} ncf;

cacheperf.CachePerfPrms-keyAllocationChunkSize = fcn ${bucketsPerDatahost} * ${bridgeHosts} * ${bridgeVMsPerHost} ncf;
