include $JTESTS/cacheperf/comparisons/replicated/peerFeedData.inc;

hydra.RegionPrms-dataPolicy         = partition partition;
hydra.RegionPrms-partitionName      = data      feed;
hydra.RegionPrms-scope              = default   default;

hydra.PartitionPrms-names           = data      feed;
hydra.PartitionPrms-localMaxMemory  = default   0;
hydra.PartitionPrms-redundantCopies = ${redundantCopies};
hydra.PartitionPrms-totalNumBuckets = fcn ${bucketsPerDatahost} * ${dataHosts} * ${dataVMsPerHost} ncf;

cacheperf.CachePerfPrms-keyAllocationChunkSize = fcn ${bucketsPerDatahost} * ${dataHosts} * ${dataVMsPerHost} ncf;
