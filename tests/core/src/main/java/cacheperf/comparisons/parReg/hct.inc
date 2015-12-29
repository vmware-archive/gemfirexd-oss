include $JTESTS/cacheperf/comparisons/replicated/hct.inc;

hydra.RegionPrms-dataPolicy    = partition empty;
hydra.RegionPrms-partitionName = bridge    none;
hydra.RegionPrms-scope         = default   local;

hydra.PartitionPrms-names = bridge;
hydra.PartitionPrms-redundantCopies = ${redundantCopies};
hydra.PartitionPrms-totalNumBuckets = fcn ${bucketsPerDatahost} * ${bridgeHosts} * ${bridgeVMsPerHost} ncf;

cacheperf.CachePerfPrms-keyAllocationChunkSize = fcn ${bucketsPerDatahost} * ${bridgeHosts} * ${bridgeVMsPerHost} ncf;
