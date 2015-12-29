include $JTESTS/cacheperf/comparisons/replicated/peerData.inc;

hydra.RegionPrms-dataPolicy = partition;
hydra.RegionPrms-partitionName = partition;
hydra.RegionPrms-scope = default;

hydra.PartitionPrms-names = partition;
hydra.PartitionPrms-redundantCopies = ${redundantCopies};
hydra.PartitionPrms-totalNumBuckets = fcn ${bucketsPerDatahost} * ${peerHosts} * ${peerVMsPerHost} ncf;

cacheperf.CachePerfPrms-keyAllocationChunkSize = fcn ${bucketsPerDatahost} * ${peerHosts} * ${peerVMsPerHost} ncf;
