include $JTESTS/cacheperf/comparisons/replicated/peerFeedData.inc;

hydra.RegionPrms-dataPolicy         = partition partition;
hydra.RegionPrms-partitionName      = data      feed;
hydra.RegionPrms-scope              = default   default;

hydra.PartitionPrms-names           = data      feed;
hydra.PartitionPrms-localMaxMemory  = default   0;
hydra.PartitionPrms-fixedPartitionName = fixed;
hydra.PartitionPrms-partitionResolver = cacheperf.comparisons.parReg.fixed.KeyResolver;
hydra.PartitionPrms-redundantCopies = ${redundantCopies};
hydra.PartitionPrms-totalNumBuckets = fcn ${bucketsPerPartition} * ${partitionsPerDatahost} * ${dataHosts} * ${dataVMsPerHost} ncf;

hydra.FixedPartitionPrms-names = fixed;
hydra.FixedPartitionPrms-datastores = fcn ${dataHosts} * ${dataVMsPerHost} ncf;
hydra.FixedPartitionPrms-partitionNames =
  fcn "hydra.TestConfigFcns.generateNames
       (\"P\", ${partitionsPerDatahost} * ${dataHosts} * ${dataVMsPerHost})"
  ncf;
hydra.FixedPartitionPrms-partitionBuckets = ${bucketsPerPartition};

cacheperf.CachePerfPrms-keyAllocationChunkSize = fcn ${bucketsPerPartition} * ${partitionsPerDatahost} * ${dataHosts} * ${dataVMsPerHost} ncf;
