include $JTESTS/cacheperf/comparisons/replicated/peerData.inc;

hydra.RegionPrms-dataPolicy = partition;
hydra.RegionPrms-partitionName = partition;
hydra.RegionPrms-scope = default;

hydra.PartitionPrms-names = partition;
hydra.PartitionPrms-fixedPartitionName = fixed;
hydra.PartitionPrms-partitionResolver =
      cacheperf.comparisons.parReg.fixed.KeyResolver;
hydra.PartitionPrms-redundantCopies = ${redundantCopies};
hydra.PartitionPrms-totalNumBuckets =
      fcn ${bucketsPerPartition} * ${partitionsPerDatahost}
          *
          ${peerHosts} * ${peerVMsPerHost}
      ncf;

hydra.FixedPartitionPrms-names = fixed;
hydra.FixedPartitionPrms-datastores = fcn ${peerHosts} * ${peerVMsPerHost} ncf;
hydra.FixedPartitionPrms-partitionNames =
  fcn "hydra.TestConfigFcns.generateNames
       (\"P\", ${partitionsPerDatahost} * ${peerHosts} * ${peerVMsPerHost})"
  ncf;
hydra.FixedPartitionPrms-partitionBuckets = ${bucketsPerPartition};

cacheperf.CachePerfPrms-keyAllocationChunkSize = fcn ${bucketsPerPartition} * ${partitionsPerDatahost} * ${peerHosts} * ${peerVMsPerHost} ncf;
