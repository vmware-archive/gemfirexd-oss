hydra.Prms-testRequirement = "
   Consistent response times while scaling to 6G total data with 10% MemLRU Eviction using shared regions";
hydra.Prms-testDescription = "
      30 concurrent threads in each of 2 VMs doing gets and use a CacheLoader
      2 gemfire systems on 2 different machines
      4G shared memory per gemfire system
      single shared region
      scope = noAck
      cache entry value = 5K byte[] 
      eviction occurs via a MemLRUCapacityController
      each shared region holds 3G of data
      10% cache misses (which generates an eviction)
";

INCLUDE $JTESTS/cacheperf/gemfire/evict/evict.inc;

hydra.RegionPrms-initialCapacity = 700000;

hydra.Prms-totalTaskTimeSec=18000;

// terminate warmup on lruEvictions
cacheperf.CachePerfPrms-LRUEvictions              = 100000;
cacheperf.CachePerfPrms-warmupTerminatorClass     = cacheperf.Terminators;
cacheperf.CachePerfPrms-warmupTerminatorMethod    = terminateOnLRUEvictions;
cacheperf.CachePerfPrms-warmupTerminatorFrequency = 60 seconds;

// Terminate the work on number of iterations
cacheperf.CachePerfPrms-taskTerminatorClass       = cacheperf.Terminators;
cacheperf.CachePerfPrms-taskTerminatorMethod      = terminateOnWorkIterations;
cacheperf.CachePerfPrms-taskTerminatorFrequency   = 1 iterations;
cacheperf.CachePerfPrms-workIterations            = 1000000000; // per entire test
cacheperf.CachePerfPrms-batchSize                 = 100000;

// fill to the first LRU eviction
INITTASK  batch taskClass = cacheperf.CachePerfClient taskMethod = getDataTask
                cacheperf.CachePerfPrms-resetKeysAfterTaskEnds   = false
                cacheperf.CachePerfPrms-LRUEvictions             = 1
                cacheperf.CachePerfPrms-taskTerminatorClass      = cacheperf.Terminators 
                cacheperf.CachePerfPrms-taskTerminatorMethod     = terminateOnLRUEvictions
                cacheperf.CachePerfPrms-taskTerminatorFrequency  = 30 seconds;

// warmup with the proper miss/hit percentage
TASK      taskClass = cacheperf.CachePerfClient taskMethod   = getRecentKeyDataTask 
          cacheperf.CachePerfPrms-resetKeysBeforeTaskStarts  = false;

cacheperf.CachePerfPrms-workIterations=1000000000;
cacheperf.CachePerfPrms-recentKeysPercentage        = 90;    // 10 percent misses
cacheperf.CachePerfPrms-maxRecentKeys               = 100;   // use no more than the last 100 recent keys
hydra.RegionPrms-evictionAttributes = lruMemorySize 3000; // 3G per

// run in 64-bit mode -- Solaris only
hydra.VmPrms-type = d64;
hydra.VmPrms-extraVMArgs = "-XX:MaxDirectMemorySize=128M -XX:+UseConcMarkSweepGC -XX:+DisableExplicitGC -XX:+UseParNewGC";
