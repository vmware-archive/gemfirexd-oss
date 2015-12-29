// terminate warmup on numDestroys, which is the eviction action
// specified by expiration tests
cacheperf.CachePerfPrms-warmupTerminatorClass     = cacheperf.Terminators;
cacheperf.CachePerfPrms-warmupTerminatorMethod    = terminateOnNumDestroys;
cacheperf.CachePerfPrms-warmupTerminatorFrequency = 60 seconds;
cacheperf.CachePerfPrms-numDestroys               = 100000;

