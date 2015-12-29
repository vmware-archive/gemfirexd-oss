// terminate warmup on lruEvictions
cacheperf.CachePerfPrms-LRUEvictions              = 100000;
cacheperf.CachePerfPrms-warmupTerminatorClass     = cacheperf.Terminators;
cacheperf.CachePerfPrms-warmupTerminatorMethod    = terminateOnLRUEvictions;
cacheperf.CachePerfPrms-warmupTerminatorFrequency = 60 seconds;

