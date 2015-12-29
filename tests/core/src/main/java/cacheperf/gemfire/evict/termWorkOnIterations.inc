// Terminate the work on number of iterations
cacheperf.CachePerfPrms-taskTerminatorClass       = cacheperf.Terminators;
cacheperf.CachePerfPrms-taskTerminatorMethod      = terminateOnWorkIterations;
cacheperf.CachePerfPrms-taskTerminatorFrequency   = 1 iterations;
cacheperf.CachePerfPrms-workIterations            = 10000000; // per entire test
cacheperf.CachePerfPrms-batchSize                 = 100000;

