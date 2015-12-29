statspec totalOps * cacheperf.CachePerfStats * operations
filter=none combine=combineAcrossArchives ops=max-min! trimspec=operations
;

// time to do a complete connection cycle
statspec totalOpTime * cacheperf.CachePerfStats * operationTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=operations
;
expr totalCycleTime = totalOpTime / totalOps ops=max-min?
;

// time to connect to the distributed system
statspec totalOpTime1 * cacheperf.CachePerfStats * operationTime1
filter=none combine=combineAcrossArchives ops=max-min! trimspec=operations
;
expr connectToDSTime = totalOpTime1 / totalOps ops=max-min?
;

// time to create the cache and region, and connect to the bridge server
statspec totalOpTime2 * cacheperf.CachePerfStats * operationTime2
filter=none combine=combineAcrossArchives ops=max-min! trimspec=operations
;
expr createCacheRegionTime = totalOpTime2 / totalOps ops=max-min?
;

// time to disconnect from the distributed system
statspec totalOpTime3 * cacheperf.CachePerfStats * operationTime3
filter=none combine=combineAcrossArchives ops=max-min! trimspec=operations
;
expr disconnectFromDSTime = totalOpTime3 / totalOps ops=max-min?
;
