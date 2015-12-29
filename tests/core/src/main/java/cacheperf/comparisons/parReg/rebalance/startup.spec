statspec totalRebalances * cacheperf.CachePerfStats * rebalances
filter=none combine=combineAcrossArchives ops=max-min! trimspec=none
;
statspec totalRebalanceTime * cacheperf.CachePerfStats * rebalanceTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=none
;
expr rebalanceResponseTime = totalRebalanceTime / totalRebalances ops=max-min?
;

statspec totalCacheOpens * cacheperf.CachePerfStats * cacheOpens
filter=none combine=combineAcrossArchives ops=max-min! trimspec=none
;
statspec totalCacheOpenTime * cacheperf.CachePerfStats * cacheOpens
filter=none combine=combineAcrossArchives ops=max-min! trimspec=none
;
expr cacheOpenResponseTime = totalCacheOpenTime / totalCacheOpens ops=max-min?
;

//statspec totalConnects * cacheperf.CachePerfStats * connects
//filter=none combine=combineAcrossArchives ops=max-min! trimspec=none
//;
//statspec totalConnectTime * cacheperf.CachePerfStats * connectTime
//filter=none combine=combineAcrossArchives ops=max-min! trimspec=none
//;
//expr connectResponseTime = totalConnectTime / totalConnects ops=max-min?
//;
//*/