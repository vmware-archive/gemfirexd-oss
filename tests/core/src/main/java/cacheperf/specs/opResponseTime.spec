statspec totalOps * cacheperf.CachePerfStats * operations
filter=none combine=combineAcrossArchives ops=max-min! trimspec=operations
;
statspec totalOpTime * cacheperf.CachePerfStats * operationTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=operations
;
expr opResponseTime = totalOpTime / totalOps ops=max-min?
;
