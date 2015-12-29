statspec putGetsPerSecond * cacheperf.CachePerfStats * combinedPutGets
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=combinedPutGets
;

statspec totalPutGets * cacheperf.CachePerfStats * combinedPutGets
filter=none combine=combineAcrossArchives ops=max-min! trimspec=combinedPutGets
;
statspec totalPutGetTime * cacheperf.CachePerfStats * combinedPutGetTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=combinedPutGets
;
expr putgetResponseTime = totalPutGetTime / totalPutGets ops=max-min?
;

statspec memory * VMStats * totalMemory
filter=none combine=combineAcrossArchives ops=max? trimspec=untrimmed
;
statspec rssSize * LinuxProcessStats * rssSize
filter=none combine=combineAcrossArchives ops=max? trimspec=untrimmed
;
statspec cpu * LinuxSystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean? trimspec=untrimmed
;
