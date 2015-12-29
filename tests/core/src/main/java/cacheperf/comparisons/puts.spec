statspec putsPerSecond * cacheperf.CachePerfStats * puts
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=puts
;
statspec totalPuts * cacheperf.CachePerfStats * puts
filter=none combine=combineAcrossArchives ops=max-min! trimspec=puts
;
statspec totalPutTime * cacheperf.CachePerfStats * putTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=puts
;
expr putResponseTime = totalPutTime / totalPuts ops=max-min?
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
