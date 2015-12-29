statspec putsPerSecond * cacheperf.CachePerfStats * puts
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=putgets
;
statspec totalPuts * cacheperf.CachePerfStats * puts
filter=none combine=combineAcrossArchives ops=max-min! trimspec=putgets
;
statspec totalPutTime * cacheperf.CachePerfStats * putTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=putgets
;
expr putResponseTime = totalPutTime / totalPuts ops=max-min?
;

statspec getsPerSecond * cacheperf.CachePerfStats * gets
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=putgets
;
statspec totalGets * cacheperf.CachePerfStats * gets
filter=none combine=combineAcrossArchives ops=max-min! trimspec=putgets
;
statspec totalGetTime * cacheperf.CachePerfStats * getTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=putgets
;
expr getResponseTime = totalGetTime / totalGets ops=max-min?
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
