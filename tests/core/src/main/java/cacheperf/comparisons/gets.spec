statspec getsPerSecond * cacheperf.CachePerfStats * gets
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=gets
;
statspec totalGets * cacheperf.CachePerfStats * gets
filter=none combine=combineAcrossArchives ops=max-min! trimspec=gets
;
statspec totalGetTime * cacheperf.CachePerfStats * getTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=gets
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
