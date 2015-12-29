statspec createsPerSecond * cacheperf.CachePerfStats * creates
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=creategets
;
statspec totalCreates * cacheperf.CachePerfStats * creates
filter=none combine=combineAcrossArchives ops=max-min! trimspec=creategets
;
statspec totalCreateTime * cacheperf.CachePerfStats * createTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=creategets
;
expr createResponseTime = totalCreateTime / totalCreates ops=max-min?
;

statspec getsPerSecond * cacheperf.CachePerfStats * gets
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=creategets
;
statspec totalGets * cacheperf.CachePerfStats * gets
filter=none combine=combineAcrossArchives ops=max-min! trimspec=creategets
;
statspec totalGetTime * cacheperf.CachePerfStats * getTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=creategets
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
