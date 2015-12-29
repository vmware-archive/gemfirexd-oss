statspec queriesPerSecond * cacheperf.CachePerfStats * queries
filter=perSecond combine=combineAcrossArchives ops=mean? trimspec=queries
;
statspec totalQueries * cacheperf.CachePerfStats * queries
filter=none combine=combineAcrossArchives ops=max-min! trimspec=queries
;
statspec totalQueryTime * cacheperf.CachePerfStats * queryTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=queries
;
expr queryResponseTime = totalQueryTime / totalQueries ops=max-min?
;
statspec memoryUse * ProcessStats * rssSize
filter=none combine=combineAcrossArchives ops=max? trimspec=queries
;
statspec memoryGrowth * ProcessStats * rssSize
filter=none combine=combineAcrossArchives ops=max-min? trimspec=queries
;
statspec loadAverage * SystemStats * loadAverage1
filter=none combine=combineAcrossArchives ops=min,max,mean? trimspec=queries
;
