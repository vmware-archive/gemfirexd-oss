//------------------------------------------------------------------------------
// Statistics Specifications for queries
//------------------------------------------------------------------------------

statspec queriesPerSecond * cacheperf.CachePerfStats * queries
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=queries
;
statspec totalQueries * cacheperf.CachePerfStats * queries
filter=none combine=combineAcrossArchives ops=max-min! trimspec=queries
;
statspec totalQueryTime * cacheperf.CachePerfStats * queryTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=queries
;
expr queryResponseTime = totalQueryTime / totalQueries ops=max-min?
;
