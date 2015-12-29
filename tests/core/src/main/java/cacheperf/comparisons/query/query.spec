//------------------------------------------------------------------------------
// Statistics Specifications for queries
//------------------------------------------------------------------------------

statspec queriesPerSecond * cacheperf.comparisons.query.QueryPerfStats * queries
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=queries
;
statspec totalQueries * cacheperf.comparisons.query.QueryPerfStats * queries
filter=none combine=combineAcrossArchives ops=max-min! trimspec=queries
;
statspec totalQueryTime * cacheperf.comparisons.query.QueryPerfStats * queryTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=queries
;
expr queryResponseTime = totalQueryTime / totalQueries ops=max-min?
;
