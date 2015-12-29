//------------------------------------------------------------------------------
// creates
//
statspec createsPerSecond * cacheperf.comparisons.gemfirexd.QueryPerfStats * creates
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=creates
;
statspec totalCreates * cacheperf.comparisons.gemfirexd.QueryPerfStats * creates
filter=none combine=combineAcrossArchives ops=max-min! trimspec=creates
;
statspec totalCreateTime * cacheperf.comparisons.gemfirexd.QueryPerfStats * createTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=creates
;
expr createResponseTime = totalCreateTime / totalCreates ops=max-min?
;

//------------------------------------------------------------------------------
// queries
//
statspec queriesPerSecond * cacheperf.comparisons.gemfirexd.QueryPerfStats * queries
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=queries
;
statspec totalQueries * cacheperf.comparisons.gemfirexd.QueryPerfStats * queries
filter=none combine=combineAcrossArchives ops=max-min! trimspec=queries
;
statspec totalQueryTime * cacheperf.comparisons.gemfirexd.QueryPerfStats * queryTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=queries
;
expr queryResponseTime = totalQueryTime / totalQueries ops=max-min?
;

