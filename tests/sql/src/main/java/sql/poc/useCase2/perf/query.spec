//------------------------------------------------------------------------------
// Statistics Specifications for queries
//------------------------------------------------------------------------------

statspec queriesPerSecond * sql.poc.useCase2.perf.QueryPerfStats * queries
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=queries
;
statspec totalQueries * sql.poc.useCase2.perf.QueryPerfStats * queries
filter=none combine=combineAcrossArchives ops=max-min! trimspec=queries
;
statspec totalQueryTime * sql.poc.useCase2.perf.QueryPerfStats * queryTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=queries
;
expr queryResponseTime = totalQueryTime / totalQueries ops=max-min?
;

statspec selectqueriesPerSecond * sql.poc.useCase2.perf.QueryPerfStats * selectqueries
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=selectqueries
;
statspec totalSelectQueries * sql.poc.useCase2.perf.QueryPerfStats * selectqueries
filter=none combine=combineAcrossArchives ops=max-min! trimspec=selectqueries
;
statspec totalSelectQueryTime * sql.poc.useCase2.perf.QueryPerfStats * selectqueryTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=selectqueries
;
expr selectqueryResponseTime = totalSelectQueryTime / totalSelectQueries ops=max-min?
;


