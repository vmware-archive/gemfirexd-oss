//------------------------------------------------------------------------------
// create throughput and response time
//------------------------------------------------------------------------------

statspec createsPerSecond *client* QueryPerfStats * creates
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=creates
;
statspec totalCreates *client* QueryPerfStats * creates
filter=none combine=combineAcrossArchives ops=max-min! trimspec=creates
;
statspec totalCreateTime *client* QueryPerfStats * createTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=creates
;
expr createResponseTime = totalCreateTime / totalCreates ops = max-min?
;

//------------------------------------------------------------------------------
// query throughput and response time
//------------------------------------------------------------------------------

statspec queriesPerSecond *client* QueryPerfStats * queries
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=queries
;
statspec totalQueries *client* QueryPerfStats * queries
filter=none combine=combineAcrossArchives ops=max-min! trimspec=queries
;
statspec totalQueryTime *client* QueryPerfStats * queryTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=queries
;
expr queryResponseTime = totalQueryTime / totalQueries ops = max-min?
;
