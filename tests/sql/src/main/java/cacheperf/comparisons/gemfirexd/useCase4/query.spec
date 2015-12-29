//------------------------------------------------------------------------------
// query throughput and response time
//------------------------------------------------------------------------------

statspec queriesPerSecond *client* UseCase4Stats * queries
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=queries
;
statspec totalQueries *client* UseCase4Stats * queries
filter=none combine=combineAcrossArchives ops=max-min! trimspec=queries
;
statspec totalQueryTime *client* UseCase4Stats * queryTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=queries
;
expr queryResponseTime = totalQueryTime / totalQueries ops = max-min?
;
