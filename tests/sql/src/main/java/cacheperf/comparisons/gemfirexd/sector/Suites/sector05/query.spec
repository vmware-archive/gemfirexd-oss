//------------------------------------------------------------------------------
// query throughput
//------------------------------------------------------------------------------

statspec queriesPerSecond *client* cacheperf.comparisons.gemfirexd.QueryPerfStats * queries
filter=perSecond combine=combineAcrossArchives ops=mean? trimspec=queries
;
statspec queriesTotal *client* cacheperf.comparisons.gemfirexd.QueryPerfStats * queries
filter=none combine=combineAcrossArchives ops=max-min! trimspec=queries
;
statspec queryTimeTotal *client* cacheperf.comparisons.gemfirexd.QueryPerfStats * queryTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=queries
;
expr queryResponseTime = queryTimeTotal / queriesTotal ops = max-min?
;
