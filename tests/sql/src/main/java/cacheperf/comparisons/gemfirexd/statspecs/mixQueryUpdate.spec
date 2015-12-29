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

//------------------------------------------------------------------------------
// update throughput and response time
//------------------------------------------------------------------------------

statspec updatesPerSecond *client* QueryPerfStats * updates
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=queries
;
statspec totalUpdates *client* QueryPerfStats * updates
filter=none combine=combineAcrossArchives ops=max-min! trimspec=queries
;
statspec totalUpdateTime *client* QueryPerfStats * updateTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=queries
;
expr updateResponseTime = totalUpdateTime / totalUpdates ops = max-min?
;
//------------------------------------------------------------------------------
// gc
//------------------------------------------------------------------------------

statspec gcClient *client* VMGCStats * collectionTime
filter=perSecond combine=combineAcrossArchives ops=mean! trimspec=queries
;
statspec totalGCClient *client* VMGCStats * collectionTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=queries
;
statspec clients *client* QueryPerfStats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=queries
;
expr vmGCClient = gcClient / clients ops = mean?
;
expr vmGCEfficiencyClient = totalQueries / totalGCClient ops = max-min?
;

statspec gcServer *server* VMGCStats * collectionTime
filter=none combine=combineAcrossArchives ops=mean! trimspec=queries
;
statspec totalGCServer *server* VMGCStats * collectionTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=queries
;
statspec servers *server* QueryPerfStats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=queries
;
expr vmGCServer = gcServer / servers ops = mean?
;
expr vmGCEfficiencyServer = totalQueries / totalGCServer ops = max-min?
;

//------------------------------------------------------------------------------
// cpu
//------------------------------------------------------------------------------

statspec cpuClient *client* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=queries
;
expr vmCPUClient = cpuClient / clients ops = mean?
;
expr vmCPUEfficiencyClient = queriesPerSecond / cpuClient ops = mean?
;

statspec cpuServer *server* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=queries
;
expr vmCPUServer = cpuServer / servers ops = mean?
;
expr vmCPUEfficiencyServer = queriesPerSecond / cpuServer ops = mean?
;
