//------------------------------------------------------------------------------
// update throughput and response time
//------------------------------------------------------------------------------

statspec updatesPerSecond *client* QueryPerfStats * updates
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=updates
;
statspec totalUpdates *client* QueryPerfStats * updates
filter=none combine=combineAcrossArchives ops=max-min! trimspec=updates
;
statspec totalUpdateTime *client* QueryPerfStats * updateTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=updates
;
expr updateResponseTime = totalUpdateTime / totalUpdates ops = max-min?
;

//------------------------------------------------------------------------------
// gc
//------------------------------------------------------------------------------

statspec gcClient *client* VMGCStats * collectionTime
filter=perSecond combine=combineAcrossArchives ops=mean! trimspec=updates
;
statspec totalGCClient *client* VMGCStats * collectionTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=updates
;
statspec clients *client* QueryPerfStats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=updates
;
expr vmGCClient = gcClient / clients ops = mean?
;
expr vmGCEfficiencyClient = totalUpdates / totalGCClient ops = max-min?
;

statspec gcServer *server* VMGCStats * collectionTime
filter=none combine=combineAcrossArchives ops=mean! trimspec=updates
;
statspec totalGCServer *server* VMGCStats * collectionTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=updates
;
statspec servers *server* QueryPerfStats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=updates
;
expr vmGCServer = gcServer / servers ops = mean?
;
expr vmGCEfficiencyServer = totalUpdates / totalGCServer ops = max-min?
;

//------------------------------------------------------------------------------
// cpu
//------------------------------------------------------------------------------

statspec cpuClient *client* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=updates
;
expr vmCPUClient = cpuClient / clients ops = mean?
;
expr vmCPUEfficiencyClient = updatesPerSecond / cpuClient ops = mean?
;

statspec cpuServer *server* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=updates
;
expr vmCPUServer = cpuServer / servers ops = mean?
;
expr vmCPUEfficiencyServer = updatesPerSecond / cpuServer ops = mean?
;
