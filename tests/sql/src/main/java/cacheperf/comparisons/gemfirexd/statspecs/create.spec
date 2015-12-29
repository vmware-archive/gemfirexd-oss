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
// gc
//------------------------------------------------------------------------------

statspec gcClient *client* VMGCStats * collectionTime
filter=perSecond combine=combineAcrossArchives ops=mean! trimspec=creates
;
statspec totalGCClient *client* VMGCStats * collectionTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=creates
;
statspec clients *client* QueryPerfStats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=creates
;
expr vmGCClient = gcClient / clients ops = mean?
;
expr vmGCEfficiencyClient = totalCreates / totalGCClient ops = max-min?
;

statspec gcServer *server* VMGCStats * collectionTime
filter=none combine=combineAcrossArchives ops=mean! trimspec=creates
;
statspec totalGCServer *server* VMGCStats * collectionTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=creates
;
statspec servers *server* QueryPerfStats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=creates
;
expr vmGCServer = gcServer / servers ops = mean?
;
expr vmGCEfficiencyServer = totalCreates / totalGCServer ops = max-min?
;

//------------------------------------------------------------------------------
// cpu
//------------------------------------------------------------------------------

statspec cpuClient *client* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=creates
;
expr vmCPUClient = cpuClient / clients ops = mean?
;
expr vmCPUEfficiencyClient = createsPerSecond / cpuClient ops = mean?
;

statspec cpuServer *server* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=creates
;
expr vmCPUServer = cpuServer / servers ops = mean?
;
expr vmCPUEfficiencyServer = createsPerSecond / cpuServer ops = mean?
;
