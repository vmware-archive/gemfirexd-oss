//------------------------------------------------------------------------------
// transaction throughput and response times
//------------------------------------------------------------------------------

statspec workloadOpsCompletedPerSecond *client* UseCase1Stats * workloadOpsCompleted
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=workload
;
statspec totalWorkloadOpsCompleted *client* UseCase1Stats * workloadOpsCompleted
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
statspec totalWorkloadOpTime *client* UseCase1Stats * workloadOpTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
expr workloadOpsCompletedResponseTime = totalWorkloadOpTime / totalWorkloadOpsCompleted ops=max-min?
;
