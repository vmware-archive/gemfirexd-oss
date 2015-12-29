//------------------------------------------------------------------------------
// Q7
//------------------------------------------------------------------------------

statspec q7_PerSecond *client* TPCHStats * q7
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=workload
;
statspec totalQ7 *client* TPCHStats * q7
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
statspec totalQ7Time *client* TPCHStats * q7Time
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
expr q7_ResponseTime = totalQ7Time / totalQ7 ops=max-min?
;
statspec totalQ7Results *client* TPCHStats * q7Results
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
expr q7_ResultSetSize = totalQ7Results / totalQ7 ops=max-min?
;

//------------------------------------------------------------------------------
// CPU during workload
//------------------------------------------------------------------------------

statspec clients *client* PerfStats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
statspec totalQ7CPUClient *client* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
expr q7_CPUClient = totalQ7CPUClient / clients ops = mean?
;

statspec servers *server* PerfStats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
statspec totalQ7CPUServer *server* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
expr q7_CPUServer = totalQ7CPUServer / servers ops = mean?
;
