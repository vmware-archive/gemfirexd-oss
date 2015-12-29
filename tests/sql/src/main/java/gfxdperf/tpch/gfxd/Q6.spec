//------------------------------------------------------------------------------
// Q6
//------------------------------------------------------------------------------

statspec q6_PerSecond *client* TPCHStats * q6
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=workload
;
statspec totalQ6 *client* TPCHStats * q6
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
statspec totalQ6Time *client* TPCHStats * q6Time
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
expr q6_ResponseTime = totalQ6Time / totalQ6 ops=max-min?
;
statspec totalQ6Results *client* TPCHStats * q6Results
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
expr q6_ResultSetSize = totalQ6Results / totalQ6 ops=max-min?
;

//------------------------------------------------------------------------------
// CPU during workload
//------------------------------------------------------------------------------

statspec clients *client* PerfStats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
statspec totalQ6CPUClient *client* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
expr q6_CPUClient = totalQ6CPUClient / clients ops = mean?
;

statspec servers *server* PerfStats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
statspec totalQ6CPUServer *server* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
expr q6_CPUServer = totalQ6CPUServer / servers ops = mean?
;
