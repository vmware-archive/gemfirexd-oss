//------------------------------------------------------------------------------
// Q18
//------------------------------------------------------------------------------

statspec q18_PerSecond *client* TPCHStats * q18
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=workload
;
statspec totalQ18 *client* TPCHStats * q18
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
statspec totalQ18Time *client* TPCHStats * q18Time
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
expr q18_ResponseTime = totalQ18Time / totalQ18 ops=max-min?
;
statspec totalQ18Results *client* TPCHStats * q18Results
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
expr q18_ResultSetSize = totalQ18Results / totalQ18 ops=max-min?
;

//------------------------------------------------------------------------------
// CPU during workload
//------------------------------------------------------------------------------

statspec clients *client* PerfStats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
statspec totalQ18CPUClient *client* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
expr q18_CPUClient = totalQ18CPUClient / clients ops = mean?
;

statspec servers *server* PerfStats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
statspec totalQ18CPUServer *server* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
expr q18_CPUServer = totalQ18CPUServer / servers ops = mean?
;
