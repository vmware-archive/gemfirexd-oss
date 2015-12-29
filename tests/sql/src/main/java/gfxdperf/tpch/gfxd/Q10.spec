//------------------------------------------------------------------------------
// Q10
//------------------------------------------------------------------------------

statspec q10_PerSecond *client* TPCHStats * q10
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=workload
;
statspec totalQ10 *client* TPCHStats * q10
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
statspec totalQ10Time *client* TPCHStats * q10Time
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
expr q10_ResponseTime = totalQ10Time / totalQ10 ops=max-min?
;
statspec totalQ10Results *client* TPCHStats * q10Results
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
expr q10_ResultSetSize = totalQ10Results / totalQ10 ops=max-min?
;

//------------------------------------------------------------------------------
// CPU during workload
//------------------------------------------------------------------------------

statspec clients *client* PerfStats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
statspec totalQ10CPUClient *client* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
expr q10_CPUClient = totalQ10CPUClient / clients ops = mean?
;

statspec servers *server* PerfStats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
statspec totalQ10CPUServer *server* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
expr q10_CPUServer = totalQ10CPUServer / servers ops = mean?
;
