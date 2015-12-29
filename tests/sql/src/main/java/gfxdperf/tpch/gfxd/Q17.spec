//------------------------------------------------------------------------------
// Q17
//------------------------------------------------------------------------------

statspec q17_PerSecond *client* TPCHStats * q17
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=workload
;
statspec totalQ17 *client* TPCHStats * q17
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
statspec totalQ17Time *client* TPCHStats * q17Time
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
expr q17_ResponseTime = totalQ17Time / totalQ17 ops=max-min?
;
statspec totalQ17Results *client* TPCHStats * q17Results
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
expr q17_ResultSetSize = totalQ17Results / totalQ17 ops=max-min?
;

//------------------------------------------------------------------------------
// CPU during workload
//------------------------------------------------------------------------------

statspec clients *client* PerfStats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
statspec totalQ17CPUClient *client* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
expr q17_CPUClient = totalQ17CPUClient / clients ops = mean?
;

statspec servers *server* PerfStats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
statspec totalQ17CPUServer *server* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
expr q17_CPUServer = totalQ17CPUServer / servers ops = mean?
;
