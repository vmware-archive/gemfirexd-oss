//------------------------------------------------------------------------------
// Q2
//------------------------------------------------------------------------------

statspec q2_PerSecond *client* TPCHStats * q2
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=workload
;
statspec totalQ2 *client* TPCHStats * q2
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
statspec totalQ2Time *client* TPCHStats * q2Time
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
expr q2_ResponseTime = totalQ2Time / totalQ2 ops=max-min?
;
statspec totalQ2Results *client* TPCHStats * q2Results
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
expr q2_ResultSetSize = totalQ2Results / totalQ2 ops=max-min?
;

//------------------------------------------------------------------------------
// CPU during workload
//------------------------------------------------------------------------------

statspec clients *client* PerfStats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
statspec totalQ2CPUClient *client* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
expr q2_CPUClient = totalQ2CPUClient / clients ops = mean?
;

statspec servers *server* PerfStats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
statspec totalQ2CPUServer *server* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
expr q2_CPUServer = totalQ2CPUServer / servers ops = mean?
;
