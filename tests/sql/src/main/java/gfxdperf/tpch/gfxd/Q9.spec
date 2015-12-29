//------------------------------------------------------------------------------
// Q9
//------------------------------------------------------------------------------

statspec q9_PerSecond *client* TPCHStats * q9
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=workload
;
statspec totalQ9 *client* TPCHStats * q9
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
statspec totalQ9Time *client* TPCHStats * q9Time
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
expr q9_ResponseTime = totalQ9Time / totalQ9 ops=max-min?
;
statspec totalQ9Results *client* TPCHStats * q9Results
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
expr q9_ResultSetSize = totalQ9Results / totalQ9 ops=max-min?
;

//------------------------------------------------------------------------------
// CPU during workload
//------------------------------------------------------------------------------

statspec clients *client* PerfStats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
statspec totalQ9CPUClient *client* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
expr q9_CPUClient = totalQ9CPUClient / clients ops = mean?
;

statspec servers *server* PerfStats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
statspec totalQ9CPUServer *server* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
expr q9_CPUServer = totalQ9CPUServer / servers ops = mean?
;
