//------------------------------------------------------------------------------
// Q8
//------------------------------------------------------------------------------

statspec q8_PerSecond *client* TPCHStats * q8
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=workload
;
statspec totalQ8 *client* TPCHStats * q8
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
statspec totalQ8Time *client* TPCHStats * q8Time
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
expr q8_ResponseTime = totalQ8Time / totalQ8 ops=max-min?
;
statspec totalQ8Results *client* TPCHStats * q8Results
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
expr q8_ResultSetSize = totalQ8Results / totalQ8 ops=max-min?
;

//------------------------------------------------------------------------------
// CPU during workload
//------------------------------------------------------------------------------

statspec clients *client* PerfStats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
statspec totalQ8CPUClient *client* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
expr q8_CPUClient = totalQ8CPUClient / clients ops = mean?
;

statspec servers *server* PerfStats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
statspec totalQ8CPUServer *server* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
expr q8_CPUServer = totalQ8CPUServer / servers ops = mean?
;
