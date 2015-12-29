//------------------------------------------------------------------------------
// Q3
//------------------------------------------------------------------------------

statspec q3_PerSecond *client* TPCHStats * q3
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=workload
;
statspec totalQ3 *client* TPCHStats * q3
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
statspec totalQ3Time *client* TPCHStats * q3Time
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
expr q3_ResponseTime = totalQ3Time / totalQ3 ops=max-min?
;
statspec totalQ3Results *client* TPCHStats * q3Results
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
expr q3_ResultSetSize = totalQ3Results / totalQ3 ops=max-min?
;

//------------------------------------------------------------------------------
// CPU during workload
//------------------------------------------------------------------------------

statspec clients *client* PerfStats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
statspec totalQ3CPUClient *client* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
expr q3_CPUClient = totalQ3CPUClient / clients ops = mean?
;

statspec servers *server* PerfStats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
statspec totalQ3CPUServer *server* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
expr q3_CPUServer = totalQ3CPUServer / servers ops = mean?
;
