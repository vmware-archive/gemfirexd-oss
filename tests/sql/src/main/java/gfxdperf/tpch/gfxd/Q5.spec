//------------------------------------------------------------------------------
// Q5
//------------------------------------------------------------------------------

statspec q5_PerSecond *client* TPCHStats * q5
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=workload
;
statspec totalQ5 *client* TPCHStats * q5
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
statspec totalQ5Time *client* TPCHStats * q5Time
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
expr q5_ResponseTime = totalQ5Time / totalQ5 ops=max-min?
;
statspec totalQ5Results *client* TPCHStats * q5Results
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
expr q5_ResultSetSize = totalQ5Results / totalQ5 ops=max-min?
;

//------------------------------------------------------------------------------
// CPU during workload
//------------------------------------------------------------------------------

statspec clients *client* PerfStats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
statspec totalQ5CPUClient *client* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
expr q5_CPUClient = totalQ5CPUClient / clients ops = mean?
;

statspec servers *server* PerfStats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
statspec totalQ5CPUServer *server* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
expr q5_CPUServer = totalQ5CPUServer / servers ops = mean?
;
