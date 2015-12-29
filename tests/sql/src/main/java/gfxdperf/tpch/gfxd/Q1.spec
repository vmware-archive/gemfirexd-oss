//------------------------------------------------------------------------------
// Q1
//------------------------------------------------------------------------------

statspec q1_PerSecond *client* TPCHStats * q1
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=workload
;
statspec totalQ1 *client* TPCHStats * q1
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
statspec totalQ1Time *client* TPCHStats * q1Time
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
expr q1_ResponseTime = totalQ1Time / totalQ1 ops=max-min?
;
statspec totalQ1Results *client* TPCHStats * q1Results
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
expr q1_ResultSetSize = totalQ1Results / totalQ1 ops=max-min?
;

//------------------------------------------------------------------------------
// CPU during workload
//------------------------------------------------------------------------------

statspec clients *client* PerfStats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
statspec totalQ1CPUClient *client* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
expr q1_CPUClient = totalQ1CPUClient / clients ops = mean?
;

statspec servers *server* PerfStats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
statspec totalQ1CPUServer *server* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
expr q1_CPUServer = totalQ1CPUServer / servers ops = mean?
;
