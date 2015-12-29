//------------------------------------------------------------------------------
// Q13
//------------------------------------------------------------------------------

statspec q13_PerSecond *client* TPCHStats * q13
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=workload
;
statspec totalQ13 *client* TPCHStats * q13
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
statspec totalQ13Time *client* TPCHStats * q13Time
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
expr q13_ResponseTime = totalQ13Time / totalQ13 ops=max-min?
;
statspec totalQ13Results *client* TPCHStats * q13Results
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
expr q13_ResultSetSize = totalQ13Results / totalQ13 ops=max-min?
;

//------------------------------------------------------------------------------
// CPU during workload
//------------------------------------------------------------------------------

statspec clients *client* PerfStats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
statspec totalQ13CPUClient *client* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
expr q13_CPUClient = totalQ13CPUClient / clients ops = mean?
;

statspec servers *server* PerfStats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
statspec totalQ13CPUServer *server* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
expr q13_CPUServer = totalQ13CPUServer / servers ops = mean?
;
