//------------------------------------------------------------------------------
// Q4
//------------------------------------------------------------------------------

statspec q4_PerSecond *client* TPCHStats * q4
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=workload
;
statspec totalQ4 *client* TPCHStats * q4
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
statspec totalQ4Time *client* TPCHStats * q4Time
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
expr q4_ResponseTime = totalQ4Time / totalQ4 ops=max-min?
;
statspec totalQ4Results *client* TPCHStats * q4Results
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
expr q4_ResultSetSize = totalQ4Results / totalQ4 ops=max-min?
;

//------------------------------------------------------------------------------
// CPU during workload
//------------------------------------------------------------------------------

statspec clients *client* PerfStats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
statspec totalQ4CPUClient *client* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
expr q4_CPUClient = totalQ4CPUClient / clients ops = mean?
;

statspec servers *server* PerfStats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
statspec totalQ4CPUServer *server* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
expr q4_CPUServer = totalQ4CPUServer / servers ops = mean?
;
