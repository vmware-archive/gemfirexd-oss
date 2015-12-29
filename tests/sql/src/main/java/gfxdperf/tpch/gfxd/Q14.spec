//------------------------------------------------------------------------------
// Q14
//------------------------------------------------------------------------------

statspec q14_PerSecond *client* TPCHStats * q14
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=workload
;
statspec totalQ14 *client* TPCHStats * q14
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
statspec totalQ14Time *client* TPCHStats * q14Time
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
expr q14_ResponseTime = totalQ14Time / totalQ14 ops=max-min?
;
statspec totalQ14Results *client* TPCHStats * q14Results
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
expr q14_ResultSetSize = totalQ14Results / totalQ14 ops=max-min?
;

//------------------------------------------------------------------------------
// CPU during workload
//------------------------------------------------------------------------------

statspec clients *client* PerfStats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
statspec totalQ14CPUClient *client* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
expr q14_CPUClient = totalQ14CPUClient / clients ops = mean?
;

statspec servers *server* PerfStats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
statspec totalQ14CPUServer *server* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
expr q14_CPUServer = totalQ14CPUServer / servers ops = mean?
;
