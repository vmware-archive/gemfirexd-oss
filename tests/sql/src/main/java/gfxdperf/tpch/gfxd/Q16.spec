//------------------------------------------------------------------------------
// Q16
//------------------------------------------------------------------------------

statspec q16_PerSecond *client* TPCHStats * q16
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=workload
;
statspec totalQ16 *client* TPCHStats * q16
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
statspec totalQ16Time *client* TPCHStats * q16Time
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
expr q16_ResponseTime = totalQ16Time / totalQ16 ops=max-min?
;
statspec totalQ16Results *client* TPCHStats * q16Results
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
expr q16_ResultSetSize = totalQ16Results / totalQ16 ops=max-min?
;

//------------------------------------------------------------------------------
// CPU during workload
//------------------------------------------------------------------------------

statspec clients *client* PerfStats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
statspec totalQ16CPUClient *client* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
expr q16_CPUClient = totalQ16CPUClient / clients ops = mean?
;

statspec servers *server* PerfStats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
statspec totalQ16CPUServer *server* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
expr q16_CPUServer = totalQ16CPUServer / servers ops = mean?
;
