//------------------------------------------------------------------------------
// Q11
//------------------------------------------------------------------------------

statspec q11_PerSecond *client* TPCHStats * q11
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=workload
;
statspec totalQ11 *client* TPCHStats * q11
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
statspec totalQ11Time *client* TPCHStats * q11Time
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
expr q11_ResponseTime = totalQ11Time / totalQ11 ops=max-min?
;
statspec totalQ11Results *client* TPCHStats * q11Results
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
expr q11_ResultSetSize = totalQ11Results / totalQ11 ops=max-min?
;

//------------------------------------------------------------------------------
// CPU during workload
//------------------------------------------------------------------------------

statspec clients *client* PerfStats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
statspec totalQ11CPUClient *client* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
expr q11_CPUClient = totalQ11CPUClient / clients ops = mean?
;

statspec servers *server* PerfStats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
statspec totalQ11CPUServer *server* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
expr q11_CPUServer = totalQ11CPUServer / servers ops = mean?
;
