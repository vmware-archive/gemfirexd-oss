//------------------------------------------------------------------------------
// Q12
//------------------------------------------------------------------------------

statspec q12_PerSecond *client* TPCHStats * q12
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=workload
;
statspec totalQ12 *client* TPCHStats * q12
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
statspec totalQ12Time *client* TPCHStats * q12Time
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
expr q12_ResponseTime = totalQ12Time / totalQ12 ops=max-min?
;
statspec totalQ12Results *client* TPCHStats * q12Results
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
expr q12_ResultSetSize = totalQ12Results / totalQ12 ops=max-min?
;

//------------------------------------------------------------------------------
// CPU during workload
//------------------------------------------------------------------------------

statspec clients *client* PerfStats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
statspec totalQ12CPUClient *client* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
expr q12_CPUClient = totalQ12CPUClient / clients ops = mean?
;

statspec servers *server* PerfStats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
statspec totalQ12CPUServer *server* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
expr q12_CPUServer = totalQ12CPUServer / servers ops = mean?
;
