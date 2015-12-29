//------------------------------------------------------------------------------
// Q20
//------------------------------------------------------------------------------

statspec q20_PerSecond *client* TPCHStats * q20
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=workload
;
statspec totalQ20 *client* TPCHStats * q20
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
statspec totalQ20Time *client* TPCHStats * q20Time
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
expr q20_ResponseTime = totalQ20Time / totalQ20 ops=max-min?
;
statspec totalQ20Results *client* TPCHStats * q20Results
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
expr q20_ResultSetSize = totalQ20Results / totalQ20 ops=max-min?
;

//------------------------------------------------------------------------------
// CPU during workload
//------------------------------------------------------------------------------

statspec clients *client* PerfStats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
statspec totalQ20CPUClient *client* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
expr q20_CPUClient = totalQ20CPUClient / clients ops = mean?
;

statspec servers *server* PerfStats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
statspec totalQ20CPUServer *server* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
expr q20_CPUServer = totalQ20CPUServer / servers ops = mean?
;
