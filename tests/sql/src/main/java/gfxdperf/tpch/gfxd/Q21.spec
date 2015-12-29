//------------------------------------------------------------------------------
// Q21
//------------------------------------------------------------------------------

statspec q21_PerSecond *client* TPCHStats * q21
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=workload
;
statspec totalQ21 *client* TPCHStats * q21
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
statspec totalQ21Time *client* TPCHStats * q21Time
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
expr q21_ResponseTime = totalQ21Time / totalQ21 ops=max-min?
;
statspec totalQ21Results *client* TPCHStats * q21Results
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
expr q21_ResultSetSize = totalQ21Results / totalQ21 ops=max-min?
;

//------------------------------------------------------------------------------
// CPU during workload
//------------------------------------------------------------------------------

statspec clients *client* PerfStats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
statspec totalQ21CPUClient *client* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
expr q21_CPUClient = totalQ21CPUClient / clients ops = mean?
;

statspec servers *server* PerfStats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
statspec totalQ21CPUServer *server* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
expr q21_CPUServer = totalQ4CPUServer / servers ops = mean?
;
