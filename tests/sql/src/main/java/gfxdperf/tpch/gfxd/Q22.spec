//------------------------------------------------------------------------------
// Q22
//------------------------------------------------------------------------------

statspec q22_PerSecond *client* TPCHStats * q22
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=workload
;
statspec totalQ22 *client* TPCHStats * q22
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
statspec totalQ22Time *client* TPCHStats * q22Time
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
expr q22_ResponseTime = totalQ22Time / totalQ22 ops=max-min?
;
statspec totalQ22Results *client* TPCHStats * q22Results
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
expr q22_ResultSetSize = totalQ22Results / totalQ22 ops=max-min?
;

//------------------------------------------------------------------------------
// CPU during workload
//------------------------------------------------------------------------------

statspec clients *client* PerfStats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
statspec totalQ22CPUClient *client* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
expr q22_CPUClient = totalQ22CPUClient / clients ops = mean?
;

statspec servers *server* PerfStats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
statspec totalQ22CPUServer *server* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
expr q22_CPUServer = totalQ22CPUServer / servers ops = mean?
;
