//------------------------------------------------------------------------------
// Q19
//------------------------------------------------------------------------------

statspec q19_PerSecond *client* TPCHStats * q19
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=workload
;
statspec totalQ19 *client* TPCHStats * q19
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
statspec totalQ19Time *client* TPCHStats * q19Time
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
expr q19_ResponseTime = totalQ19Time / totalQ19 ops=max-min?
;
statspec totalQ19Results *client* TPCHStats * q19Results
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
expr q19_ResultSetSize = totalQ19Results / totalQ19 ops=max-min?
;

//------------------------------------------------------------------------------
// CPU during workload
//------------------------------------------------------------------------------

statspec clients *client* PerfStats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
statspec totalQ19CPUClient *client* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
expr q19_CPUClient = totalQ19CPUClient / clients ops = mean?
;

statspec servers *server* PerfStats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
statspec totalQ19CPUServer *server* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
expr q19_CPUServer = totalQ19CPUServer / servers ops = mean?
;
