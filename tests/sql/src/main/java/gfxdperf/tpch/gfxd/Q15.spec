//------------------------------------------------------------------------------
// Q15
//------------------------------------------------------------------------------

statspec q15_PerSecond *client* TPCHStats * q15
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=workload
;
statspec totalQ15 *client* TPCHStats * q15
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
statspec totalQ15Time *client* TPCHStats * q15Time
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
expr q15_ResponseTime = totalQ15Time / totalQ15 ops=max-min?
;
statspec totalQ15Results *client* TPCHStats * q15Results
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
expr q15_ResultSetSize = totalQ15Results / totalQ15 ops=max-min?
;

//------------------------------------------------------------------------------
// CPU during workload
//------------------------------------------------------------------------------

statspec clients *client* PerfStats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
statspec totalQ15CPUClient *client* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
expr q15_CPUClient = totalQ15CPUClient / clients ops = mean?
;

statspec servers *server* PerfStats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
statspec totalQ15CPUServer *server* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=workload
;
expr q15_CPUServer = totalQ15CPUServer / servers ops = mean?
;
