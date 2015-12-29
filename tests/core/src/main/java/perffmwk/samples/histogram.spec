statspec putsPerSecond * cacheperf.CachePerfStats * puts
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=puts
;
statspec totalPuts * cacheperf.CachePerfStats * puts
filter=none combine=combineAcrossArchives ops=max-min! trimspec=puts
;
statspec totalPutTime * cacheperf.CachePerfStats * putTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=puts
;
expr putResponseTime = totalPutTime / totalPuts ops=max-min?
;

statspec opsLessThan1ms * perffmwk.HistogramStats puts* opsLessThan1000000ns
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=puts
;
statspec totalOpsLessThan1ms * perffmwk.HistogramStats puts* opsLessThan1000000ns
filter=none combine=combineAcrossArchives ops=max-min! trimspec=puts
;
expr ratioLessThan1ms = totalOpsLessThan1ms / totalPuts ops=max-min?
;

statspec opsLessThan2ms * perffmwk.HistogramStats puts* opsLessThan2000000ns
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=puts
;
statspec totalOpsLessThan2ms * perffmwk.HistogramStats puts* opsLessThan2000000ns
filter=none combine=combineAcrossArchives ops=max-min! trimspec=puts
;
expr ratioLessThan2ms = totalOpsLessThan2ms / totalPuts ops=max-min?
;

statspec opsMoreThan2ms * perffmwk.HistogramStats puts* opsMoreThan2000000ns
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=puts
;
statspec totalOpsMoreThan2ms * perffmwk.HistogramStats puts* opsMoreThan2000000ns
filter=none combine=combineAcrossArchives ops=max-min! trimspec=puts
;
expr ratioMoreThan2ms = totalOpsMoreThan2ms / totalPuts ops=max-min?
;
