include $JTESTS/cacheperf/gemfire/specs/putupdateEvents.spec;

statspec totalLatencySpikes * cacheperf.CachePerfStats * latencySpikes
filter=none combine=combineAcrossArchives ops=max-min trimspec=puts
;
statspec cpu * LinuxSystemStats * cpuActive
filter=none combine=combine ops=mean? trimspec=puts
;
