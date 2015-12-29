include $JTESTS/cacheperf/gemfire/specs/puts.spec;

statspec updateEventsPerSecond * cacheperf.CachePerfStats * updateEvents
filter=perSecond combine=combineAcrossArchives ops=mean? trimspec=puts
;

statspec cpu * LinuxSystemStats * cpuActive
filter=none combine=raw ops=mean? trimspec=puts
;
