include $JTESTS/smoketest/perf/common.spec
;
statspec queriesPerSecond * cacheperf.gemfire.query.QueryPerfStats * queries
filter=perSecond combine=combineAcrossArchives ops=mean? trimspec=queries
;
