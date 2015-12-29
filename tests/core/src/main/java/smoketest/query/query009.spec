include $JTESTS/smoketest/perf/common.spec
;
statspec whereConditionQueriesPerSecond * cacheperf.gemfire.query.QueryPerfStats * whereConditionQueries
filter=perSecond combine=combineAcrossArchives ops=mean? trimspec=whereConditionQueries
;
