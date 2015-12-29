include $JTESTS/cacheperf/gemfire/specs/createKeys.spec;
include $JTESTS/cacheperf/gemfire/specs/creates.spec;
include $JTESTS/cacheperf/gemfire/specs/puts.spec;
include $JTESTS/cacheperf/gemfire/specs/gets.spec;

//// SYSTEM STATS
statspec totalImageGrowth * SolarisProcessStats * imageSize
filter=none combine=combineAcrossArchives ops=min,max,max-min? trimspec=default
;
statspec cpuUsed * SolarisProcessStats * cpuUsed
filter=none combine=combineAcrossArchives ops=mean? trimspec=default
;
