// Statistics Specifications for Cache Performance Tests
//==============================================================================

//// GETS
include $JTESTS/cacheperf/gemfire/specs/gets.spec;

statspec getImageGrowth * SolarisProcessStats * imageSize
filter=none combine=combineAcrossArchives ops=min,max,max-min? trimspec=gets
;

//// SYSTEM STATS
statspec totalImageGrowth * SolarisProcessStats * imageSize
filter=none combine=combineAcrossArchives ops=min,max,max-min? trimspec=default
;
statspec cpuUsed * SolarisProcessStats * cpuUsed
filter=none combine=combineAcrossArchives ops=mean? trimspec=default
;
statspec cpuActive * LinuxSystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean? trimspec=default
;
