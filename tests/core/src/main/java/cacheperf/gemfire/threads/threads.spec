include $JTESTS/cacheperf/gemfire/specs/putgets.spec;

statspec memory * VMStats * totalMemory
filter=none combine=combineAcrossArchives ops=min,max,max-min? trimspec=putgets
;
statspec cpu * SolarisProcessStats * cpuUsed
filter=none combine=combineAcrossArchives ops=mean? trimspec=putgets
;
