include $JTESTS/cacheperf/gemfire/specs/puts.spec;

statspec memory gemfire1 VMStats * freeMemory
filter=none combine=raw ops=min? trimspec=untrimmed
;
statspec rssSize * LinuxProcessStats * rssSize
filter=none combine=combineAcrossArchives ops=max? trimspec=untrimmed
;
statspec cpu * LinuxSystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean? trimspec=untrimmed
;
