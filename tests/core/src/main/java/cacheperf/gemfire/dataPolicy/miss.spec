include $JTESTS/cacheperf/gemfire/specs/gets.spec;

statspec memory gemfire2 VMStats * totalMemory
filter=none combine=raw ops=min,max,max-min? trimspec=untrimmed
;
statspec rssSize * LinuxProcessStats * rssSize
filter=none combine=combineAcrossArchives ops=max? trimspec=untrimmed
;
