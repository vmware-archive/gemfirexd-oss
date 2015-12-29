// Statistics Specification for UseCase14 POC Tests
//==============================================================================

include $JTESTS/cacheperf/gemfire/specs/creates.spec;

statspec updateEventsPerSecond * cacheperf.CachePerfStats * updateEvents
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=creates
;
statspec totalUpdateEvents * cacheperf.CachePerfStats * updateEvents
filter=none combine=combineAcrossArchives ops=max-min! trimspec=creates
;
statspec totalUpdateLatency * cacheperf.CachePerfStats * updateLatency
filter=none combine=combineAcrossArchives ops=max-min! trimspec=creates
;
expr updateLatency = totalUpdateEventTime / totalUpdateEvents ops=max-min?
;
