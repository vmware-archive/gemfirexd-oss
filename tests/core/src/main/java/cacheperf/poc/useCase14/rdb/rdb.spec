// Statistics Specification for UseCase14 POC Tests
//==============================================================================

statspec createsPerSecond * cacheperf.CachePerfStats * creates
filter=perSecond combine=combineAcrossArchives ops=mean? trimspec=creates
;
statspec totalCreates * cacheperf.CachePerfStats * creates
filter=none combine=combineAcrossArchives ops=max-min trimspec=creates
;
statspec putsPerSecond * cacheperf.CachePerfStats * puts
filter=perSecond combine=combineAcrossArchives ops=mean? trimspec=puts
;
statspec totalPuts * cacheperf.CachePerfStats * puts
filter=none combine=combineAcrossArchives ops=max-min trimspec=puts
;
