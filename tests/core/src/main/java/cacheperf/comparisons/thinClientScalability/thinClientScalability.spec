// Statistics Specification for Thin Client Scalability Test
//==============================================================================

//// CREATES
statspec createsPerSecond * cacheperf.CachePerfStats * creates
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=creates
;

//// PUTS
statspec putsPerSecond * cacheperf.CachePerfStats * puts
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=putgets
;

//// GETS
statspec getsPerSecond * cacheperf.CachePerfStats * gets
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=putgets
;
