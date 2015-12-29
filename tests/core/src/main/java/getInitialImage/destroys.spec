//------------------------------------------------------------------------------
// Statistics Specifications for Destroys
//------------------------------------------------------------------------------

statspec destroysPerSecond * cacheperf.CachePerfStats * destroys
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=destroys
;
statspec totalDestroys * cacheperf.CachePerfStats * destroys
filter=none combine=combineAcrossArchives ops=max-min! trimspec=destroys
;
statspec totalDestroyTime * cacheperf.CachePerfStats * destroyTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=destroys
;
expr destroyResponseTime = totalDestroyTime / totalDestroys ops=max-min?
;
