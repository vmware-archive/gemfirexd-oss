// accessors
statspec totalAccessorCacheOpens accessor* cacheperf.CachePerfStats * cacheOpens
filter=none combine=combineAcrossArchives ops=max-min! trimspec=untrimmed
;
statspec totalAccessorCacheOpenTime accessor* cacheperf.CachePerfStats * cacheOpenTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=untrimmed
;
expr accessorCacheOpenResponseTime = totalAccessorCacheOpenTime / totalAccessorCacheOpens ops=max-min?
;
