statspec queriesPerSecond *edge* cacheperf.CachePerfStats * queries
filter=perSecond combine=combineAcrossArchives ops=mean? trimspec=queries
;
statspec queriesTotal *edge* cacheperf.CachePerfStats * queries
filter=none combine=combineAcrossArchives ops=max-min! trimspec=queries
;
