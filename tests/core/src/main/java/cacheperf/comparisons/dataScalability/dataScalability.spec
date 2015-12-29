statspec getsPerSecond * cacheperf.CachePerfStats * gets
filter=perSecond combine=combineAcrossArchives ops=mean? trimspec=putgets
;
statspec putsPerSecond * cacheperf.CachePerfStats * puts
filter=perSecond combine=combineAcrossArchives ops=mean? trimspec=putgets
;
