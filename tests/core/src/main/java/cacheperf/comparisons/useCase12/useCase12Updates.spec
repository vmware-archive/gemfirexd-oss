statspec useCase12UpdatesPerSecond * cacheperf.CachePerfStats * useCase12Updates
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=useCase12Updates
;
statspec totalUseCase12Updates * cacheperf.CachePerfStats * useCase12Updates
filter=none combine=combineAcrossArchives ops=max-min! trimspec=useCase12Updates
;
statspec totalUseCase12UpdateTime * cacheperf.CachePerfStats * useCase12UpdatesTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=useCase12Updates
;
expr useCase12UpdatesResponseTime = totalUseCase12UpdateTime / totalUseCase12Updates ops=max-min?
;
