statspec totalRecoveries locator* cacheperf.CachePerfStats * recoveries
filter=none combine=combineAcrossArchives ops=max-min! trimspec=none
;
statspec totalRecoveryTime locator* cacheperf.CachePerfStats * recoveryTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=none
;
expr recoveryResponseTime = totalRecoveryTime / totalRecoveries ops=max-min?
;
