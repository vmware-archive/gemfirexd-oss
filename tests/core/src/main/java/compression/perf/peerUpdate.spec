include $JTESTS/cacheperf/gemfire/specs/createupdates.spec;

statspec compressionsPerSecond * CachePerfStats * compressions
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=compressions
;

statspec totalCompressions * CachePerfStats * compressions
filter=none combine=combineAcrossArchives ops=max? trimspec=compressions
;

statspec decompressionsPerSecond * CachePerfStats * decompressions
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=decompressions
;

statspec totalDecompressions * CachePerfStats * decompressions
filter=none combine=combineAcrossArchives ops=max? trimspec=decompressions
;
