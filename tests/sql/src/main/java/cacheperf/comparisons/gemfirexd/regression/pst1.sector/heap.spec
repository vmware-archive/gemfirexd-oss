statspec totalBytes *server* PartitionedRegionStats * dataStoreBytesInUse
filter=none combine=combineAcrossArchives ops=max trimspec=untrimmed
;
statspec heapServerPeak *server* VMMemoryUsageStats vmHeapMemoryStats usedMemory
filter=none combine=combineAcrossArchives ops=max trimspec=none
;
statspec totalHeapServer *server* VMMemoryUsageStats vmHeapMemoryStats maxMemory
filter=none combine=combineAcrossArchives ops=mean trimspec=none
;
