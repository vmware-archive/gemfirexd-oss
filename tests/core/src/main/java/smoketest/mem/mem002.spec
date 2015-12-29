statspec perEntryOverheadPutter * cacheperf.memory.CacheSizeStats putter* perEntryOverhead
filter=none combine=combineAcrossArchives ops=max? trimspec=cacheSize
;

statspec objectSizePutter * cacheperf.memory.CacheSizeStats putter* objectSize
filter=none combine=combineAcrossArchives ops=max trimspec=cacheSize
;

statspec cacheSizePutter * cacheperf.memory.CacheSizeStats putter* cacheSize
filter=none combine=combineAcrossArchives ops=max trimspec=cacheSize
;

statspec cacheMemSizePutter * cacheperf.memory.CacheSizeStats putter* cacheMemSize
filter=none combine=combineAcrossArchives ops=max? trimspec=cacheSize
;

statspec perEntryOverheadWatcher * cacheperf.memory.CacheSizeStats watcher* perEntryOverhead
filter=none combine=combineAcrossArchives ops=max? trimspec=cacheSize
;

statspec objectSizeWatcher * cacheperf.memory.CacheSizeStats watcher* objectSize
filter=none combine=combineAcrossArchives ops=max trimspec=cacheSize
;

statspec cacheSizeWatcher * cacheperf.memory.CacheSizeStats watcher* cacheSize
filter=none combine=combineAcrossArchives ops=max trimspec=cacheSize
;

statspec cacheMemSizeWatcher * cacheperf.memory.CacheSizeStats watcher* cacheMemSize
filter=none combine=combineAcrossArchives ops=max? trimspec=cacheSize
;