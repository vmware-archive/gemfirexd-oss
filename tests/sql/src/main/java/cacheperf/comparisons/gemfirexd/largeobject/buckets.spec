statspec totalEntriesLargeObject *server* PartitionedRegionStats *LARGE_OBJECT* dataStoreEntryCount
filter=none combine=raw ops=max? trimspec=none
;
statspec bucketCountLargeObject *server* PartitionedRegionStats *LARGE_OBJECT* bucketCount
filter=none combine=raw ops=max? trimspec=none
;
expr entriesPerBucketLargeObject = totalEntriesLargeObject / bucketCountLargeObject ops=max?
;
