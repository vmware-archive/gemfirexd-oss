//------------------------------------------------------------------------------
// entry counts, and sizes
//------------------------------------------------------------------------------

statspec totalEntriesUsertable *server* PartitionedRegionStats *USERTABLE* dataStoreEntryCount
filter=none combine=combineAcrossArchives ops=max? trimspec=none
;
statspec totalBytesUsertable *server* PartitionedRegionStats *USERTABLE* dataStoreBytesInUse
filter=none combine=combineAcrossArchives ops=max? trimspec=none
;
expr bytesPerUsertable = totalBytesUsertable / totalEntriesUsertable ops=max?
;
