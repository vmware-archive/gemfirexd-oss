//------------------------------------------------------------------------------
// ParNewGC stddev on servers during queries
// used to detect Bug 40546
//------------------------------------------------------------------------------
statspec vmGCParNewDev *server* VMGCStats ParNew collectionTime
filter=perSecond combine=combineAcrossArchives ops=mean? trimspec=queries
;
