//------------------------------------------------------------------------------
// Statistics Specifications for locks (acquire)
//------------------------------------------------------------------------------

statspec locksPerSecond * dlock.DLSPerfStats * locks
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=locks
;
statspec totalLocks * dlock.DLSPerfStats * locks
filter=none combine=combineAcrossArchives ops=max-min! trimspec=locks
;
statspec totalLockTime * dlock.DLSPerfStats * lockTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=locks
;
expr lockResponseTime = totalLockTime / totalLocks ops=max-min?
;
