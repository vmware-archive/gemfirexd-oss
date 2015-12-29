//------------------------------------------------------------------------------
// Statistics Specifications for locks (release)
//------------------------------------------------------------------------------

statspec unlocksPerSecond * dlock.DLSPerfStats * unlocks
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=unlocks
;
statspec totalUnlocks * dlock.DLSPerfStats * unlocks
filter=none combine=combineAcrossArchives ops=max-min! trimspec=unlocks
;
statspec totalUnlockTime * dlock.DLSPerfStats * unlockTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=unlocks
;
expr unlockResponseTime = totalUnlockTime / totalUnlocks ops=max-min?
;
