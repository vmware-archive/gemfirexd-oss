//------------------------------------------------------------------------------
// memory
//------------------------------------------------------------------------------

statspec emptyTableMemoryUsed *server* QueryPerfStats * emptyTableMemUse
filter=none combine=combineAcrossArchives ops=max trimspec=none
;
statspec loadedTableMemoryUsed *server* QueryPerfStats * loadedTableMemUse
filter=none combine=combineAcrossArchives ops=max trimspec=none
;
statspec totalMemoryUsed *server* QueryPerfStats * tableMemUse
filter=none combine=combineAcrossArchives ops=max? trimspec=none
;
