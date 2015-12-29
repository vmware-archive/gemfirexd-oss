//------------------------------------------------------------------------------
// update response time histogram
//------------------------------------------------------------------------------

statspec updatesTotal *client* perffmwk.HistogramStats updates* operations
filter=none combine=combineAcrossArchives ops=max-min! trimspec=updates
;

statspec updatesTotal_00000_ms_00001_ms *client* perffmwk.HistogramStats updates* opsLessThan1000000ns
filter=none combine=combineAcrossArchives ops=max-min! trimspec=updates
;
statspec updatesTotal_00001_ms_00002_ms *client* perffmwk.HistogramStats updates* opsLessThan2000000ns
filter=none combine=combineAcrossArchives ops=max-min! trimspec=updates
;
statspec updatesTotal_00002_ms_00005_ms *client* perffmwk.HistogramStats updates* opsLessThan5000000ns
filter=none combine=combineAcrossArchives ops=max-min! trimspec=updates
;
statspec updatesTotal_00005_ms_00010_ms *client* perffmwk.HistogramStats updates* opsLessThan10000000ns
filter=none combine=combineAcrossArchives ops=max-min! trimspec=updates
;
statspec updatesTotal_00010_ms_00100_ms *client* perffmwk.HistogramStats updates* opsLessThan100000000ns
filter=none combine=combineAcrossArchives ops=max-min! trimspec=updates
;
statspec updatesTotal_00100_ms_01000_ms *client* perffmwk.HistogramStats updates* opsLessThan1000000000ns
filter=none combine=combineAcrossArchives ops=max-min! trimspec=updates
;
statspec updatesTotal_01000_ms_10000_ms *client* perffmwk.HistogramStats updates* opsLessThan10000000000ns
filter=none combine=combineAcrossArchives ops=max-min! trimspec=updates
;
statspec updatesTotal_10000_ms_and_over *client* perffmwk.HistogramStats updates* opsMoreThan10000000000ns
filter=none combine=combineAcrossArchives ops=max-min! trimspec=updates
;

expr updates_00000_ms_00001_ms = updatesTotal_00000_ms_00001_ms / updatesTotal ops=max-min?
;
expr updates_00001_ms_00002_ms = updatesTotal_00001_ms_00002_ms / updatesTotal ops=max-min?
;
expr updates_00002_ms_00005_ms = updatesTotal_00002_ms_00005_ms / updatesTotal ops=max-min?
;
expr updates_00005_ms_00010_ms = updatesTotal_00005_ms_00010_ms / updatesTotal ops=max-min?
;
expr updates_00010_ms_00100_ms = updatesTotal_00010_ms_00100_ms / updatesTotal ops=max-min?
;
expr updates_00100_ms_01000_ms = updatesTotal_00100_ms_01000_ms / updatesTotal ops=max-min?
;
expr updates_01000_ms_10000_ms = updatesTotal_01000_ms_10000_ms / updatesTotal ops=max-min?
;
expr updates_10001_ms_and_over = updatesTotal_10000_ms_and_over / updatesTotal ops=max-min?
;
