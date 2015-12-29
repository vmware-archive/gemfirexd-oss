//------------------------------------------------------------------------------
// query response time histogram
//------------------------------------------------------------------------------

statspec opsTotal *client* perffmwk.HistogramStats queries* operations
filter=none combine=combineAcrossArchives ops=max-min! trimspec=queries
;

statspec opsTotal_00000_ms_00001_ms *client* perffmwk.HistogramStats queries* opsLessThan1000000ns
filter=none combine=combineAcrossArchives ops=max-min! trimspec=queries
;
statspec opsTotal_00001_ms_00002_ms *client* perffmwk.HistogramStats queries* opsLessThan2000000ns
filter=none combine=combineAcrossArchives ops=max-min! trimspec=queries
;
statspec opsTotal_00002_ms_00005_ms *client* perffmwk.HistogramStats queries* opsLessThan5000000ns
filter=none combine=combineAcrossArchives ops=max-min! trimspec=queries
;
statspec opsTotal_00005_ms_00010_ms *client* perffmwk.HistogramStats queries* opsLessThan10000000ns
filter=none combine=combineAcrossArchives ops=max-min! trimspec=queries
;
statspec opsTotal_00010_ms_00100_ms *client* perffmwk.HistogramStats queries* opsLessThan100000000ns
filter=none combine=combineAcrossArchives ops=max-min! trimspec=queries
;
statspec opsTotal_00100_ms_01000_ms *client* perffmwk.HistogramStats queries* opsLessThan1000000000ns
filter=none combine=combineAcrossArchives ops=max-min! trimspec=queries
;
statspec opsTotal_01000_ms_10000_ms *client* perffmwk.HistogramStats queries* opsLessThan10000000000ns
filter=none combine=combineAcrossArchives ops=max-min! trimspec=queries
;
statspec opsTotal_10000_ms_and_over *client* perffmwk.HistogramStats queries* opsMoreThan10000000000ns
filter=none combine=combineAcrossArchives ops=max-min! trimspec=queries
;

expr ops_00000_ms_00001_ms = opsTotal_00000_ms_00001_ms / opsTotal ops=max-min?
;
expr ops_00001_ms_00002_ms = opsTotal_00001_ms_00002_ms / opsTotal ops=max-min?
;
expr ops_00002_ms_00005_ms = opsTotal_00002_ms_00005_ms / opsTotal ops=max-min?
;
expr ops_00005_ms_00010_ms = opsTotal_00005_ms_00010_ms / opsTotal ops=max-min?
;
expr ops_00010_ms_00100_ms = opsTotal_00010_ms_00100_ms / opsTotal ops=max-min?
;
expr ops_00100_ms_01000_ms = opsTotal_00100_ms_01000_ms / opsTotal ops=max-min?
;
expr ops_01000_ms_10000_ms = opsTotal_01000_ms_10000_ms / opsTotal ops=max-min?
;
expr ops_10001_ms_and_over = opsTotal_10000_ms_and_over / opsTotal ops=max-min?
;
