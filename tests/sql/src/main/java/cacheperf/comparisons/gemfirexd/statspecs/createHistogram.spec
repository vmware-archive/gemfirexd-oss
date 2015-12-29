//------------------------------------------------------------------------------
// create response time histogram
//------------------------------------------------------------------------------

statspec createsTotal *client* QueryPerfStats * creates
filter=none combine=combineAcrossArchives ops=max-min! trimspec=creates
;

statspec createsTotal_00000_ms_00001_ms *client* perffmwk.HistogramStats creates* opsLessThan1000000ns
filter=none combine=combineAcrossArchives ops=max-min! trimspec=creates
;
statspec createsTotal_00001_ms_00002_ms *client* perffmwk.HistogramStats creates* opsLessThan2000000ns
filter=none combine=combineAcrossArchives ops=max-min! trimspec=creates
;
statspec createsTotal_00002_ms_00005_ms *client* perffmwk.HistogramStats creates* opsLessThan5000000ns
filter=none combine=combineAcrossArchives ops=max-min! trimspec=creates
;
statspec createsTotal_00005_ms_00010_ms *client* perffmwk.HistogramStats creates* opsLessThan10000000ns
filter=none combine=combineAcrossArchives ops=max-min! trimspec=creates
;
statspec createsTotal_00010_ms_00100_ms *client* perffmwk.HistogramStats creates* opsLessThan100000000ns
filter=none combine=combineAcrossArchives ops=max-min! trimspec=creates
;
statspec createsTotal_00100_ms_01000_ms *client* perffmwk.HistogramStats creates* opsLessThan1000000000ns
filter=none combine=combineAcrossArchives ops=max-min! trimspec=creates
;
statspec createsTotal_01000_ms_10000_ms *client* perffmwk.HistogramStats creates* opsLessThan10000000000ns
filter=none combine=combineAcrossArchives ops=max-min! trimspec=creates
;
statspec createsTotal_10000_ms_and_over *client* perffmwk.HistogramStats creates* opsMoreThan10000000000ns
filter=none combine=combineAcrossArchives ops=max-min! trimspec=creates
;

expr creates_00000_ms_00001_ms = createsTotal_00000_ms_00001_ms / createsTotal ops=max-min?
;
expr creates_00001_ms_00002_ms = createsTotal_00001_ms_00002_ms / createsTotal ops=max-min?
;
expr creates_00002_ms_00005_ms = createsTotal_00002_ms_00005_ms / createsTotal ops=max-min?
;
expr creates_00005_ms_00010_ms = createsTotal_00005_ms_00010_ms / createsTotal ops=max-min?
;
expr creates_00010_ms_00100_ms = createsTotal_00010_ms_00100_ms / createsTotal ops=max-min?
;
expr creates_00100_ms_01000_ms = createsTotal_00100_ms_01000_ms / createsTotal ops=max-min?
;
expr creates_01000_ms_10000_ms = createsTotal_01000_ms_10000_ms / createsTotal ops=max-min?
;
expr creates_10001_ms_and_over = createsTotal_10000_ms_and_over / createsTotal ops=max-min?
;
