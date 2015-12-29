//------------------------------------------------------------------------------
// query response time histogram
//------------------------------------------------------------------------------

statspec queriesTotal *client* QueryPerfStats * queries
filter=none combine=combineAcrossArchives ops=max-min! trimspec=queries
;

statspec queriesTotal_00000_ms_00001_ms *client* perffmwk.HistogramStats queries* opsLessThan1000000ns
filter=none combine=combineAcrossArchives ops=max-min! trimspec=queries
;
statspec queriesTotal_00001_ms_00002_ms *client* perffmwk.HistogramStats queries* opsLessThan2000000ns
filter=none combine=combineAcrossArchives ops=max-min! trimspec=queries
;
statspec queriesTotal_00002_ms_00005_ms *client* perffmwk.HistogramStats queries* opsLessThan5000000ns
filter=none combine=combineAcrossArchives ops=max-min! trimspec=queries
;
statspec queriesTotal_00005_ms_00010_ms *client* perffmwk.HistogramStats queries* opsLessThan10000000ns
filter=none combine=combineAcrossArchives ops=max-min! trimspec=queries
;
statspec queriesTotal_00010_ms_00100_ms *client* perffmwk.HistogramStats queries* opsLessThan100000000ns
filter=none combine=combineAcrossArchives ops=max-min! trimspec=queries
;
statspec queriesTotal_00100_ms_01000_ms *client* perffmwk.HistogramStats queries* opsLessThan1000000000ns
filter=none combine=combineAcrossArchives ops=max-min! trimspec=queries
;
statspec queriesTotal_01000_ms_10000_ms *client* perffmwk.HistogramStats queries* opsLessThan10000000000ns
filter=none combine=combineAcrossArchives ops=max-min! trimspec=queries
;
statspec queriesTotal_10000_ms_and_over *client* perffmwk.HistogramStats queries* opsMoreThan10000000000ns
filter=none combine=combineAcrossArchives ops=max-min! trimspec=queries
;

expr queries_00000_ms_00001_ms = queriesTotal_00000_ms_00001_ms / queriesTotal ops=max-min?
;
expr queries_00001_ms_00002_ms = queriesTotal_00001_ms_00002_ms / queriesTotal ops=max-min?
;
expr queries_00002_ms_00005_ms = queriesTotal_00002_ms_00005_ms / queriesTotal ops=max-min?
;
expr queries_00005_ms_00010_ms = queriesTotal_00005_ms_00010_ms / queriesTotal ops=max-min?
;
expr queries_00010_ms_00100_ms = queriesTotal_00010_ms_00100_ms / queriesTotal ops=max-min?
;
expr queries_00100_ms_01000_ms = queriesTotal_00100_ms_01000_ms / queriesTotal ops=max-min?
;
expr queries_01000_ms_10000_ms = queriesTotal_01000_ms_10000_ms / queriesTotal ops=max-min?
;
expr queries_10001_ms_and_over = queriesTotal_10000_ms_and_over / queriesTotal ops=max-min?
;
