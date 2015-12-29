//
// turns on clock skew management for better latency numbers
//
// needed by tests that do operations in one vm and record latencies in another
// due to listeners or subscriptions or cqs
//

hydra.timeserver.TimeServerPrms-clockSkewUpdateFrequencyMs = 1000;
hydra.timeserver.TimeServerPrms-clockSkewMaxLatencyMs = 25;
