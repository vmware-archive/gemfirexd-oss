// A Hydra include file that configures a test to profile the Client
// VM's heap using JProbe

// NOTE that you must set triggers in order to get a heap dump:
// 
// hydra.JProbePrms-triggers =
//   cacheperf.CachePerfClient.openCache():entry:start_use_case
//   cacheperf.CachePerfClient.closeCache():exit:finish_use_case
//   ;


hydra.ClientPrms-jprobeNames = clientProbe;

hydra.JProbePrms-names = clientProbe;
hydra.JProbePrms-function = heapdump;
hydra.JProbePrms-filters = *:include;
