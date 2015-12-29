//
// settings common to all performance tests, including smokeperf and smokemem
//

include $JTESTS/hydraconfig/performance.inc;

hydra.gemfirexd.FabricServerPrms-enableTimeStatistics = false;
hydra.gemfirexd.FabricServerPrms-persistDD = false;

hydra.gemfirexd.LonerPrms-enableTimeStatistics = false;
