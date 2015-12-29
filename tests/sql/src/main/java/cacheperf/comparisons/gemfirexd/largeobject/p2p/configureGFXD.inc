include $JTESTS/hydraconfig/gemfirexd/topology_1_locator.inc;
include $JTESTS/hydraconfig/gemfirexd/performance.inc;

hydra.gemfirexd.FabricServerPrms-conserveSockets = ${conserveSockets};
hydra.gemfirexd.FabricServerPrms-hostData = false true;
