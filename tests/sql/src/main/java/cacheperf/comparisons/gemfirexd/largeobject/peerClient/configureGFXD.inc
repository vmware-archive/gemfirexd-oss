include $JTESTS/hydraconfig/gemfirexd/topology_2_locator.inc;
include $JTESTS/hydraconfig/gemfirexd/performance.inc;

hydra.gemfirexd.FabricServerPrms-conserveSockets = false ${conserveSockets} false;
hydra.gemfirexd.FabricServerPrms-distributedSystem = ds;
hydra.gemfirexd.FabricServerPrms-hostData = false false true;
