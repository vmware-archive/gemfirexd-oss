include $JTESTS/hydraconfig/gemfirexd/topology_2_locator.inc;
include $JTESTS/hydraconfig/gemfirexd/performance.inc;

// GIVE LONER SAME LOGICAL NAME AS CLIENT AS SEEN IN STATSPECS
hydra.gemfirexd.LonerPrms-names = client;
hydra.gemfirexd.LonerPrms-clientNames =
  fcn "hydra.TestConfigFcns.generateNames(\"client\", ${clientHosts})" ncf
  ;

hydra.gemfirexd.ThinClientPrms-names = thinClient;
hydra.gemfirexd.ThinClientPrms-clientNames =
  fcn "hydra.TestConfigFcns.generateNames(\"client\", ${clientHosts})" ncf
  ;

hydra.gemfirexd.FabricServerPrms-names = locator server;
hydra.gemfirexd.FabricServerPrms-clientNames =
  fcn "hydra.TestConfigFcns.generateNames(\"locator\", ${locatorHosts})" ncf
  ,
  fcn "hydra.TestConfigFcns.generateNames(\"server\", ${serverHosts})" ncf
  ;
hydra.gemfirexd.FabricServerPrms-conserveSockets = false;
hydra.gemfirexd.FabricServerPrms-distributedSystem = ds;
hydra.gemfirexd.FabricServerPrms-hostData = false true;

hydra.gemfirexd.NetworkServerPrms-names = network;

hydra.gemfirexd.GfxdConfigPrms-networkServerConfig = network;

INITTASK  taskClass = cacheperf.comparisons.gemfirexd.QueryPerfClient
          taskMethod = createLocatorTask
          threadGroups = locator
          ;
INITTASK  taskClass = cacheperf.comparisons.gemfirexd.QueryPerfClient
          taskMethod = startLocatorTask
          threadGroups = locator
          ;
