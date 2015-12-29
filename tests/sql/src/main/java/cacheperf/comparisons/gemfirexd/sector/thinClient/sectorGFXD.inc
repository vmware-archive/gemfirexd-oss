//------------------------------------------------------------------------------
// General
//------------------------------------------------------------------------------

hydra.VmPrms-extraVMArgs =
  fcn "hydra.TestConfigFcns.duplicate
       (\"-Xms256m -Xmx256m\", ${locatorHosts}, true)" ncf
  ,
  fcn "hydra.TestConfigFcns.duplicate
       (\"-Xms256m -Xmx256m\", ${clientHosts}, true)" ncf
  ,
  fcn "hydra.TestConfigFcns.duplicate
       (\"-Xms${heapMB}m -Xmx${heapMB}m -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+DisableExplicitGC\", ${serverHosts}, true)" ncf
  ;

//------------------------------------------------------------------------------
// GFXD
//------------------------------------------------------------------------------

// GIVE LONER SAME LOGICAL NAME AS CLIENT AS SEEN IN STATSPECS
hydra.gemfirexd.LonerPrms-names = client;
hydra.gemfirexd.LonerPrms-clientNames =
  fcn "hydra.TestConfigFcns.generateNames(\"client\", ${clientHosts})" ncf
  ;

hydra.gemfirexd.ThinClientPrms-names = client;
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
