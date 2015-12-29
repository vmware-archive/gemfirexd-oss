hydra.Prms-testDescription += "Uses thin clients.";

hydra.VmPrms-extraVMArgs +=
  fcn "hydra.TestConfigFcns.duplicate
       (\"-XX:+DisableExplicitGC\",
                ${locatorHosts}, true)" ncf
  ,
  fcn "hydra.TestConfigFcns.duplicate
       (\"-XX:+DisableExplicitGC\",
                ${clientHosts}, true)" ncf
  ,
  fcn "hydra.TestConfigFcns.duplicateString
       (\"-XX:+UseConcMarkSweepGC -XX:+DisableExplicitGC\",
                ${serverHosts}, true)" ncf
  ;

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
hydra.gemfirexd.FabricServerPrms-hostData = false true;

hydra.gemfirexd.NetworkServerPrms-names = network;

hydra.gemfirexd.GfxdConfigPrms-networkServerConfig = network;

gfxdperf.tpch.gfxd.GFXDPrms-connectionType = thin;
