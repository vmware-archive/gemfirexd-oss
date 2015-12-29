hydra.Prms-testDescription += "Uses peer clients.";

hydra.VmPrms-extraVMArgs +=
  fcn "hydra.TestConfigFcns.duplicate
       (\"-XX:+DisableExplicitGC\",
                ${locatorHosts}, true)" ncf
  ,
  fcn "hydra.TestConfigFcns.duplicate
       (\"-XX:+UseConcMarkSweepGC -XX:+DisableExplicitGC\",
                ${clientHosts}, true)" ncf
  ,
  fcn "hydra.TestConfigFcns.duplicateString
       (\"-XX:+UseConcMarkSweepGC -XX:+DisableExplicitGC\",
                ${serverHosts}, true)" ncf
  ;

hydra.gemfirexd.FabricServerPrms-names = locator server client;
hydra.gemfirexd.FabricServerPrms-clientNames =
  fcn "hydra.TestConfigFcns.generateNames(\"locator\", ${locatorHosts})" ncf
  ,
  fcn "hydra.TestConfigFcns.generateNames(\"server\", ${serverHosts})" ncf
  ,
  fcn "hydra.TestConfigFcns.generateNames(\"client\", ${clientHosts})" ncf
  ;
hydra.gemfirexd.FabricServerPrms-hostData = false true false;

gfxdperf.tpch.gfxd.GFXDPrms-connectionType = peer;
