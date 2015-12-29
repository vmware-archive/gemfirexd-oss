hydra.Prms-testDescription += "Uses peer clients.";

hydra.gemfirexd.FabricServerPrms-names = locator server client;
hydra.gemfirexd.FabricServerPrms-clientNames =
  fcn "hydra.TestConfigFcns.generateNames(\"locator\", ${locatorHosts})" ncf
  ,
  fcn "hydra.TestConfigFcns.generateNames(\"server\", ${serverHosts})" ncf
  ,
  fcn "hydra.TestConfigFcns.generateNames(\"client\", ${clientHosts})" ncf
  ;
hydra.gemfirexd.FabricServerPrms-hostData = false true false;

gfxdperf.ycsb.gfxd.GFXDPrms-connectionType = peer;
