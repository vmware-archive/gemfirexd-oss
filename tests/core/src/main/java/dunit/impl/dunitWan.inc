INCLUDE $JTESTS/dunit/impl/dunitN.inc;

 hydra.GemFirePrms-names =
   fcn "hydra.TestConfigFcns.generateNames(\"locator\", ${dunitSites}, false)" ncf
   fcn "hydra.TestConfigFcns.generateNames(\"controller\", ${dunitSites}, false)" ncf
   fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames(\"gemfire\", 4, ${dunitSites}, true)" ncf
   ;
 hydra.GemFirePrms-hostNames =
   fcn "hydra.TestConfigFcns.generateNames(\"host\", ${dunitSites}, false)" ncf
   fcn "hydra.TestConfigFcns.generateNames(\"host\", ${dunitSites}, false)" ncf
   fcn "hydra.TestConfigFcns.generateNamesRepeatedly(\"host\", ${dunitSites}, 4, false)" ncf
   ;
 hydra.GemFirePrms-distributedSystem  =
   fcn "hydra.TestConfigFcns.generateNames(\"ds_\", ${dunitSites}, false)" ncf
   fcn "hydra.TestConfigFcns.generateNames(\"ds_\", ${dunitSites}, false)" ncf
   fcn "hydra.TestConfigFcns.generateNamesRepeatedly(\"ds_\", ${dunitSites}, 4, false)" ncf
   ;

 hydra.ClientPrms-names =
   fcn "hydra.TestConfigFcns.generateNames(\"locator\", ${dunitSites}, false)" ncf
   fcn "hydra.TestConfigFcns.generateNames(\"controller\", ${dunitSites}, false)" ncf
   fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames(\"client\", 4, ${dunitSites}, true)" ncf
   ;
 hydra.ClientPrms-vmNames =
   fcn "hydra.TestConfigFcns.generateNames(\"vm\", ${dunitSites}, false)" ncf
   fcn "hydra.TestConfigFcns.generateNames(\"vm\", ${dunitSites}, false)" ncf
  fcn "hydra.TestConfigFcns.generateNamesRepeatedly(\"vm\", ${dunitSites}, 4, false)" ncf
   ;
 hydra.ClientPrms-gemfireNames =
   fcn "hydra.TestConfigFcns.generateNames(\"locator\", ${dunitSites}, false)" ncf
   fcn "hydra.TestConfigFcns.generateNames(\"controller\", ${dunitSites}, false)" ncf
   fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames(\"gemfire\", 4, ${dunitSites}, true)" ncf
   ;
 hydra.ClientPrms-vmQuantities =
    fcn "hydra.TestConfigFcns.duplicate(\"1\", 1 * ${dunitSites})" ncf // locators
    fcn "hydra.TestConfigFcns.duplicate(\"1\", 1 * ${dunitSites})" ncf // controllers
    fcn "hydra.TestConfigFcns.duplicate(\"4\", 4 * ${dunitSites})" ncf // clients
    ;



