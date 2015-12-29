// This Hydra include file sets up the environment for executing Distributed Unit Tests.

INCLUDE $JTESTS/hydraconfig/hydraparams1.inc;

hydra.Prms-haltIfBadResult  = false;

hydra.HostPrms-names = 
  fcn "hydra.TestConfigFcns.generateNames(\"host\", ${dunitSites}, false)" ncf
  ;

hydra.VmPrms-names =
  fcn "hydra.TestConfigFcns.generateNames(\"vm\", ${dunitSites}, false)" ncf
  ;
//hydra.VmPrms-names =
//  fcn "hydra.TestConfigFcns.generateNames(\"locatorvm\", ${dunitSites}, false)" ncf
//  fcn "hydra.TestConfigFcns.generateNames(\"controllervm\", ${dunitSites}, false)" ncf
//  fcn "hydra.TestConfigFcns.generateNamesRepeatedly(\"clientvm\", ${dunitSites}, 2, false)" ncf
//  ;
hydra.VmPrms-hostNames = 
  fcn "hydra.TestConfigFcns.generateNames(\"host\", ${dunitSites}, false)" ncf
  ;

hydra.GemFirePrms-names = 
  fcn "hydra.TestConfigFcns.generateNames(\"locator\", ${dunitSites}, false)" ncf
  fcn "hydra.TestConfigFcns.generateNames(\"controller\", ${dunitSites}, false)" ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames(\"gemfire\", 2, ${dunitSites}, true)" ncf
  ;
hydra.GemFirePrms-hostNames =
  fcn "hydra.TestConfigFcns.generateNames(\"host\", ${dunitSites}, false)" ncf
  fcn "hydra.TestConfigFcns.generateNames(\"host\", ${dunitSites}, false)" ncf
  fcn "hydra.TestConfigFcns.generateNamesRepeatedly(\"host\", ${dunitSites}, 2, false)" ncf
  ;
hydra.GemFirePrms-distributedSystem  =
  fcn "hydra.TestConfigFcns.generateNames(\"ds_\", ${dunitSites}, false)" ncf
  fcn "hydra.TestConfigFcns.generateNames(\"ds_\", ${dunitSites}, false)" ncf
  fcn "hydra.TestConfigFcns.generateNamesRepeatedly(\"ds_\", ${dunitSites}, 2, false)" ncf
  ;

hydra.ClientPrms-names = 
  fcn "hydra.TestConfigFcns.generateNames(\"locator\", ${dunitSites}, false)" ncf
  fcn "hydra.TestConfigFcns.generateNames(\"controller\", ${dunitSites}, false)" ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames(\"client\", 2, ${dunitSites}, true)" ncf
  ;
hydra.ClientPrms-vmNames =
  fcn "hydra.TestConfigFcns.generateNames(\"vm\", ${dunitSites}, false)" ncf
  fcn "hydra.TestConfigFcns.generateNames(\"vm\", ${dunitSites}, false)" ncf
  fcn "hydra.TestConfigFcns.generateNamesRepeatedly(\"vm\", ${dunitSites}, 2, false)" ncf
  ;
//hydra.ClientPrms-vmNames =
//  fcn "hydra.TestConfigFcns.generateNames(\"locatorvm\", ${dunitSites}, false)" ncf
//  fcn "hydra.TestConfigFcns.generateNames(\"controllervm\", ${dunitSites}, false)" ncf
//  fcn "hydra.TestConfigFcns.generateNamesRepeatedly(\"clientvm\", ${dunitSites}, 2, false)" ncf
//  ;
hydra.ClientPrms-gemfireNames = 
  fcn "hydra.TestConfigFcns.generateNames(\"locator\", ${dunitSites}, false)" ncf
  fcn "hydra.TestConfigFcns.generateNames(\"controller\", ${dunitSites}, false)" ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames(\"gemfire\", 2, ${dunitSites}, true)" ncf
  ;
hydra.ClientPrms-vmQuantities = 
   fcn "hydra.TestConfigFcns.duplicate(\"1\", 1 * ${dunitSites})" ncf // locators
   fcn "hydra.TestConfigFcns.duplicate(\"1\", 1 * ${dunitSites})" ncf // controllers
   fcn "hydra.TestConfigFcns.duplicate(\"2\", 2 * ${dunitSites})" ncf // clients
   ;
hydra.ClientPrms-vmThreads = 1;

//controller section
THREADGROUP locator
            totalThreads = ${dunitSites} 
            clientNames= fcn "hydra.TestConfigFcns.generateNames
                             (\"locator\", ${dunitSites}, true)"
                         ncf
            ;
THREADGROUP controller
            totalThreads = ${dunitSites} 
            clientNames= fcn "hydra.TestConfigFcns.generateNames
                             (\"controller\", ${dunitSites}, true)"
                         ncf
            ;

INITTASK taskClass = hydra.UnitTestController taskMethod = createAndStartLocator
         threadGroups = locator
         ;
// Starts the UnitTestControllers and begins running tests 
INITTASK taskClass = hydra.UnitTestController taskMethod = scheduleUnitTests
         threadGroups = controller
         ;
//Report the failures, if any
INITTASK taskClass = hydra.UnitTestController taskMethod = checkUnitTestResults
         threadGroups = controller
         ;

CLOSETASK taskClass = hydra.UnitTestController taskMethod = stopLocator
          threadGroups = locator
          ;
// Return the test result
ENDTASK taskClass = hydra.UnitTestController taskMethod = reportUnitTestResults 
         hydratest.TaskAttributes-name = end
         ;

// dunit tests must complete within 9 hours to avoid being considered hung
// 10 hours * 60 min/hour * 60 sec/min == 36000 sec
hydra.Prms-maxResultWaitSec = 36000;

hydra.log.LogPrms-mergeLogFiles = onFailure;
hydra.log.LogPrms-mergeLogFilesArgs = "-align -pids -noblanks";

perffmwk.PerfReportPrms-generatePerformanceReport = false;
