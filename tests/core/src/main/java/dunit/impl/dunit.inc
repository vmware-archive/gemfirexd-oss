// This Hydra include file sets up the environment for executing
// Distributed Unit Tests.

INCLUDE $JTESTS/hydraconfig/hydraparams1.inc;

// Always create two systems on localhost
hydra.GemFirePrms-names       = gemfire1 gemfire2;
hydra.GemFirePrms-hostNames   = host     host;

hydra.ClientPrms-names        = controller client1    client2;
//hydra.ClientPrms-vmNames      = vm;
hydra.ClientPrms-gemfireNames = gemfire1   gemfire1   gemfire2;
hydra.ClientPrms-vmQuantities = 1          2          2;
hydra.ClientPrms-vmThreads =    1          1          1;

THREADGROUP controller
            totalThreads=1 clientNames=controller
            ;

INITTASK taskClass = hydra.UnitTestController taskMethod = scheduleUnitTests
         threadGroups = controller
         ;

// dunit tests must complete within six hours to avoid being considered hung
hydra.Prms-maxResultWaitSec = 21600;

hydra.log.LogPrms-mergeLogFiles = onFailure;
hydra.log.LogPrms-mergeLogFilesArgs = "-align -pids -noblanks";

perffmwk.PerfReportPrms-generatePerformanceReport = false;
