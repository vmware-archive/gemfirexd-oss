// This is a hydra include file that configures remote systems for
// running the MessagingPerf tests.  By default, it sends 200 messages
// back and forth.  Note that you must use a local.conf to specify the
// names of the machines that the test uses.

INCLUDE $JTESTS/hydraconfig/hydraparams1.inc;

THREADGROUP controller
            totalThreads=1 clientNames=controller
            ;

INITTASK taskClass = hydra.UnitTestController taskMethod = scheduleUnitTests
         threadGroups = controller
         ;

// trigger creation of a locator
hydra.GemFirePrms-names = dummy;

hydra.HostPrms-names          = host1     host2;
hydra.HostPrms-hostNames      = unknown   unknown;

hydra.VmPrms-names            = vm1      vm2;
hydra.VmPrms-hostNames        = host1    host2;

hydra.ClientPrms-names        = controller client1  client2;
hydra.ClientPrms-vmNames      = vm1        vm1      vm2;

hydra.ClientPrms-vmQuantities = 1        1;
hydra.ClientPrms-vmThreads =    1        1;

com.gemstone.gemfire.distributed.internal.MessagingPerf$Prms-warmupCount = 30000;
com.gemstone.gemfire.distributed.internal.MessagingPerf$Prms-messageCount = 10000;

perffmwk.PerfReportPrms-generatePerformanceReport = false;

