INCLUDE $JTESTS/management/test/jmx/memberMBean.inc;
INCLUDE $JTESTS/management/test/jmx/cacheServerMBean.inc;
INCLUDE $JTESTS/management/test/jmx/regionMBean.inc;
INCLUDE $JTESTS/management/test/jmx/lockServiceMBean.inc;
INCLUDE $JTESTS/management/test/jmx/diskStoreMBean.inc;
INCLUDE $JTESTS/management/test/jmx/gatewaySenderMBean.inc;
INCLUDE $JTESTS/management/test/jmx/gatewayReceiverMBean.inc;
INCLUDE $JTESTS/management/test/jmx/locatorMBean.inc;
INCLUDE $JTESTS/management/test/jmx/managerMBean.inc;
INCLUDE $JTESTS/management/test/jmx/distributedSystemMBean.inc;
INCLUDE $JTESTS/management/test/jmx/distributeRegionMBean.inc;
INCLUDE $JTESTS/management/test/jmx/distributedLockServiceMBean.inc;
INCLUDE $JTESTS/management/test/jmx/memberGroupMBean.inc;

management.jmx.JMXPrms-sleepTime = 2000;
hydra.VmPrms-extraClassPaths = $GEMFIRE/lib/antlr.jar $GEMFIRE/lib/gemfire.jar $GEMFIRE/lib/gfsh-dependencies.jar;