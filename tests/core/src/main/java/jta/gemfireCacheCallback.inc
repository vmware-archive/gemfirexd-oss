
INCLUDE $JTESTS/hydraconfig/hydraparams1.inc;
INCLUDE $JTESTS/hydraconfig/systemparamsN.inc;

hydra.ClientPrms-vmQuantities = 1;
hydra.ClientPrms-vmThreads    = 15;

hydra.VmPrms-extraClassPaths  = $GEMFIRE/lib/gemfire.jar /gcm/where/jdbcdriver/oracle/jdbc/lib/classes12.jar; 

STARTTASK    taskClass   = jta.GemFireTxCallback taskMethod = createDatabase
             clientNames = client1;

INITTASK     taskClass   = jta.GemFireTxCallback taskMethod = initTask
             sequential;

TASK         taskClass   = jta.GemFireTxCallback taskMethod = testTask;

CLOSETASK    taskClass   = jta.GemFireTxCallback taskMethod = closeTask;

ENDTASK      taskClass   = jta.GemFireTxCallback taskMethod = dumpDatabase
             clientNames = client1;

ENDTASK      taskClass   = jta.GemFireTxCallback taskMethod = dropOracleTable
             clientNames = client1;

hydra.GemFirePrms-stopSystemsAfterTest = true;

hydra.Prms-totalTaskTimeSec = 360;
hydra.Prms-maxResultWaitSec = 300;
hydra.Prms-haltIfBadResult = true;
hydra.Prms-serialExecution = false;
hydra.Prms-alwaysDoEndTasks = true;
hydra.Prms-maxClientShutdownWaitSec = 360;
hydra.Prms-finalClientSleepSec = 60;
hydra.Prms-maxEndTaskResultWaitSec = 300;

// Logging params
hydra.log.LogPrms-file_logging = true;

// cache-xml with NO JNDI name-bindings => use GemFire Transaction Manager
jta.JtaPrms-useGemFireTransactionManager = true;
jta.JtaPrms-cacheXmlFile = $JTESTS/jta/cache.xml;

// Oracle DataSource
// (String) The JDBC driver class name.
jta.JtaPrms-jdbcDriver = oracle.jdbc.driver.OracleDriver;
jta.JtaPrms-jdbcUrl = jdbc:oracle:thin:@oracle.gemstone.com:1521:XE;
jta.JtaPrms-rdbUser = gemfire;
jta.JtaPrms-rdbPassword = lu5Pheko;
jta.JtaPrms-poolMinLimit = 5;

// region parameters for 'employee' region
hydra.ConfigPrms-regionConfig   = region1;
hydra.RegionPrms-names          = region1;
hydra.RegionPrms-regionName     = employee;
hydra.RegionPrms-scope          = ack;
hydra.RegionPrms-dataPolicy     = normal;
hydra.RegionPrms-cacheLoader    = jta.GemFireDBLoader;
hydra.RegionPrms-cacheWriter    = jta.GemFireDBWriter;



