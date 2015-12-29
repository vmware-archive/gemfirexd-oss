hydra.Prms-testDescription = " ";

INCLUDE $JTESTS/hydraconfig/hydraparams1.inc;
INCLUDE $JTESTS/hydraconfig/gemfirexd/topology_accessor_locator.inc;

THREADGROUP locator
    totalThreads = fcn ${${C}Hosts} * ${${C}VMsPerHost} * ${${C}ThreadsPerVM} ncf
    clientNames  = fcn "hydra.TestConfigFcns.generateNames (\"${C}\", ${${C}Hosts}, true)"  ncf;

THREADGROUP datastore
    totalThreads = fcn ${${A}Hosts} * ${${A}VMsPerHost} * ${${A}ThreadsPerVM} ncf
    totalVMs     = fcn ${${A}Hosts} * ${${A}VMsPerHost} ncf
    clientNames  = fcn "hydra.TestConfigFcns.generateNames (\"${A}\", ${${A}Hosts}, true)" ncf;

THREADGROUP ddl
    totalThreads = 1
    clientNames  = fcn "hydra.TestConfigFcns.generateNames (\"${B}\", ${${B}Hosts}, true)" ncf;

THREADGROUP validator
    totalThreads = fcn ${${B}Hosts} * ${${B}VMsPerHost} ncf
    totalVMs     = fcn ${${B}Hosts} * ${${B}VMsPerHost} ncf
    clientNames  = fcn "hydra.TestConfigFcns.generateNames (\"${B}\", ${${B}Hosts}, true)" ncf;

THREADGROUP accessor
    totalThreads = fcn ${${B}Hosts} * ${${B}VMsPerHost} * ${${B}ThreadsPerVM}
                       - (${${B}Hosts} * ${${B}VMsPerHost} + 1) ncf
    totalVMs     = fcn ${${B}Hosts} * ${${B}VMsPerHost} ncf
    clientNames  = fcn "hydra.TestConfigFcns.generateNames (\"${B}\", ${${B}Hosts}, true)" ncf;

STARTTASK taskClass   = hdfs.HDFSUtil
          taskMethod  = configureHadoopTask
          clientNames = accessor1
          ;
STARTTASK taskClass   = hdfs.HDFSUtil
          taskMethod  = startHDFSCluster
          clientNames = accessor1
          ;
INITTASK taskClass    = sql.hadoopHA.HadoopHATest
         taskMethod   = HydraTask_createGfxdLocatorTask
         threadGroups = locator
         ;
INITTASK taskClass    = sql.hadoopHA.HadoopHATest
         taskMethod   = HydraTask_startGfxdLocatorTask
         threadGroups = locator
         ;
INITTASK taskClass    = sql.hadoopHA.HadoopHATest
         taskMethod   = HydraTask_initializeFabricServer
         runMode      = always
	 threadGroups = locator, datastore, accessor, ddl, validator
	 ;
INITTASK taskClass    = sql.hadoopHA.HadoopHATest
         taskMethod   = HydraTask_startFabricServer_Once
         runMode      = always
         threadGroups = datastore, validator
         ;
INITTASK taskClass    = sql.hadoopHA.HadoopHATest
         taskMethod   = HydraTask_createGFESchemas
         threadGroups = ddl
         ;
INITTASK taskClass    = sql.hadoopHA.HadoopHATest
         taskMethod   = HydraTask_createDiskStores
         threadGroups = ddl
         ;
INITTASK taskClass    = sql.SQLTest
         taskMethod   = HydraTask_createHDFSSTORE
         threadGroups = ddl
         ;
INITTASK taskClass    = sql.hadoopHA.HadoopHATest
         taskMethod   = HydraTask_createGFETables
         threadGroups = ddl
         ;
INITTASK taskClass    = sql.hadoopHA.HadoopHATest
         taskMethod   = HydraTask_populateTables
	 threadGroups = ddl
	 ;
INITTASK taskClass    = sql.hadoopHA.HadoopHATest
	 taskMethod   = HydraTask_verifyHdfsNonOperationData
	 threadGroups = validator
	 ;
INITTASK taskClass    = sql.hadoopHA.HadoopHATest
	 taskMethod   = HydraTask_verifyHdfsOperationalData
	 threadGroups = validator
	 ;
TASK taskClass    = sql.hadoopHA.HadoopHATest
         taskMethod   = HydraTask_doDMLOp
         threadGroups = accessor, ddl
         ;
CLOSETASK taskClass    = sql.hadoopHA.HadoopHATest
          taskMethod   = HydraTask_verifyHdfsNonOperationData
          threadGroups = validator
          ;
CLOSETASK taskClass    = sql.hadoopHA.HadoopHATest
          taskMethod   = HydraTask_verifyHdfsOperationalData
          threadGroups = validator
          ;
CLOSETASK taskClass    = sql.hadoopHA.HadoopHATest
          taskMethod   = HydraTask_shutDownDB
	  threadGroups = validator
	  ;
CLOSETASK taskClass    = sql.hadoopHA.HadoopHATest
          taskMethod   = HydraTask_shutDownDB
	  threadGroups = datastore
	  ;
ENDTASK taskClass   = hdfs.HDFSUtil
        taskMethod  = stopHDFSCluster
        clientNames = accessor1
        ;

hydra.VmPrms-extraClassPaths += $GEMFIRE/../product-gfxd/lib/gemfirexd.jar;
//*** From local conf
hydra.VmPrms-extraClassPaths +=
  fcn "hydra.TestConfigFcns.duplicate(\"none\", ${locatorHosts}, true)" ncf
  ,
  fcn "hydra.HadoopPrms.getServerJars(\"$HADOOP_DIST\", ${${A}Hosts})" ncf
  ,
  fcn "hydra.HadoopPrms.getServerJars(\"$HADOOP_DIST\", ${${B}Hosts})" ncf
  ;

hydra.VmPrms-extraClassPaths      += "/export/gcm/where/java/derby/derby-10.8.2.2/jars/insane/derby.jar";
hydra.VmPrms-extraClassPaths      += "/export/gcm/where/java/derby/derby-10.8.2.2/jars/insane/derbyclient.jar";
hydra.VmPrms-extraClassPaths      += "/export/gcm/where/java/jakarta/commons/lang/2.5/commons-lang-2.5.jar";
hydra.Prms-derbyServerClassPath   += "/export/gcm/where/java/derby/derby-10.8.2.2/jars/insane/derbynet.jar";
hydra.Prms-extraDerbyServerVMArgs += " -Xmx1024m -Dderby.storage.pageCacheSize=32000 -Dderby.locks.waitTimeout=30 -Dderby.locks.deadlockTimeout=20 ";
hydra.Prms-clientShutdownHook     += sql.hadoopHA.HadoopHATest dumpResults;
hydra.Prms-checkTaskMethodsExist   = false;
hydra.gemfirexd.FabricServerPrms-conserveSockets = false; //due to #44545 & #47177
//*** end

hydra.Prms-manageDerbyServer         = false;
hydra.Prms-totalTaskTimeSec          = 120;
hydra.Prms-maxResultWaitSec          = 600;
hydra.Prms-maxCloseTaskResultWaitSec = 600;
hydra.Prms-serialExecution           = false;
hydra.Prms-alwaysDoEndTasks          = true;
hydra.Prms-checkTaskMethodsExist     = false;

// Hadoop & HDFS Configuration
sql.SQLPrms-hasHDFS                           = true;
hydra.ConfigPrms-hadoopConfig                 = hdfs;
hydra.HadoopPrms-names                        = hdfs;
hydra.HadoopPrms-securityAuthentication       = kerberos;
hydra.gemfirexd.DiskStorePrms-names             = hdfsDiskStore;
hydra.gemfirexd.GfxdConfigPrms-hdfsStoreConfig  = securitiesStore customersStore;
hydra.gemfirexd.HDFSStorePrms-names             = securitiesStore customersStore;
hydra.gemfirexd.HDFSStorePrms-homeDir           = gemfirexd_data;
hydra.gemfirexd.HDFSStorePrms-hadoopName        = hdfs;
hydra.gemfirexd.HDFSStorePrms-diskStoreName     = hdfsDiskStore; //disk store name for overflow or persistence of HDFS event queue.
hydra.gemfirexd.HDFSStorePrms-clientConfigFile  = $JTESTS/sql/hdfs/hdfs-site-secure-clientConfig.xml;
// HDFS Configuration - END //

// SQL Parms
sql.SQLPrms-dmlTables                   = "trade.securities" "trade.customers" ;
sql.SQLPrms-initSecuritiesSizePerThread = 200;
sql.SQLPrms-dmlOperations               = ONEOF insert update update update query query query delete FOENO;
sql.SQLPrms-testPartitionBy             = true;
sql.SQLPrms-withReplicatedTables        = false;
sql.SQLPrms-testMultiTableJoin          = ${testMultiTableJoin};
sql.SQLPrms-testUniqueKeys              = ${testUniqueKeys};
sql.SQLPrms-hasNetworth                 = false;		//will create networth table - legacy flag
sql.SQLPrms-useGfxdConfig               = true;
sql.SQLPrms-supportDuplicateTables      = true;
sql.SQLPrms-createDiskStore             = "create diskstore hdfsDiskStore 'hdfsDiskStore'" ;

sql.SQLPrms-createSchemas = "create schema trade" ;

sql.SQLPrms-createTablesStatements =
	"create table trade.securities (sec_id int not null, symbol varchar(10) not null, price decimal (30, 20), exchange varchar(10) not null, tid int, constraint sec_pk primary key (sec_id), constraint sec_uq unique (symbol, exchange), constraint exc_ch check (exchange in ('nasdaq', 'nye', 'amex', 'lse', 'fse', 'hkse', 'tse')))"
  "create table trade.customers (cid int not null, cust_name varchar(100), since date, addr varchar(100), tid int, primary key (cid))"
  ;

sql.SQLPrms-gfeDDLExtension =
  "${securities}"
  "${customers}"
  ;

sql.SQLPrms-redundancyClause =
  " REDUNDANCY ${redundantCopies} "
  " REDUNDANCY ${redundantCopies} "
  ;

sql.SQLPrms-hdfsDDLExtn =
  " HDFSSTORE (securitiesStore)"
  " HDFSSTORE (customersStore)"
  ;
