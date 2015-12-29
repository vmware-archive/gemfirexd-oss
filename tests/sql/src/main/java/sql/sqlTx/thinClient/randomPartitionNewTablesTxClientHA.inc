hydra.Prms-testDescription = "test gemfirexd tx using thin client driver with batching enabled and node failure occurs";

include $JTESTS/hydraconfig/hydraparams1.inc;
include $JTESTS/hydraconfig/gemfirexd/topology_client_locator.inc;

hydra.VmPrms-extraClassPaths        += $GEMFIRE/../product-gfxd/lib/gemfirexd.jar;
hydra.VmPrms-extraClassPaths        += $GEMFIRE/../product-gfxd/lib/gemfirexd-client.jar;

hydra.VmPrms-extraVMArgs   = fcn "hydra.TestConfigFcns.duplicate
                                 (\"-Xmx128m\", ${locatorHosts}, true)"
                             ncf
                             ,
                             fcn "hydra.TestConfigFcns.duplicate
                                  (\"-Xms512m -Xmx512m\", ${serverHosts}, true)"
                             ncf
                              ,
                             fcn "hydra.TestConfigFcns.duplicate
                                  (\"-Xmx256m \", ${clientHosts}, true)"
                             ncf;

hydra.VmPrms-extraClassPaths        += $JTESTS;

hydra.Prms-manageDerbyServer = true;
hydra.Prms-totalTaskTimeSec           = 55500;
hydra.Prms-maxResultWaitSec           = 600;
hydra.Prms-maxCloseTaskResultWaitSec  = 300;
hydra.Prms-serialExecution            = false;	
hydra.gemfirexd.FabricServerPrms-persistDD = fcn "hydra.TestConfigFcns.oneof(\"true\", \"false\")" ncf;

sql.SQLPrms-initCustomersSizePerThread = RANGE 20 50 EGNAR;
sql.SQLPrms-initSecuritiesSizePerThread = RANGE 10 30 EGNAR;                        

sql.SQLPrms-hasNetworth = true;		//will create networth table
sql.SQLPrms-dmlOperations = ONEOF insert update update update query query query delete FOENO;
sql.SQLPrms-dmlTables = "trade.securities" "trade.customers" "trade.networth" "trade.portfolio" "trade.sellorders" "trade.buyorders" "trade.txhistory" ; 	
sql.SQLPrms-createSchemas = "create schema trade" "create schema default1" "create schema emp" ;

THREADGROUP locator
  totalThreads = fcn  ${locatorHosts} * ${locatorVMsPerHost} * ${locatorThreadsPerVM} ncf
  totalVMs     = fcn "(${locatorHosts} * ${locatorVMsPerHost})" ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"locator\",
                                ${locatorHosts}, true)" ncf;
  
THREADGROUP serverThreads
            totalThreads = fcn ${serverHosts} * ${serverVMsPerHost} * ${serverThreadsPerVM} ncf
            totalVMs     = fcn ${serverHosts} * ${serverVMsPerHost} ncf
            clientNames  = fcn "hydra.TestConfigFcns.generateNames
                                (\"server\", ${serverHosts}, true)" ncf;                                
THREADGROUP clientThreads
            totalThreads = fcn ${clientHosts} * ${clientVMsPerHost} * ${clientThreadsPerVM} -1  ncf
            totalVMs     = fcn ${clientHosts} * ${clientVMsPerHost} ncf
            clientNames  = fcn "hydra.TestConfigFcns.generateNames
                                (\"client\", ${clientHosts}, true)" ncf;
                                
THREADGROUP ddlThread
            totalThreads = 1
            totalVMs     = 1
            clientNames  = fcn "hydra.TestConfigFcns.generateNames
                                (\"client\", ${clientHosts}, true)" ncf;      

INITTASK taskClass = sql.sqlTx.thinClient.SQLDistTxClientTest 
		 taskMethod = HydraTask_createGfxdLocatorTask
         threadGroups = locator;
         
INITTASK taskClass = sql.sqlTx.thinClient.SQLDistTxClientTest 
		 taskMethod = HydraTask_startGfxdLocatorTask
		 runMode = always
         threadGroups = locator;
                                
INITTASK taskClass   = sql.sqlTx.thinClient.SQLDistTxClientTest 
		 taskMethod  = HydraTask_initializeServer
         runMode = always
         threadGroups = serverThreads;
         
INITTASK taskClass   = sql.sqlTx.thinClient.SQLDistTxClientTest  
		 taskMethod  = HydraTask_startFabricServer
		 runMode = always
         threadGroups = serverThreads;           
		
INITTASK taskClass   = sql.sqlTx.thinClient.SQLDistTxClientTest 
		 taskMethod  = HydraTask_startNetworkServer
		 runMode = always
		 threadGroups =  serverThreads;    

INITTASK taskClass   = sql.sqlTx.thinClient.SQLDistTxClientTest 
		 taskMethod  = HydraTask_initClient
		 threadGroups = clientThreads, ddlThread;				

INITTASK taskClass   = sql.sqlTx.thinClient.SQLDistTxClientTest 
		 taskMethod  = HydraTask_createDiscDB
		 threadGroups = clientThreads;

INITTASK taskClass   = sql.sqlTx.thinClient.SQLDistTxClientTest 
		 taskMethod  = HydraTask_createDiscSchemas
		 threadGroups = ddlThread;

INITTASK taskClass   = sql.sqlTx.thinClient.SQLDistTxClientTest 
		 taskMethod  = HydraTask_createDiscTables
		 threadGroups = ddlThread;	  
  
	
INITTASK taskClass   = sql.sqlTx.thinClient.SQLDistTxClientTest 
		 taskMethod  = HydraTask_createGfxdSchemasByClients
		 threadGroups = ddlThread; 		
	
INITTASK taskClass   = sql.sqlTx.thinClient.SQLDistTxClientTest  
		 taskMethod  = HydraTask_createGfxdTablesByClients
		 threadGroups = ddlThread;      
  
INITTASK taskClass   = sql.sqlTx.thinClient.SQLDistTxClientTest  
		 taskMethod  = HydraTask_setTableCols
  		 threadGroups = ddlThread;  

INITTASK taskClass   = sql.sqlTx.thinClient.SQLDistTxClientTest  
		 taskMethod  = HydraTask_initConnections
 		 threadGroups = clientThreads, ddlThread; 
  
INITTASK taskClass   = sql.sqlTx.thinClient.SQLDistTxClientTest 
		 taskMethod  = HydraTask_initThreadLocals
  		 threadGroups = clientThreads, ddlThread; 
  
INITTASK taskClass   = sql.sqlTx.thinClient.SQLDistTxClientTest  
		 taskMethod  = HydraTask_populateTxTables
  		 threadGroups = clientThreads;   
  
INITTASK taskClass   = sql.sqlTx.thinClient.SQLDistTxClientTest  
		 taskMethod  = HydraTask_verifyResultSets
  		 threadGroups = ddlThread; 

/*  		 
INITTASK taskClass   = sql.sqlTx.thinClient.SQLDistTxClientTest
		 taskMethod  = HydraTask_setCriticalHeapPercentage
 		 threadGroups = ddlThread;  
*/

INITTASK taskClass   = sql.sqlTx.thinClient.SQLDistTxClientTest  
		 taskMethod  = HydraTask_createNewTables
  		 threadGroups = ddlThread; 
  		 
INITTASK taskClass   = sql.sqlTx.thinClient.SQLDistTxClientTest  
		 taskMethod  = HydraTask_alterCustomersTableGenerateAlways
  		 threadGroups = ddlThread; 

INITTASK taskClass   = sql.sqlTx.thinClient.SQLDistTxClientTest  
		 taskMethod  = HydraTask_createIndex
  		 threadGroups = clientThreads, ddlThread;  
  		 
INITTASK taskClass   = sql.sqlTx.thinClient.SQLDistTxClientTest  
		 taskMethod  = HydraTask_createIndex
  		 threadGroups = clientThreads, ddlThread;  

INITTASK taskClass   = sql.sqlTx.thinClient.SQLDistTxClientTest  
		 taskMethod  = HydraTask_createIndex
  		 threadGroups = clientThreads, ddlThread;  

TASK     taskClass   = sql.sqlTx.thinClient.SQLDistTxClientTest 
		 taskMethod  = HydraTask_doDMLOp
  		 threadGroups = clientThreads
  		 maxTimesToRun = fcn "(${clientHosts} * ${clientVMsPerHost} *  ${clientThreadsPerVM} -1) *   ${workIterationsPerThread}" ncf;    

 
CLOSETASK taskClass   = sql.sqlTx.thinClient.SQLDistTxClientTest 
		  taskMethod  = HydraTask_verifyResultSets
   		  threadGroups = ddlThread;      

sql.SQLPrms-hasNetworth = true;    //will create networth table
sql.SQLPrms-dmlOperations = ONEOF insert insert insert update update update update update update update query delete FOENO;
sql.SQLPrms-dmlTables = "trade.securities" "trade.customers" "trade.networth" "trade.portfolio" "trade.sellorders" "trade.buyorders" "trade.txhistory" ;   
sql.SQLPrms-createSchemas = "create schema trade" "create schema default1" "create schema emp" ;

sql.SQLPrms-createTablesStatements =
  "create table trade.securities (sec_id int not null, symbol varchar(10) not null, price decimal (30, 1), exchange varchar(10) not null, tid int, constraint sec_pk primary key (sec_id), constraint sec_uq unique (symbol, exchange), constraint exc_ch check (exchange in ('nasdaq', 'nye', 'amex', 'lse', 'fse', 'hkse', 'tse')))"
  "create table trade.customers (cid int not null, cust_name varchar(100), since date, addr varchar(100), tid int, primary key (cid))"
  "create table trade.networth (cid int not null, cash decimal (30, 1), securities decimal (30, 1), loanlimit int, availloan decimal (30, 1),  tid int, constraint netw_pk primary key (cid), constraint cust_newt_fk foreign key (cid) references trade.customers (cid) on delete restrict, constraint cash_ch check (cash>=0), constraint sec_ch check (securities >=0), constraint availloan_ck check (loanlimit>=availloan and availloan >=0))"
  "create table trade.portfolio (cid int not null, sid int not null, qty int not null, availQty int not null, subTotal decimal(30, 1), tid int, constraint portf_pk primary key (cid, sid), constraint cust_fk foreign key (cid) references trade.customers (cid) on delete restrict, constraint sec_fk foreign key (sid) references trade.securities (sec_id) on delete restrict, constraint qty_ck check (qty>=0), constraint avail_ch check (availQty>=0 and availQty<=qty))" 
  "create table trade.sellorders (oid int not null constraint orders_pk primary key, cid int, sid int, qty int, ask decimal (30, 1), order_time timestamp, status varchar(10), tid int, constraint portf_fk foreign key (cid, sid) references trade.portfolio (cid, sid) on delete restrict, constraint status_ch check (status in ('cancelled', 'open', 'filled')))"
  "create table trade.buyorders(oid int not null constraint buyorders_pk primary key, cid int, sid int, qty int, bid decimal (30, 1), ordertime timestamp, status varchar(10), tid int, constraint bo_cust_fk foreign key (cid) references trade.customers (cid), constraint bo_sec_fk foreign key (sid) references trade.securities (sec_id) on delete restrict, constraint bo_qty_ck check (qty>=0))"
  "create table trade.txhistory(cid int, oid int, sid int, qty int, price decimal (30, 1), ordertime timestamp, type varchar(10), tid int,  constraint type_ch check (type in ('buy', 'sell')))"
  "create table emp.employees (eid int not null constraint employees_pk primary key, emp_name varchar(100), since date, addr varchar(100), ssn varchar(9))"
  "create table trade.trades (tid int, cid int, eid int, tradedate date, primary Key (tid), foreign key (cid) references trade.customers (cid), constraint emp_fk foreign key (eid) references emp.employees (eid))";

hydra.gemfirexd.NetworkServerPrms-names = server;
hydra.gemfirexd.GfxdConfigPrms-networkServerConfig = server;
sql.SQLPrms-useGemFireXDHA = false;
sql.SQLPrms-numOfStores = fcn "(${serverHosts} * ${serverVMsPerHost})" ncf;
sql.SQLPrms-numOfWorkers = fcn ${clientHosts} * ${clientVMsPerHost} * ${clientThreadsPerVM} ncf;
sql.SQLPrms-useGfxdConfig = true;
sql.SQLPrms-testUniqueKeys = false;
sql.sqlTx.SQLTxPrms-doOpByOne = true;
sql.SQLPrms-hasTx = true;
sql.sqlTx.SQLTxPrms-useThinClientDriverInTx = true;
sql.SQLPrms-useNewTables = true;

RANDOMINCLUDE $JTESTS/sql/offHeap.inc; // uses off-heap if include is randomly chosen

sql.SQLPrms-testPartitionBy = true;
sql.SQLPrms-withReplicatedTables = true;

sql.SQLPrms-useGemFireXDHA = true;

TASK  	taskClass   = sql.sqlTx.thinClient.SQLDistTxClientTest 
		taskMethod  = HydraTask_cycleStoreVms
  		threadGroups = ddlThread
  		maxTimesToRun = 8;

sql.SQLPrms-gfeDDLExtension =
	"trade.securities:random" 
    "trade.customers:random"
	"trade.networth:random"
	"trade.portfolio:random"
	"trade.sellorders:random"
    "trade.buyorders:random" 
    "trade.txhistory:random"
	"emp.employees:replicate"
	"trade.trades:replicate" ;

sql.SQLPrms-redundancyClause =
	" REDUNDANCY ${redundantCopies}"
    " REDUNDANCY ${redundantCopies}"
	" REDUNDANCY ${redundantCopies}"
	" REDUNDANCY ${redundantCopies}"
	" REDUNDANCY ${redundantCopies}"  
	" REDUNDANCY ${redundantCopies}" 
    " REDUNDANCY ${redundantCopies}"
	" REDUNDANCY ${redundantCopies}"
	" REDUNDANCY ${redundantCopies}" ;
	
util.StopStartPrms-stopModes = ONEOF NICE_EXIT MEAN_KILL MEAN_EXIT NICE_KILL FOENO;
util.StopStartPrms-numVMsToStop = RANGE 1 ${redundantCopies} EGNAR;

INITTASK     taskClass   = util.PRObserver  taskMethod = initialize
             runMode = once
             threadGroups = serverThreads;

INITTASK     taskClass   = util.StopStartVMs  taskMethod = StopStart_initTask;
