-- Title
JoinQueryToDAP Example


-- Overview:
The join query described below take a very long time to execute, because the query and underlying tables result in the optimizer choosing a nested loop join (NLJoin) instead of a hash join. Converting the query to Data Aware Procedure (DAP) that hashes the result yields 100+ times better performance as measured in query execution time.

You can consider replacing similar queries with data-aware procedures.


-- Query:
select a.equip_id, a.context_id, a.stop_date, dsid() as datanode_id 
from CONTEXT_HISTORY a 
left join CHART_HISTORY b 
on (a.equip_id =b.equip_id and a.context_id=b.context_id and a.stop_date=b.stop_date) 
where a.equip_id||cast(a.context_id as char(100)) 
in 
( 
select equip_id||cast(t.context_id as char(100)) 
from RECEIVED_HISTORY_LOG t where 1=1 
and exec_time > 40 
and stop_date > '2014-01-24 18:47:59' 
and stop_date < '2014-01-24 18:49:59' 
)

Query execution time = 12 minutes


-- Tables involved in query and number of rows in each table (schema is given in next section):
CONTEXT_HISTORY       : 20000
CHART_HISTORY         : 300000 
RECEIVED_HISTORY_LOG  : 40000


-- GemFire XD cluster details:
1 locator and 4 data nodes (each with 4GB memory) 
All tables are partitioned and persistent with redundancy of 1. CHART_HISTORY is colocated with CONTEXT_HISTORY.
All tables are on a single machine with the configuration: 8 cpu(s) amd64 Linux 2.6.32-358.18.1.el6.x86_64, 32GB RAM


-- Observations:
The inner and outer queries were quick to execute, but the overall performance is slow because of numerous CPU computations (CPU pegs at 100%). The reasons for this are:
  1) The outer and inner queries return 200K and 8K rows respectively, which results in 1.6 billion comparisons for the IN predicate evaluation.
  2) The expression [... a.equip_id||cast(a.context_id as char(100)) ...] creates numerous objects, which creates pressure on young generation and is highly cpu-intensive.
  
By converting the query into a Data Aware Procedure, #2 is avoided completely. Hashing in the procedure helps to optimize #1. 

In comparison, executing the query using the DAP completes in 4 seconds.


-- Schema:
CREATE DISKSTORE CONTEXTDISKSTORE;

CREATE TABLE CONTEXT_HISTORY (EQUIP_ID VARCHAR(40) NOT NULL, CONTEXT_ID INTEGER NOT NULL, STOP_DATE TIMESTAMP NOT NULL, START_DATE TIMESTAMP, CONSTRAINT CONTEST_HISTORY_PK PRIMARY KEY(EQUIP_ID,CONTEXT_ID,STOP_DATE)) PARTITION BY COLUMN (EQUIP_ID,CONTEXT_ID,STOP_DATE) REDUNDANCY 1 PERSISTENT 'CONTEXTDISKSTORE';
CREATE TABLE CHART_HISTORY ( EQUIP_ID VARCHAR(40) NOT NULL, CONTEXT_ID INTEGER NOT NULL, STOP_DATE TIMESTAMP NOT NULL, SVID_NAME VARCHAR(64) NOT NULL, CONSTRAINT CHART_HISTORY_PK PRIMARY KEY(EQUIP_ID,CONTEXT_ID,STOP_DATE,SVID_NAME) )PARTITION BY COLUMN (EQUIP_ID,CONTEXT_ID,STOP_DATE) COLOCATE WITH (CONTEXT_HISTORY) REDUNDANCY 1 PERSISTENT 'CONTEXTDISKSTORE';
CREATE TABLE RECEIVED_HISTORY_LOG ( EQUIP_ID VARCHAR(40) NOT NULL, CONTEXT_ID INTEGER NOT NULL, STOP_DATE TIMESTAMP NOT NULL, UPDATE_DATE TIMESTAMP NOT NULL, EXEC_TIME DOUBLE, CONSTRAINT RECEIVER_HISTORY_LOG_PK PRIMARY KEY(EQUIP_ID,CONTEXT_ID,STOP_DATE,UPDATE_DATE) )PARTITION BY COLUMN (EQUIP_ID,CONTEXT_ID,STOP_DATE,UPDATE_DATE) REDUNDANCY 1 PERSISTENT 'CONTEXTDISKSTORE';

create index IDX01_CONTEXT_HISTORY on CONTEXT_HISTORY (CONTEXT_ID, EQUIP_ID);
create index IDX02_DISKSTOR_HISTORY on CONTEXT_HISTORY (EQUIP_ID, STOP_DATE);


-- Steps to compile, install, and execute the data-aware procedure:
1) Compile the DAP source and create a jar (see the DAP source code for additional information):
javac -cp <gfxd-home>/lib/gemfirexd.jar -d temp <gfxd-home>/examples/joinquerytodap/*.java
jar -cfv JoinQueryToDap.jar -C temp/ .

2) Install, replace, or remove the DAP JAR:
To install:
gfxd install-jar -name=JOIN_QUERY_TO_DAP_JAR -file=<path>/JoinQueryToDap.jar -client-bind-address=<hostname> -client-port=1528

To replace:
gfxd replace-jar -name=JOIN_QUERY_TO_DAP_JAR -file=<path>/JoinQueryToDap.jar -client-bind-address=<hostname> -client-port=1528

To remove:
gfxd remove-jar -name=JOIN_QUERY_TO_DAP_JAR -client-bind-address=<hostname> -client-port=1528

3) Create the DAP in GemFire XD: 
CREATE PROCEDURE QUERY_TUNE_DAP()
LANGUAGE JAVA PARAMETER STYLE JAVA 
READS SQL DATA DYNAMIC RESULT SETS 1  
EXTERNAL NAME 'examples.joinquerytodap.JoinQueryTuneProcedure.execute' ;

4) Execute the DAP:
Execute from java code examples.joinquerytodap.JoinQueryTuneDAPClient, which uses {CALL QUERY_TUNE_DAP() WITH RESULT PROCESSOR examples.joinquerytodap.JoinQueryTuneProcessor ON ALL}

java -cp <gfxd-home>/lib/gemfirexd-client.jar examples.joinquerytodap.JoinQueryTuneDAPClient


-- Detailed analysis:
1) The query plan for the inner query does not take much time to execute:

explain select eqp_id, cntxt_id
from FDCMGR.FDC_NRT_PUMPER_HIST_LOG t where 1=1 
and exec_time > 40 
and stop_dt > '2014-01-24 18:47:59' 
and stop_dt < '2014-01-24 18:49:59' 

Output:	
ORIGINATOR pnq-rdiyewar2(17972)<v2>:35301 BEGIN TIME 2014-01-28 05:16:03.397 END TIME 2014-01-28 05:16:03.499
DISTRIBUTION to 4 members took 4266 microseconds  ( message sending min/max/avg time 215/2255/2950 microseconds and receiving min/max/avg time 18/1094/1574 microseconds ) 
SEQUENTIAL-ITERATION of 8000 rows took 67099 microseconds
Local plan:
member pnq-rdiyewar2(17972)<v2>:35301 begin_execution 2014-01-28 05:16:03.4 end_execution 2014-01-28 05:16:03.468
PROJECTION (6.75%) execute_time 2.622859 ms returned_rows 3946 no_opens 1 node_details SELECT :EQP_ID CNTXT_ID 
  PROJECT-FILTER (15.90%) execute_time 6.175447 ms returned_rows 3946 no_opens 1 node_details SELECT :EQP_ID CNTXT_ID STOP_DT EXEC_TIME 
    TABLESCAN (77.33%) execute_time 30.022636 ms returned_rows 3946 no_opens 1 scan_qualifiers Column[0][0] Id: STOP_DT  Operator: < 2014-01-24 18:49:59 Ordered nulls: false Unknown return value: false Column[0][1] Id: STOP_DT  Operator: > 2014-01-24 18:47:59 Ordered nulls: false Unknown return value: true Column[0][2] Id: EXEC_TIME  Operator: > 40.0 Ordered nulls: false Unknown return value: true  scanned_object FDCMGR.FDC_NRT_PUMPER_HIST_LOG scan_type HEAP

Slowest Member Plan:
member pnq-rdiyewar2(17798)<v1>:6618 begin_execution 2014-01-28 05:16:03.4 end_execution 2014-01-28 05:16:03.425
QUERY-RECEIVE  execute_time 25.753745 ms member_node pnq-rdiyewar2(17972)<v2>:35301
  RESULT-SEND  execute_time 0.237251 ms member_node pnq-rdiyewar2(17972)<v2>:35301
    RESULT-HOLDER  execute_time 21.637013 ms returned_rows 1510 no_opens 1
      PROJECTION (6.16%) execute_time 1.203555 ms returned_rows 1510 no_opens 1 node_details SELECT :EQP_ID CNTXT_ID 
        PROJECT-FILTER (17.78%) execute_time 3.469075 ms returned_rows 1510 no_opens 1 node_details SELECT :EQP_ID CNTXT_ID STOP_DT EXEC_TIME 
          TABLESCAN (76.04%) execute_time 14.836624 ms returned_rows 1510 no_opens 1 scan_qualifiers Column[0][0] Id: STOP_DT  Operator: < 2014-01-24 18:49:59 Ordered nulls: false Unknown return value: false Column[0][1] Id: STOP_DT  Operator: > 2014-01-24 18:47:59 Ordered nulls: false Unknown return value: true Column[0][2] Id: EXEC_TIME  Operator: > 40.0 Ordered nulls: false Unknown return value: true  scanned_object FDCMGR.FDC_NRT_PUMPER_HIST_LOG scan_type HEAP

Fastest Member Plan:
member pnq-rdiyewar2(18182)<v3>:7996 begin_execution 2014-01-28 05:16:03.4 end_execution 2014-01-28 05:16:03.417
QUERY-RECEIVE  execute_time 16.792439 ms member_node pnq-rdiyewar2(17972)<v2>:35301
  RESULT-SEND  execute_time 0.330365 ms member_node pnq-rdiyewar2(17972)<v2>:35301
    RESULT-HOLDER  execute_time 13.916073 ms returned_rows 1057 no_opens 1
      PROJECTION (5.88%) execute_time 0.75547 ms returned_rows 1057 no_opens 1 node_details SELECT :EQP_ID CNTXT_ID 
        PROJECT-FILTER (14.27%) execute_time 1.832783 ms returned_rows 1057 no_opens 1 node_details SELECT :EQP_ID CNTXT_ID STOP_DT EXEC_TIME 
          TABLESCAN (79.84%) execute_time 10.254514 ms returned_rows 1057 no_opens 1 scan_qualifiers Column[0][0] Id: STOP_DT  Operator: < 2014-01-24 18:49:59 Ordered nulls: false Unknown return value: false Column[0][1] Id: STOP_DT  Operator: > 2014-01-24 18:47:59 Ordered nulls: false Unknown return value: true Column[0][2] Id: EXEC_TIME  Operator: > 40.0 Ordered nulls: false Unknown return value: true  scanned_object FDCMGR.FDC_NRT_PUMPER_HIST_LOG scan_type HEAP


2) The query plan for the outer query does not take much time to execute:

explain select a.eqp_id, a.cntxt_id, a.stop_dt, dsid() as datanode_id 
from FDCMGR.FDC_NRT_CNTXT_HIST a 
left join FDCMGR.FDC_NRT_TCHART_HIST b 
on (a.eqp_id =b.eqp_id and a.cntxt_id=b.cntxt_id and a.stop_dt=b.stop_dt) 
join rep_temp_3 t on (a.eqp_id =t.eqp_id and a.cntxt_id=t.cntxt_id)

Output:

ORIGINATOR pnq-rdiyewar2(17972)<v2>:35301 BEGIN TIME 2014-01-28 05:21:21.183 END TIME 2014-01-28 05:21:22.237
DISTRIBUTION to 4 members took 257393 microseconds  ( message sending min/max/avg time 197/255557/256323 microseconds and receiving min/max/avg time 16/3739/16969 microseconds ) 
SEQUENTIAL-ITERATION of 82648 rows took 473691 microseconds
Local plan:
member pnq-rdiyewar2(17972)<v2>:35301 begin_execution 2014-01-28 05:21:21.214 end_execution 2014-01-28 05:21:21.734
PROJECTION (6.39%) execute_time 24.20785 ms returned_rows 21396 no_opens 1 node_details SELECT :EQP_ID CNTXT_ID STOP_DT DATANODE_ID 
  NLJOIN (4.47%) execute_time 16.934077 ms returned_rows 21396 no_opens 1
    NLJOIN (3.87%) execute_time 14.645315 ms returned_rows 61935 no_opens 1
      TABLESCAN (1.78%) execute_time 6.748956 ms returned_rows 5095 no_opens 1 scan_qualifiers None scanned_object FDCMGR.FDC_NRT_CNTXT_HIST scan_type HEAP
      HASHSCAN (69.98%) execute_time 264.796076 ms returned_rows 121785 no_opens 5095 scan_qualifiers None scanned_object FDC_NRT_TCHART_HIST scan_type HEAP
    HASHSCAN (13.48%) execute_time 51.032397 ms returned_rows 42792 no_opens 61935 scan_qualifiers None scanned_object REP_TEMP_3 scan_type HEAP

Slowest Member Plan:
member pnq-rdiyewar2(18182)<v3>:7996 begin_execution 2014-01-28 05:21:21.185 end_execution 2014-01-28 05:21:21.643
QUERY-RECEIVE  execute_time 457.715431 ms member_node pnq-rdiyewar2(17972)<v2>:35301
  RESULT-SEND  execute_time 1.607986 ms member_node pnq-rdiyewar2(17972)<v2>:35301
    RESULT-HOLDER  execute_time 201.926536 ms returned_rows 20758 no_opens 1
      RESULT-SEND  execute_time 2.279193 ms member_node pnq-rdiyewar2(17972)<v2>:35301
        RESULT-HOLDER  execute_time 201.926536 ms returned_rows 20758 no_opens 1
          RESULT-SEND  execute_time 1.307043 ms member_node pnq-rdiyewar2(17972)<v2>:35301
            RESULT-HOLDER  execute_time 201.926536 ms returned_rows 20758 no_opens 1
              RESULT-SEND  execute_time 0.528661 ms member_node pnq-rdiyewar2(17972)<v2>:35301
                RESULT-HOLDER  execute_time 201.926536 ms returned_rows 20758 no_opens 1
                  PROJECTION (6.94%) execute_time 25.971176 ms returned_rows 20758 no_opens 1 node_details SELECT :EQP_ID CNTXT_ID STOP_DT DATANODE_ID 
                    NLJOIN (4.95%) execute_time 18.539514 ms returned_rows 20758 no_opens 1
                      NLJOIN (3.54%) execute_time 13.254042 ms returned_rows 60424 no_opens 1
                        TABLESCAN (1.92%) execute_time 7.178874 ms returned_rows 4970 no_opens 1 scan_qualifiers None scanned_object FDCMGR.FDC_NRT_CNTXT_HIST scan_type HEAP
                        HASHSCAN (68.55%) execute_time 256.246352 ms returned_rows 118815 no_opens 4970 scan_qualifiers None scanned_object FDC_NRT_TCHART_HIST scan_type HEAP
                      HASHSCAN (14.07%) execute_time 52.5924 ms returned_rows 41516 no_opens 60424 scan_qualifiers None scanned_object REP_TEMP_3 scan_type HEAP

Fastest Member Plan:
member pnq-rdiyewar2(18433)<v4>:37434 begin_execution 2014-01-28 05:21:21.185 end_execution 2014-01-28 05:21:21.63
QUERY-RECEIVE  execute_time 444.596088 ms member_node pnq-rdiyewar2(17972)<v2>:35301
  RESULT-SEND  execute_time 1.718587 ms member_node pnq-rdiyewar2(17972)<v2>:35301
    RESULT-HOLDER  execute_time 186.775506 ms returned_rows 19334 no_opens 1
      RESULT-SEND  execute_time 1.856727 ms member_node pnq-rdiyewar2(17972)<v2>:35301
        RESULT-HOLDER  execute_time 186.775506 ms returned_rows 19334 no_opens 1
          RESULT-SEND  execute_time 1.156596 ms member_node pnq-rdiyewar2(17972)<v2>:35301
            RESULT-HOLDER  execute_time 186.775506 ms returned_rows 19334 no_opens 1
              RESULT-SEND  execute_time 0.115584 ms member_node pnq-rdiyewar2(17972)<v2>:35301
                RESULT-HOLDER  execute_time 186.775506 ms returned_rows 19334 no_opens 1
                  PROJECTION (6.39%) execute_time 23.721161 ms returned_rows 19334 no_opens 1 node_details SELECT :EQP_ID CNTXT_ID STOP_DT DATANODE_ID 
                    NLJOIN (4.45%) execute_time 16.527641 ms returned_rows 19334 no_opens 1
                      NLJOIN (3.78%) execute_time 14.033567 ms returned_rows 60529 no_opens 1
                        TABLESCAN (1.56%) execute_time 5.817944 ms returned_rows 4963 no_opens 1 scan_qualifiers None scanned_object FDCMGR.FDC_NRT_CNTXT_HIST scan_type HEAP
                        HASHSCAN (70.77%) execute_time 262.476442 ms returned_rows 119055 no_opens 4963 scan_qualifiers None scanned_object FDC_NRT_TCHART_HIST scan_type HEAP
                      HASHSCAN (13.02%) execute_time 48.299543 ms returned_rows 38668 no_opens 60529 scan_qualifiers None scanned_object REP_TEMP_3 scan_type HEAP

3) Output of 'select * from sys.querystats' for the original query:

select a.eqp_id, a.cntxt_id, a.stop_dt, dsid() as datanode_id 
from FDCMGR.FDC_NRT_CNTXT_HIST a 
left join FDCMGR.FDC_NRT_TCHART_HIST b 
on (a.eqp_id =b.eqp_id and a.cntxt_id=b.cntxt_id and a.stop_dt=b.stop_dt) 
where a.eqp_id||cast(a.cntxt_id as char(100)) 
in 
( 
select eqp_id||cast(t.cntxt_id as char(100)) 
from FDCMGR.FDC_NRT_PUMPER_HIST_LOG t where 1=1 
and exec_time > 40 
and stop_dt > '2014-01-24 18:47:59' 
and stop_dt < '2014-01-24 18:49:59' 
);

Results: The query took approximately 16 minutes to execute.

select * from sys.querystats;

QUERY,MEMBERID,PARAMSSIZE,PLAN,NUMINVOCATIONS,TOTALTIME,DISTRIBUTIONTIME,SERIALIZATIONTIME,EXECUTIONTIME,ORMTIME
/nselect eqp_id||cast(t.cntxt_id as char(100)) /nfrom FDCMGR.FDC_NRT_PUMPER_HIST_LOG t where <?>=<?> /nand exec_time > <?> /nand stop_dt > <?> /nand stop_dt < <?>,pnq-rdiyewar2(17972)<v2>:35301,0,,4,2317372,0,20456249,111541287,473324729232
select * from sys.querystats,pnq-rdiyewar2(17972)<v2>:35301,0,,1,1097935,0,0,0,0
"select a.eqp_id, a.cntxt_id, a.stop_dt, dsid() as datanode_id /nfrom FDCMGR.FDC_NRT_CNTXT_HIST a /nleft join FDCMGR.FDC_NRT_TCHART_HIST b /non (a.eqp_id =b.eqp_id and a.cntxt_id=b.cntxt_id and a.stop_dt=b.stop_dt) /nwhere a.eqp_id||cast(a.cntxt_id as char(100)) /nin /n( /nselect eqp_id||cast(t.cntxt_id as char(100)) /nfrom FDCMGR.FDC_NRT_PUMPER_HIST_LOG t where 1=1 /nand exec_time > 40 /nand stop_dt > '2014-01-24 18:47:59' /nand stop_dt < '2014-01-24 18:49:59' /n)",pnq-rdiyewar2(17972)<v2>:35301,0,,1,0,0,0,0,0
"select a.eqp_id, a.cntxt_id, a.stop_dt, dsid() as datanode_id /nfrom FDCMGR.FDC_NRT_CNTXT_HIST a /nleft join FDCMGR.FDC_NRT_TCHART_HIST b /non (a.eqp_id =b.eqp_id and a.cntxt_id=b.cntxt_id and a.stop_dt=b.stop_dt) /nwhere a.eqp_id||cast(a.cntxt_id as char(100)) /nin /n( /nselect eqp_id||cast(t.cntxt_id as char(100)) /nfrom FDCMGR.FDC_NRT_PUMPER_HIST_LOG t where <?>=<?> /nand exec_time > <?> /nand stop_dt > <?> /nand stop_dt < <?> /n)",pnq-rdiyewar2(17972)<v2>:35301,0,,1,219799180,0,147280704,0,0
/nselect eqp_id||cast(t.cntxt_id as char(100)) /nfrom FDCMGR.FDC_NRT_PUMPER_HIST_LOG t where <?>=<?> /nand exec_time > <?> /nand stop_dt > <?> /nand stop_dt < <?>,pnq-rdiyewar2(18182)<v3>:7996,0,,4,2391192,0,17494470,124607555,478822699897
select * from sys.querystats,pnq-rdiyewar2(18182)<v3>:7996,0,,1,156132,0,0,0,0
"select a.eqp_id, a.cntxt_id, a.stop_dt, dsid() as datanode_id /nfrom FDCMGR.FDC_NRT_CNTXT_HIST a /nleft join FDCMGR.FDC_NRT_TCHART_HIST b /non (a.eqp_id =b.eqp_id and a.cntxt_id=b.cntxt_id and a.stop_dt=b.stop_dt) /nwhere a.eqp_id||cast(a.cntxt_id as char(100)) /nin /n( /nselect eqp_id||cast(t.cntxt_id as char(100)) /nfrom FDCMGR.FDC_NRT_PUMPER_HIST_LOG t where <?>=<?> /nand exec_time > <?> /nand stop_dt > <?> /nand stop_dt < <?> /n)",pnq-rdiyewar2(18182)<v3>:7996,0,,1,221823731,0,117202667,287831804921,0
/nselect eqp_id||cast(t.cntxt_id as char(100)) /nfrom FDCMGR.FDC_NRT_PUMPER_HIST_LOG t where <?>=<?> /nand exec_time > <?> /nand stop_dt > <?> /nand stop_dt < <?>,pnq-rdiyewar2(17798)<v1>:6618,0,,4,2253039,0,22854600,108986758,456040154848
select * from sys.querystats,pnq-rdiyewar2(17798)<v1>:6618,0,,1,202864,0,0,0,0
"select a.eqp_id, a.cntxt_id, a.stop_dt, dsid() as datanode_id /nfrom FDCMGR.FDC_NRT_CNTXT_HIST a /nleft join FDCMGR.FDC_NRT_TCHART_HIST b /non (a.eqp_id =b.eqp_id and a.cntxt_id=b.cntxt_id and a.stop_dt=b.stop_dt) /nwhere a.eqp_id||cast(a.cntxt_id as char(100)) /nin /n( /nselect eqp_id||cast(t.cntxt_id as char(100)) /nfrom FDCMGR.FDC_NRT_PUMPER_HIST_LOG t where <?>=<?> /nand exec_time > <?> /nand stop_dt > <?> /nand stop_dt < <?> /n)",pnq-rdiyewar2(17798)<v1>:6618,0,,1,215553231,0,109363283,251380142024,0
/nselect eqp_id||cast(t.cntxt_id as char(100)) /nfrom FDCMGR.FDC_NRT_PUMPER_HIST_LOG t where <?>=<?> /nand exec_time > <?> /nand stop_dt > <?> /nand stop_dt < <?>,pnq-rdiyewar2(18433)<v4>:37434,0,,4,3349185,0,28945399,110935189,438599654278
select * from sys.querystats,pnq-rdiyewar2(18433)<v4>:37434,0,,1,169878,0,0,0,0
"select a.eqp_id, a.cntxt_id, a.stop_dt, dsid() as datanode_id /nfrom FDCMGR.FDC_NRT_CNTXT_HIST a /nleft join FDCMGR.FDC_NRT_TCHART_HIST b /non (a.eqp_id =b.eqp_id and a.cntxt_id=b.cntxt_id and a.stop_dt=b.stop_dt) /nwhere a.eqp_id||cast(a.cntxt_id as char(100)) /nin /n( /nselect eqp_id||cast(t.cntxt_id as char(100)) /nfrom FDCMGR.FDC_NRT_PUMPER_HIST_LOG t where <?>=<?> /nand exec_time > <?> /nand stop_dt > <?> /nand stop_dt < <?> /n)",pnq-rdiyewar2(18433)<v4>:37434,0,,1,210855243,0,99858581,280166784551,0