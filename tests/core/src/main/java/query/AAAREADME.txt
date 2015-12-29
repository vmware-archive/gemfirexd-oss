This test package is for integrating the querying and indexing functionality along with events.
Each test is described below:


Test   Name of conf file           Scope              Mirroring  Execution   Operations done by              Output of 
                                                                             multiple threads.               last run.   
-----  ------------------------    ----------------  ----------- ----------- ------------------------------- ---------
HYD1   serialQueryEntry.conf       distriAck/global  keysValues  Serial.     Querying and Entry operations.    PASS
    
HYD2   concQueryEntry.conf         distriAck/global  keysValues  Concurrent. Querying and Entry operations.    PASS

HYD3   serialQueryIndexEntry.conf  distriAck/global  keysValues  Serial.     Querying, Index creation/Removal  PASS
                                                                             and Entry operations.

HYD4   concQueryIndexEntry.conf    distriAck/global  keysValues  Concurrent. Querying, Index creation/Removal  PASS
                                                                             and Entry operations.
-----------------------------------------------------------------------------------------------------------------------

Each of the conf file includes queryEntry.inc file and overrides some of the parameters like
QueryPrms.entryAndQueryOperations. 
Only HYD4 was producing some errors.txt, the corresponding bugs were filed.
Bug#: 32363
      32365
      32366
      32367.
The bug fixes have been checked in.
Now the test result is found to be PASS.


Test   Name of conf file             Scope        Mirroring  Execution   Description.                      Output of 
                                                                                                           last run.   
-----  ------------------------      ----------  ----------- ----------- -------------------------------   ---------
HYD5   index/putPerfWithIndex.conf   distriAck   keysValues  Concurrent. This test creates the index in      PASS  
                                                                         a region and then measures the     
                                                                         performance of put operations.
                                                                         It thus checks the Impact of
                                                                         index creation on put operations.
                                                                                    
-----------------------------------------------------------------------------------------------------------------------
