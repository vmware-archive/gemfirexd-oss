SET CURRENT SCHEMA=SEC_OWNER;

  /*
  * Create listeners
  */ 
 create asynceventlistener SECL_BO_DATA_STATUS_HIST_LISTENER  (
 listenerclass 'com.pivotal.gemfirexd.callbacks.DBSynchronizer' 
 initparams    'com.jpmorgan.tss.securitas.strategic.logsynctable.db.oracle.DelegatingOracleUCPDriver,jdbc:oracle:thin:@(DESCRIPTION=(CONNECT_TIMEOUT=5)(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=u-frc8db-k0scan.emea.usecase1.net)(PORT=1621))(ADDRESS=(PROTOCOL=TCP)(HOST=u-frc8db-r0scan.emea.usecase1.net)(PORT=1621)))(CONNECT_DATA=(SERVICE_NAME=SECW01S_JDBC))),SEC_OWNER,SecOwner$#1'
ENABLEPERSISTENCE true
ALERTTHRESHOLD 10 )
 SERVER GROUPS(CHANNELDATAGRP);
 
 
 create asynceventlistener SECL_CHN_DATA_STATUS_HIST_LISTENER  (
 listenerclass 'com.pivotal.gemfirexd.callbacks.DBSynchronizer' 
 initparams    'com.jpmorgan.tss.securitas.strategic.logsynctable.db.oracle.DelegatingOracleUCPDriver,jdbc:oracle:thin:@(DESCRIPTION=(CONNECT_TIMEOUT=5)(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=u-frc8db-k0scan.emea.usecase1.net)(PORT=1621))(ADDRESS=(PROTOCOL=TCP)(HOST=u-frc8db-r0scan.emea.usecase1.net)(PORT=1621)))(CONNECT_DATA=(SERVICE_NAME=SECW01S_JDBC))),SEC_OWNER,SecOwner$#1'
ENABLEPERSISTENCE true
ALERTTHRESHOLD 10 )
 SERVER GROUPS(CHANNELDATAGRP);
 
 
 create asynceventlistener SECL_CHN_ORPHAN_DATA_STATUS_LISTENER  (
 listenerclass 'com.pivotal.gemfirexd.callbacks.DBSynchronizer' 
 initparams    'com.jpmorgan.tss.securitas.strategic.logsynctable.db.oracle.DelegatingOracleUCPDriver,jdbc:oracle:thin:@(DESCRIPTION=(CONNECT_TIMEOUT=5)(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=u-frc8db-k0scan.emea.usecase1.net)(PORT=1621))(ADDRESS=(PROTOCOL=TCP)(HOST=u-frc8db-r0scan.emea.usecase1.net)(PORT=1621)))(CONNECT_DATA=(SERVICE_NAME=SECW01S_JDBC))),SEC_OWNER,SecOwner$#1'
ENABLEPERSISTENCE true
ALERTTHRESHOLD 10 )
 SERVER GROUPS(CHANNELDATAGRP);
 
 
  create asynceventlistener SECL_OFAC_MESSAGE_LISTENER  (
 listenerclass 'com.pivotal.gemfirexd.callbacks.DBSynchronizer' 
 initparams    'com.jpmorgan.tss.securitas.strategic.logsynctable.db.oracle.DelegatingOracleUCPDriver,jdbc:oracle:thin:@(DESCRIPTION=(CONNECT_TIMEOUT=5)(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=u-frc8db-k0scan.emea.usecase1.net)(PORT=1621))(ADDRESS=(PROTOCOL=TCP)(HOST=u-frc8db-r0scan.emea.usecase1.net)(PORT=1621)))(CONNECT_DATA=(SERVICE_NAME=SECW01S_JDBC))),SEC_OWNER,SecOwner$#1'
ENABLEPERSISTENCE true
ALERTTHRESHOLD 10 )
 SERVER GROUPS(CHANNELDATAGRP);
 
  create asynceventlistener SECL_OFAC_CHUNKED_MESSAGE_HIST_LISTENER  (
 listenerclass 'com.pivotal.gemfirexd.callbacks.DBSynchronizer' 
 initparams    'com.jpmorgan.tss.securitas.strategic.logsynctable.db.oracle.DelegatingOracleUCPDriver,jdbc:oracle:thin:@(DESCRIPTION=(CONNECT_TIMEOUT=5)(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=u-frc8db-k0scan.emea.usecase1.net)(PORT=1621))(ADDRESS=(PROTOCOL=TCP)(HOST=u-frc8db-r0scan.emea.usecase1.net)(PORT=1621)))(CONNECT_DATA=(SERVICE_NAME=SECW01S_JDBC))),SEC_OWNER,SecOwner$#1'
ENABLEPERSISTENCE true
ALERTTHRESHOLD 10 )
 SERVER GROUPS(CHANNELDATAGRP); 
 
   create asynceventlistener SECT_MATCHED_DATA_LISTENER  (
 listenerclass 'com.pivotal.gemfirexd.callbacks.DBSynchronizer' 
 initparams    'com.jpmorgan.tss.securitas.strategic.logsynctable.db.oracle.DelegatingOracleUCPDriver,jdbc:oracle:thin:@(DESCRIPTION=(CONNECT_TIMEOUT=5)(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=u-frc8db-k0scan.emea.usecase1.net)(PORT=1621))(ADDRESS=(PROTOCOL=TCP)(HOST=u-frc8db-r0scan.emea.usecase1.net)(PORT=1621)))(CONNECT_DATA=(SERVICE_NAME=SECW01S_JDBC))),SEC_OWNER,SecOwner$#1'
ENABLEPERSISTENCE true
ALERTTHRESHOLD 10 )
 SERVER GROUPS(CHANNELDATAGRP); 
 
   create asynceventlistener SECL_BO_RAW_DATA_LISTENER  (
 listenerclass 'com.pivotal.gemfirexd.callbacks.DBSynchronizer' 
 initparams    'com.jpmorgan.tss.securitas.strategic.logsynctable.db.oracle.DelegatingOracleUCPDriver,jdbc:oracle:thin:@(DESCRIPTION=(CONNECT_TIMEOUT=5)(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=u-frc8db-k0scan.emea.usecase1.net)(PORT=1621))(ADDRESS=(PROTOCOL=TCP)(HOST=u-frc8db-r0scan.emea.usecase1.net)(PORT=1621)))(CONNECT_DATA=(SERVICE_NAME=SECW01S_JDBC))),SEC_OWNER,SecOwner$#1'
ENABLEPERSISTENCE true
ALERTTHRESHOLD 10 )
 SERVER GROUPS(CHANNELDATAGRP); 
 
   create asynceventlistener SECT_CHANNEL_RAW_DATA_LISTENER  (
 listenerclass 'com.pivotal.gemfirexd.callbacks.DBSynchronizer' 
 initparams    'com.jpmorgan.tss.securitas.strategic.logsynctable.db.oracle.DelegatingOracleUCPDriver,jdbc:oracle:thin:@(DESCRIPTION=(CONNECT_TIMEOUT=5)(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=u-frc8db-k0scan.emea.usecase1.net)(PORT=1621))(ADDRESS=(PROTOCOL=TCP)(HOST=u-frc8db-r0scan.emea.usecase1.net)(PORT=1621)))(CONNECT_DATA=(SERVICE_NAME=SECW01S_JDBC))),SEC_OWNER,SecOwner$#1'
ENABLEPERSISTENCE true
ALERTTHRESHOLD 10 )
 SERVER GROUPS(CHANNELDATAGRP); 
 
  /*
  * Associate listeners with the tables
  */ 
   ALTER TABLE SECL_BO_DATA_STATUS_HIST 
 SET  asynceventlistener(SECL_BO_DATA_STATUS_HIST_LISTENER);
 
 ALTER TABLE SECL_CHN_DATA_STATUS_HIST 
 SET  asynceventlistener(SECL_CHN_DATA_STATUS_HIST_LISTENER);
 
  ALTER TABLE SECL_CHN_ORPHAN_DATA_STATUS 
 SET  asynceventlistener(SECL_CHN_ORPHAN_DATA_STATUS_LISTENER);
 
 ALTER TABLE SECL_OFAC_MESSAGE 
 SET  asynceventlistener(SECL_OFAC_MESSAGE_LISTENER);
 
   ALTER TABLE SECL_OFAC_CHUNKED_MESSAGE_HIST 
 SET  asynceventlistener(SECL_OFAC_CHUNKED_MESSAGE_HIST_LISTENER);
 
   ALTER TABLE SECT_MATCHED_DATA 
 SET  asynceventlistener(SECT_MATCHED_DATA_LISTENER);
 
   ALTER TABLE SECL_BO_RAW_DATA 
 SET  asynceventlistener(SECL_BO_RAW_DATA_LISTENER);
 
   ALTER TABLE SECT_CHANNEL_RAW_DATA 
 SET  asynceventlistener(SECT_CHANNEL_RAW_DATA_LISTENER);
 
/*
  * Start the listeners
  */

 Call sys.start_async_event_listener('SECL_BO_DATA_STATUS_HIST_LISTENER'); 

Call sys.start_async_event_listener('SECL_CHN_DATA_STATUS_HIST_LISTENER');

Call sys.start_async_event_listener('SECL_CHN_ORPHAN_DATA_STATUS_LISTENER'); 

Call sys.start_async_event_listener('SECL_OFAC_MESSAGE_LISTENER'); 


Call sys.start_async_event_listener('SECL_OFAC_CHUNKED_MESSAGE_HIST_LISTENER');

Call sys.start_async_event_listener('SECT_MATCHED_DATA_LISTENER'); 

Call sys.start_async_event_listener('SECL_BO_RAW_DATA_LISTENER'); 

Call sys.start_async_event_listener('SECT_CHANNEL_RAW_DATA_LISTENER'); 



 
 
 
