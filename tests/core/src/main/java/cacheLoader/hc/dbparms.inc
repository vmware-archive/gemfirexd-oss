/* RDB parameters */

// (int) The number of bytes per blob value
cacheLoader.hc.DBParms-numBytes = RANGE 1000 4000 EGNAR;

// (int) The number of elements to preload in this slave
cacheLoader.hc.DBParms-numPreload = 1000;

// (String) The JDBC driver class name.
cacheLoader.hc.DBParms-jdbcDriver = oracle.jdbc.driver.OracleDriver;

// (String) The JDBC URL.
cacheLoader.hc.DBParms-jdbcUrl = jdbc:oracle:thin:@oracle.gemstone.com:1521:XE;

// (String) The RDB user.
cacheLoader.hc.DBParms-rdbUser = gemfire;

// (String) The RDB password.
cacheLoader.hc.DBParms-rdbPassword = lu5Pheko;

// (int) The poolMinLimit.
cacheLoader.hc.DBParms-poolMinLimit = 5;

// The sleep time, in milliseconds:
cacheLoader.hc.DBParms-sleepMs = 0;

// Oracle JDBC driver classes
hydra.VmPrms-extraClassPaths = /gcm/where/jdbcdriver/oracle/jdbc/lib/classes12.jar;

// Optional verbose JDBC logging
// cacheLoader.hc.DBParms-driverLogging = true;
