SET CURRENT SCHEMA=SEC_OWNER;

CREATE TYPE MapType
EXTERNAL NAME 'java.util.Map'
LANGUAGE JAVA;

CREATE TYPE SortedMapType
EXTERNAL NAME 'java.util.SortedMap'
LANGUAGE JAVA;

CREATE TYPE ListType
EXTERNAL NAME 'java.util.List'
LANGUAGE JAVA;

CREATE PROCEDURE SEC_OWNER.matchStoredProc (IN backOfficeMsg MapType, IN matchingKeyMap SortedMapType, OUT errorStatus INTEGER)
LANGUAGE JAVA
PARAMETER STYLE JAVA
READS SQL DATA DYNAMIC RESULT SETS 1
EXTERNAL NAME 'cacheperf.comparisons.gemfirexd.useCase1.src.matcher.storedproc.MatchStoredProc.match';

CREATE PROCEDURE SEC_OWNER.queryExecutorStoredProc (IN query VARCHAR(10000), IN paramValueList ListType, OUT errorStatus INTEGER)
LANGUAGE JAVA
PARAMETER STYLE JAVA
READS SQL DATA DYNAMIC RESULT SETS 1
EXTERNAL NAME 'cacheperf.comparisons.gemfirexd.useCase1.src.matcher.storedproc.QueryExecutorStoredProc.executeSelect';
