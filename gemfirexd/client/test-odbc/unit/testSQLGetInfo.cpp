/*
 * Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
 
#include "GemFireXDHelper.h"
using namespace std;

#define TESTNAME "SQLGetInfo"
#define TABLE ""

#define MAX_RGB_VALUE 300
#define MAX_NAME_LEN  50

//#define GETINFO_ALL
#define GETINFO_OPT1
//#define GETINFO_OPT2
#define GETINFO_OPT3
/*#define GETINFO_OPT4
 #define GETINFO_OPT5
 #define GETINFO_OPT6
 #define GETINFO_OPT7*/
#define GETINFO_OPT8

/* Following info type are supported currently
 *
 Info Type                               Values
 ==============================================================
 SQL_DRIVER_NAME                         "Pivotal GemFire XD ODBC Driver"
 SQL_DRIVER_VER                          "01.00.02.0001"
 SQL_DRIVER_ODBC_VER                     "03.51"
 SQL_ODBC_VER                            "03.51.0000"
 SQL_SCROLL_OPTIONS                      (SQL_SO_DYNAMIC | SQL_SO_STATIC)
 SQL_DYNAMIC_CURSOR_ATTRIBUTES1          (SQL_CA1_LOCK_NO_CHANGE | SQL_CA1_POSITIONED_UPDATE
 | SQL_CA1_POSITIONED_DELETE | SQL_CA1_SELECT_FOR_UPDATE)
 SQL_DYNAMIC_CURSOR_ATTRIBUTES2          (SQL_CA2_SIMULATE_UNIQUE | SQL_CA2_SENSITIVITY_ADDITIONS
 |SQL_CA2_SENSITIVITY_DELETIONS |SQL_CA2_SENSITIVITY_UPDATES)
 SQL_STATIC_CURSOR_ATTRIBUTES1           (SQL_CA1_LOCK_NO_CHANGE | SQL_CA1_POSITIONED_UPDATE
 | SQL_CA1_POSITIONED_DELETE | SQL_CA1_SELECT_FOR_UPDATE)
 SQL_STATIC_CURSOR_ATTRIBUTES2           SQL_CA2_SIMULATE_UNIQUE
 SQL_BATCH_SUPPORT                       SQL_BS_ROW_COUNT_EXPLICIT, 0
 SQL_BATCH_ROW_COUNT                     SQL_BRC_EXPLICIT
 SQL_PARAM_ARRAY_ROW_COUNTS              SQL_PARC_BATCH
 SQL_CURSOR_COMMIT_BEHAVIOR              SQL_CB_PRESERVE, SQL_CB_CLOSE, SQL_CB_DELETE
 SQL_CURSOR_ROLLBACK_BEHAVIOR            SQL_CB_PRESERVE, SQL_CB_CLOSE, SQL_CB_DELETE
 SQL_EXPRESSIONS_IN_ORDERBY              "Y", "N"
 SQL_GETDATA_EXTENSIONS                  SQL_GD_ANY_COLUMN | SQL_GD_ANY_ORDER | SQL_GD_BLOCK | SQL_GD_BOUND

 */

/* ************************************************************************* */

/* ------------------------------------------------------------------------- */
/* Information Types - supported by SQLGetInfo */
/* ------------------------------------------------------------------------- */

/* ------------------------------------------------------------------------- */
/* ***** Driver Information */
/* ------------------------------------------------------------------------- */
/*
 SQL_ACTIVE_CONNECTIONS
 SQL_ACTIVE_STATEMENTS
 SQL_DATA_SOURCE_NAME
 SQL_DRIVER_HDBC
 SQL_DRIVER_HENV
 SQL_DRIVER_HLIB                              (ODBC 2.0)
 SQL_DRIVER_HSTMT
 SQL_DRIVER_NAME
 SQL_DRIVER_ODBC_VER                          (ODBC 2.0)
 SQL_DRIVER_VER
 SQL_FETCH_DIRECTION
 SQL_FILE_USAGE                               (ODBC 2.0)
 SQL_GETDATA_EXTENSIONS                       (ODBC 2.0)
 SQL_LOCK_TYPES                               (ODBC 2.0)
 SQL_ODBC_API_CONFORMANCE
 SQL_ODBC_SAG_CLI_CONFORMANCE
 SQL_ODBC_VER
 SQL_POS_OPERATIONS                           (ODBC 2.0)
 SQL_ROW_UPDATES
 SQL_SEARCH_PATTERN_ESCAPE
 SQL_SERVER_NAME
 */
/* ------------------------------------------------------------------------- */
/* ***** DBMS Product Information */
/* ------------------------------------------------------------------------- */
/*
 SQL_DATABASE_NAME
 SQL_DBMS_NAME
 SQL_DBMS_VER
 */
/* ------------------------------------------------------------------------- */
/* ***** Data Source Information */
/* ------------------------------------------------------------------------- */
/*
 SQL_ACCESSIBLE_PROCEDURES
 SQL_ACCESSIBLE_TABLES
 SQL_BOOKMARK_PERSISTENCE                     (ODBC 2.0)
 SQL_COLUMN_ALIAS                             (ODBC 2.0)
 SQL_CONCAT_NULL_BEHAVIOR
 SQL_CURCOR_COMMIT_BEHAVIOR
 SQL_CURSOR_ROLLBACK_BEHAVIOR
 SQL_DATA_SOURCE_READ_ONLY
 SQL_DEFAULT_TXN_ISOLATION
 SQL_MULT_RESULT_SET
 SQL_MULTIPLE_ACTIVE_TXN
 SQL_NEED_LONG_DATA_LEN                       (ODBC 2.0)
 SQL_NULL_COLLATION                           (ODBC 2.0)
 SQL_OWNER_TERM
 SQL_PROCEDURE_TERM
 SQL_QUALIFIER_TERM
 SQL_SCROLL_CONCURRENCY
 SQL_SCROLL_OPTIONS
 SQL_STATIC_SENSITIVITY                       (ODBC 2.0)
 SQL_TABLE_TERM
 SQL_TXN_CAPABLE
 SQL_TXN_ISOLATION_OPTION
 SQL_USER_NAME
 */
/* ------------------------------------------------------------------------- */
/* ***** Supported SQL */
/* ------------------------------------------------------------------------- */
/*
 SQL_ALTER_TABLE                              (ODBC 2.0)
 SQL_COLUMN_ALIAS
 SQL_CORRELATION_NAME
 SQL_EXPRESSIONS_IN_ORDERBY
 SQL_GROUP_BY                                 (ODBC 2.0)
 SQL_IDENTIFIER_CASE
 SQL_IDENTIFIER_QUOTE_CHAR
 SQL_KEYWORDS                                 (ODBC 2.0)
 SQL_LIKE_ESCAPE_CLAUSE                       (ODBC 2.0)
 SQL_NON_NULLABLE_COLUMNS
 SQL_ODBC_SQL_CONFORMANCE
 SQL_ODBC_SQL_OPT_IEF
 SQL_ORDER_BY_COLUMNS_IN_SELECT               (ODBC 2.0)
 SQL_OUTER_JOINS
 SQL_OWNER_USAGE                              (ODBC 2.0)
 SQL_POSITIONED_STATEMENTS                    (ODBC 2.0)
 SQL_PROCEDURES
 SQL_QUALIFIER_LOCATION                       (ODBC 2.0)
 SQL_QUALIFIER_NAME_SEPARATOR
 SQL_QUALIFIER_USAGE                          (ODBC 2.0)
 SQL_QUALIFIER_IDENTIFIER_CASE                (ODBC 2.0)
 SQL_SPECIAL_CHARACTERS                       (ODBC 2.0)
 SQL_SUBQUERIES                               (ODBC 2.0)
 SQL_UNION                                    (ODBC 2.0)
 */
/* ------------------------------------------------------------------------- */
/* ***** SQL Limits */
/* ------------------------------------------------------------------------- */
/*
 SQL_MAX_BINARY_LITERAL_LEN                   (ODBC 2.0)
 SQL_MAX_CHAR_LITERAL_LEN                     (ODBC 2.0)
 SQL_MAX_COLUMN_NAME_LEN
 SQL_MAX_COLUMNS_IN_GROUP_BY                  (ODBC 2.0)
 SQL_MAX_COLUMNS_ORDER_BY                     (ODBC 2.0)
 SQL_MAX_COLUMNS_IN_INDEX                     (ODBC 2.0)
 SQL_MAX_COLUMNS_IN_SELECT                    (ODBC 2.0)
 SQL_MAX_COLUMNS_IN_TABLE                     (ODBC 2.0)
 SQL_MAX_CURSOR_NAME_LEN
 SQL_MAX_INDEX_SIZE
 SQL_MAX_OWNER_NAME_LEN
 SQL_MAX_PROCEDURE_NAME_LEN
 SQL_MAX_QUALIFIER_NAME_LEN
 SQL_MAX_ROW_SIZE                             (ODBC 2.0)
 SQL_MAX_ROW_SIZE_INCLUDES_LONG               (ODBC 2.0)
 SQL_MAX_STATEMENT_LEN                        (OBDC 2.0)
 SQL_MAX_TABLE_NAME_LEN
 SQL_MAX_TABLES_IN_SELECT                     (ODBC 2.0)
 SQL_MAX_USER_NAME_LEN                        (ODBC 2.0)
 */
/* ------------------------------------------------------------------------- */
/* ***** Scalar Function Information */
/* ------------------------------------------------------------------------- */
/*
 SQL_CONVERT_FUNCTIONS
 SQL_NUMERIC_FUNCTIONS
 SQL_STRING_FUNCTIONS
 SQL_SYSTEM_FUNCTIONS
 SQL_TIMEDATE_ADD_INTERVALS                   (ODBC 2.0)
 SQL_TIMEDATE_DIFF_INTERVALS                  (ODBC 2.0)
 SQL_TIMEDATE_FUNCTIONS
 */
/* ------------------------------------------------------------------------- */
/* ***** Conversion Information */
/* ------------------------------------------------------------------------- */
/*
 SQL_CONVERT_BIGINT
 SQL_CONVERT_BINARY
 SQL_CONVERT_BIT
 SQL_CONVERT_CHAR
 SQL_CONVERT_DATE
 SQL_CONVERT_DECIMAL
 SQL_CONVERT_DOUBLE
 SQL_CONVERT_FLOAT
 SQL_CONVERT_INTEGER
 SQL_CONVERT_LONGVARBINARY
 SQL_CONVERT_LONGVARCHAR
 SQL_CONVERT_NUMERIC
 SQL_CONVERT_REAL
 SQL_CONVERT_SMALLINT
 SQL_CONVERT_TIME
 SQL_CONVERT_TIMESTAMP
 SQL_CONVERT_TINYINT
 SQL_CONVERT_VARBINARY
 SQL_CONVERT_VARCHAR
 */
/* ------- *** Options ODBC 3.0 *** ----------------------------------------

 SQL_ACTIVE_ENVIRONMENTSR
 SQL_AGGREGATE_FUNCTIONS
 SQL_ALTER_DOMAIN
 SQL_ASYNC_MODE
 SQL_BATCH_ROW_COUNT
 SQL_BATCH_SUPPORT
 SQL_CATALOG_NAME
 SQL_COLLATION_SEQ
 SQL_CREATE_ASSERTION
 SQL_DROP_ASSERTION
 SQL_CREATE_CHARACTER_SET
 SQL_DROP_CHARACTER_SET
 SQL_CREATE_COLLATION
 SQL_DROP_COLLATION
 SQL_CREATE_DOMAIN
 SQL_DROP_DOMAIN
 SQL_CREATE_SCHEMA
 SQL_DROP_SCHEMA
 SQL_CREATE_TABLE
 SQL_DROP_TABLE
 SQL_CREATE_TRANSLATION
 SQL_DROP_TRANSLATION
 SQL_CREATE_VIEW
 SQL_DROP_VIEW
 SQL_CURSOR_SENSITIVITY
 SQL_DATETIME_LITERALS
 SQL_DDL_INDEX
 SQL_DESCRIBE_PARAMETER
 SQL_DM_VER
 SQL_DRIVER_HDESC
 SQL_INDEX_KEYWORDS
 SQL_INFO_SCHEMA_VIEWS
 SQL_INSERT_STATEMENT
 SQL_MAX_ASYNC_CONCURRENT_STATEMENTS
 SQL_MAX_IDENTIFIER_LEN
 SQL_ODBC_INTERFACE_CONFORMANCE
 SQL_OJ_CAPABILITIES        SQLCHAR
 SQL_PARAM_ARRAY_ROW_COUNTS
 SQL_PARAM_ARRAY_SELECTS
 SQL_SQL_CONFORMANCE
 SQL_STANDARD_CLI_CONFORMANCE
 SQL_XOPEN_CLI_YEAR

 SQL_SQL92_DATETIME_FUNCTIONS
 SQL_SQL92_FOREIGN_KEY_DELETE_RULE
 SQL_SQL92_FOREIGN_KEY_UPDATE_RULE
 SQL_SQL92_GRANT
 SQL_SQL92_NUMERIC_VALUE_FUNCTIONS
 SQL_SQL92_PREDICATES
 SQL_SQL92_RELATIONAL_JOIN_OPERATORS
 SQL_SQL92_REVOKE
 SQL_SQL92_ROW_VALUE_CONSTRUCTOR
 SQL_SQL92_STRING_FUNCTIONS
 SQL_SQL92_VALUE_EXPRESSIONS

 SQL_DYNAMIC_CURSOR_ATTRIBUTES1   SQLUINTEGER
 SQL_DYNAMIC_CURSOR_ATTRIBUTES2   SQLUINTEGER
 SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1    SQLUINTEGER
 SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2    SQLUINTEGER
 SQL_KEYSET_CURSOR_ATTRIBUTES1    SQLUINTEGER
 SQL_KEYSET_CURSOR_ATTRIBUTES2    SQLUINTEGER
 SQL_STATIC_CURSOR_ATTRIBUTES1    SQLUINTEGER
 SQL_STATIC_CURSOR_ATTRIBUTES2    SQLUINTEGER
 ************************************************************************* */

void CheckConversion(SQLINTEGER rgbInfoValue, CHAR buffer[MAX_NAME_LEN])
{
  strcpy(buffer, " ");
  if (rgbInfoValue & SQL_CVT_BIGINT) strcat(buffer, "SQL_CVT_BIGINT");
  if (rgbInfoValue & SQL_CVT_BINARY) strcat(buffer, "SQL_CVT_BINARY");
  if (rgbInfoValue & SQL_CVT_BIT) strcat(buffer, "SQL_CVT_BIT");
  if (rgbInfoValue & SQL_CVT_CHAR) strcat(buffer, "SQL_CVT_CHAR");
  if (rgbInfoValue & SQL_CVT_DATE) strcat(buffer, "SQL_CVT_DATE");
  if (rgbInfoValue & SQL_CVT_DECIMAL) strcat(buffer, "SQL_CVT_DECIMAL");
  if (rgbInfoValue & SQL_CVT_DOUBLE) strcat(buffer, "SQL_CVT_DOUBLE");
  if (rgbInfoValue & SQL_CVT_FLOAT) strcat(buffer, "SQL_CVT_LONGVARBINARY");
  if (rgbInfoValue & SQL_CVT_INTEGER) strcat(buffer, "SQL_CVT_LONGVARBINARY");
  if (rgbInfoValue & SQL_CVT_LONGVARBINARY) strcat(buffer,
      "SQL_CVT_LONGVARBINARY");
  if (rgbInfoValue & SQL_CVT_LONGVARCHAR) strcat(buffer,
      "SQL_CVT_LONGVARBINARY");
  if (rgbInfoValue & SQL_CVT_NUMERIC) strcat(buffer, "SQL_CVT_NUMERIC");
  if (rgbInfoValue & SQL_CVT_REAL) strcat(buffer, "SQL_CVT_REAL");
  if (rgbInfoValue & SQL_CVT_SMALLINT) strcat(buffer, "SQL_CVT_SMALLINT");
  if (rgbInfoValue & SQL_CVT_TIME) strcat(buffer, "SQL_CVT_TIME");
  if (rgbInfoValue & SQL_CVT_TIMESTAMP) strcat(buffer, "SQL_CVT_TIMESTAMP");
  if (rgbInfoValue & SQL_CVT_TINYINT) strcat(buffer, "SQL_CVT_TINYINT");
  if (rgbInfoValue & SQL_CVT_VARBINARY) strcat(buffer, "SQL_CVT_VARBINARY");
  if (rgbInfoValue & SQL_CVT_VARCHAR) strcat(buffer, "SQL_CVT_VARCHAR");
}

/* *********************************************************************** */
/*
 -------------------------------------------------------------------------
 | 1.PutDriverInformation:
 |
 |
 | Returns:
 |
 -------------------------------------------------------------------------
 */
void PutDriverInformation(SQLHENV henv, SQLHDBC hdbc, SQLHSTMT hstmt)
{
  RETCODE retcode;
  CHAR buffer[MAX_NAME_LEN * 5];

  CHAR rgbInfoValueChar[MAX_RGB_VALUE];
  SQLINTEGER rgbInfoValue;
#ifdef WIN16
  SQLINTEGER FAR * rgbInfoValueHandle;
#endif
#ifdef WIN32
  ULONG FAR * rgbInfoValueHandle;
#endif
#if !defined( WIN32 ) && !defined( WIN16 )
  SQLLEN *rgbInfoValueHandle;
#endif
  SQLSMALLINT cbInfoValueMax;

#ifdef WIN16
  rgbInfoValueHandle = (SQLINTEGER *) malloc(sizeof(SQLINTEGER));
#endif
#ifdef WIN32
  rgbInfoValueHandle = (ULONG *) malloc(sizeof(ULONG));
#endif
#if !defined( WIN32 ) && !defined( WIN16 )
  rgbInfoValueHandle = (SQLLEN*)malloc(sizeof(SQLHANDLE));
#endif
  /* ---------------------------------------------------------------------- */
  /* ***** 1.) Driver Information */
  /* ---------------------------------------------------------------------- */
#if defined(GETINFO_ALL) || defined(GETINFO_OPT1)
  LOGF(" 1.) Driver Information -->");

  /* *** 1. SQL_ACTIVE_CONNECTIONS !*/

  LOGF("1.1.SQL_ACTIVE_CONNECTIONS : ");
  retcode = SQLGetInfo(hdbc, SQL_ACTIVE_CONNECTIONS, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 1.1");
  sprintf(buffer, "%d", (SQLSMALLINT)rgbInfoValue);
  LOGF("Value = '%s'", buffer);
  /* *** 2. SQL_ACTIVE_STATEMENTS !*/

  LOGF("1.2.SQL_ACTIVE_STATEMENTS : ");
  retcode = SQLGetInfo(hdbc, SQL_ACTIVE_STATEMENTS, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode, "SQLGetInfo 1.2");
  sprintf(buffer, "%d", (SQLSMALLINT)rgbInfoValue);
  LOGF("Value = '%s'", buffer);

  /* *** 3. SQL_DATA_SOURCE_NAME !*/
  LOGF("1.3.SQL_DATA_SOURCE_NAME : ");
  retcode = SQLGetInfo(hdbc, SQL_DATA_SOURCE_NAME, (PTR)rgbInfoValueChar,
      sizeof(rgbInfoValueChar), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode, "SQLGetInfo 1.3");
  LOGF("Value = '%s'", rgbInfoValueChar);

  /* *** 4. SQL_DRIVER_HDBC !*/
  LOGF("1.4.SQL_DRIVER_HDBC : ");
  cbInfoValueMax = sizeof(SQLINTEGER);
#ifdef WIN32
  memcpy(rgbInfoValueHandle, hdbc, sizeof(SQLHDBC));
  cbInfoValueMax= sizeof(ULONG);
#endif
#if !defined( WIN32 ) && !defined( WIN16 )
  memcpy(rgbInfoValueHandle, hdbc, sizeof(SQLHANDLE));
  cbInfoValueMax = sizeof(SQLHANDLE);
#endif
  retcode = SQLGetInfo(hdbc, SQL_DRIVER_HDBC, (PTR)rgbInfoValueHandle,
      cbInfoValueMax, NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode, "SQLGetInfo 1.4");
  sprintf(buffer, "%p", *rgbInfoValueHandle);
  LOGF("Value = '%s'", buffer);

  /* *** 5. SQL_DRIVER_HENV !*/
  LOGF("1.5.SQL_DRIVER_HENV : ");
  cbInfoValueMax = sizeof(SQLINTEGER);
#ifdef WIN32
  memcpy(rgbInfoValueHandle, henv, sizeof(SQLHENV));
  cbInfoValueMax= sizeof(ULONG);
#endif
#if !defined( WIN32 ) && !defined( WIN16 )
  memcpy(rgbInfoValueHandle, henv, sizeof(SQLHANDLE));
  cbInfoValueMax = sizeof(SQLHANDLE);
#endif
  retcode = SQLGetInfo(hdbc, SQL_DRIVER_HENV, (PTR)rgbInfoValueHandle,
      cbInfoValueMax, NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode, "SQLGetInfo 1.5");
  sprintf(buffer, "%p", *rgbInfoValueHandle);
  LOGF("Value = '%s'", buffer);

  /* *** 6. SQL_DRIVER_HLIB       (ODBC 2.0) !*/
  LOGF("1.6.SQL_DRIVER_HLIB (ODBC 2.0) : ");
  cbInfoValueMax = sizeof(SQLINTEGER);
#ifdef WIN32
  memcpy(rgbInfoValueHandle, hdbc, sizeof(SQLHDBC));
  cbInfoValueMax= sizeof(ULONG);
#endif
#if !defined( WIN32 ) && !defined( WIN16 )
  memcpy(rgbInfoValueHandle, hdbc, sizeof(SQLHANDLE));
  cbInfoValueMax = sizeof(SQLHANDLE);
#endif
  retcode = SQLGetInfo(hdbc, SQL_DRIVER_HLIB, (PTR)rgbInfoValueHandle,
      cbInfoValueMax, NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode, "SQLGetInfo 1.6");
  sprintf(buffer, "%p", *rgbInfoValueHandle);
  LOGF("Value = '%s'", buffer);

  /* *** 7. SQL_DRIVER_HSTMT ! */
  LOGF("1.7.SQL_DRIVER_HSTMT : ");
  cbInfoValueMax = sizeof(SQLINTEGER);
#ifdef WIN16
  *rgbInfoValueHandle = (SQLINTEGER) hstmt;
#endif
#ifdef WIN32
  memcpy(rgbInfoValueHandle, hstmt, sizeof(HSTMT));
  cbInfoValueMax= sizeof(ULONG);
#endif
#if !defined( WIN32 ) && !defined( WIN16 )
  memcpy(rgbInfoValueHandle, hstmt, sizeof(SQLHANDLE));
  cbInfoValueMax = sizeof(SQLHANDLE);
#endif
  retcode = SQLGetInfo(hdbc, SQL_DRIVER_HSTMT, (PTR)rgbInfoValueHandle,
      cbInfoValueMax, NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode, "SQLGetInfo 1.7");
  sprintf(buffer, "%p", *rgbInfoValueHandle);
  LOGF("Value = '%s'", buffer);

  /* *** 8. SQL_DRIVER_NAME !*/
  LOGF("1.8.SQL_DRIVER_NAME : ");
  retcode = SQLGetInfo(hdbc, SQL_DRIVER_NAME, (PTR)rgbInfoValueChar,
      sizeof(rgbInfoValueChar), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 1.8");
  LOGF("Value = '%s'", rgbInfoValueChar);

  /* *** 9. SQL_DRIVER_ODBC_VER   (ODBC 2.0) */
  LOGF("1.9.SQL_DRIVER_ODBC_VER (ODBC 2.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_DRIVER_ODBC_VER, (PTR)rgbInfoValueChar,
      sizeof(rgbInfoValueChar), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 1.9");
  LOGF("Value = '%s'", rgbInfoValueChar);

  /* *** 10. SQL_DRIVER_VER */
  LOGF("1.10.SQL_DRIVER_VER : ");
  retcode = SQLGetInfo(hdbc, SQL_DRIVER_VER, (PTR)rgbInfoValueChar,
      sizeof(rgbInfoValueChar), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 1.10");
  LOGF("Value = '%s'", rgbInfoValueChar);

  /* *** 11.SQL_FETCH_DIRECTION !*/  //DEPRECATED in ODBC 3.0
  /*LOGF("1.11.SQL_FETCH_DIRECTION : ");
   retcode = SQLGetInfo(hdbc, SQL_FETCH_DIRECTION, (PTR) & rgbInfoValue,
   sizeof(rgbInfoValue), NULL);
   DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
   "SQLGetInfo 1.11");
   strcpy(buffer, " ");
   if (rgbInfoValue & SQL_FD_FETCH_NEXT) strcat(buffer,
   "SQL_FD_FETCH_NEXT | ");
   if (rgbInfoValue & SQL_FD_FETCH_FIRST) strcat(buffer,
   "SQL_FD_FETCH_FIRST |");
   if (rgbInfoValue & SQL_FD_FETCH_LAST) strcat(buffer, "SQL_FD_FETCH_LAST |");
   if (rgbInfoValue & SQL_FD_FETCH_PRIOR) strcat(buffer,
   "SQL_FD_FETCH_PRIOR |");
   if (rgbInfoValue & SQL_FD_FETCH_ABSOLUTE) strcat(buffer,
   "SQL_FD_FETCH_ABSOLUTE |");
   if (rgbInfoValue & SQL_FD_FETCH_RELATIVE) strcat(buffer,
   "SQL_FD_FETCH_RELATIVE |");*/

  /* if (rgbInfoValue & SQL_FD_FETCH_RESUME)   strcat(buffer,"SQL_FD_FETCH_RESUME |"); */
  /*if (rgbInfoValue & SQL_FD_FETCH_BOOKMARK) strcat(buffer,
   "SQL_FD_FETCH_BOOKMARK");
   LOGF("Value = '%s'", buffer);*/

  /* *** 12.SQL_FILE_USAGE        (ODBC 2.0) !*/
  LOGF("1.12.SQL_FILE_USAGE (ODBC 2.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_FILE_USAGE, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode, "SQLGetInfo 1.12");
  /* switch (rgbInfoValue) {
   case SQL_FILE_NOT_SUPPORTED:
   strcpy(buffer, "SQL_FILE_NOT_SUPPORTED");
   break;
   case SQL_FILE_TABLE:
   strcpy(buffer, "SQL_FILE_TABLE");
   break;
   case SQL_FILE_QUALIFIER:
   strcpy(buffer, "SQL_FILE_QUALIFIER");
   break;
   default:
   sprintf(buffer, "%d", rgbInfoValue);
   }
   LOGF("Value = '%s'", buffer);*/

  /* *** 13.SQL_GETDATA_EXTENSIONS(ODBC 2.0) !*/
  LOGF("1.13.SQL_GETDATA_EXTENSIONS  (ODBC 2.0): ");
  retcode = SQLGetInfo(hdbc, SQL_GETDATA_EXTENSIONS, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 1.13");
  strcpy(buffer, " ");
  if (rgbInfoValue & SQL_GD_ANY_COLUMN) strcat(buffer, "SQL_GD_ANY_COLUMN |");
  if (rgbInfoValue & SQL_GD_ANY_ORDER) strcat(buffer, "SQL_GD_ANY_ORDER |");
  if (rgbInfoValue & SQL_GD_BLOCK) strcat(buffer, "SQL_GD_BLOCK |");
  if (rgbInfoValue & SQL_GD_BOUND) strcat(buffer, "SQL_GD_BOUND");
  LOGF("Value = '%s'", buffer);

  /* *** 14.SQL_LOCK_TYPES        (ODBC 2.0) !*/  //DEPRECATED in ODBC3.0
  /*LOGF("1.14.SQL_LOCK_TYPES (ODBC 2.0): ");
   retcode = SQLGetInfo(hdbc, SQL_LOCK_TYPES, (PTR) & rgbInfoValue,
   sizeof(rgbInfoValue), NULL);
   DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
   "SQLGetInfo 1.14");
   strcpy(buffer, " ");
   if (rgbInfoValue & SQL_LCK_NO_CHANGE) strcat(buffer, "SQL_LCK_NO_CHANGE |");
   if (rgbInfoValue & SQL_LCK_EXCLUSIVE) strcat(buffer, "SQL_LCK_EXCLUSIVE |");
   if (rgbInfoValue & SQL_LCK_UNLOCK) strcat(buffer, "SQL_LCK_UNLOCK");
   LOGF("Value = '%s'", buffer);*/

  /* *** 15.SQL_ODBC_API_CONFORMANCE !*///DEPRECATED in ODBC3.0
  /*LOGF("1.15.SQL_ODBC_API_CONFORMANCE : ");
   retcode = SQLGetInfo(hdbc, SQL_ODBC_API_CONFORMANCE, (PTR) & rgbInfoValue,
   sizeof(rgbInfoValue), NULL);
   DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
   "SQLGetInfo 1.15");
   switch (rgbInfoValue) {
   case SQL_OAC_NONE:
   strcpy(buffer, "SQL_OAC_NONE");
   break;
   case SQL_OAC_LEVEL1:
   strcpy(buffer, "SQL_OAC_LEVEL1");
   break;
   case SQL_OAC_LEVEL2:
   strcpy(buffer, "SQL_OAC_LEVEL2");
   break;
   default:
   strcpy(buffer, "!!!");
   }
   LOGF("Value = '%s'", buffer);*/

  /* *** 16.SQL_ODBC_SAG_CLI_CONFORMANCE !*///DEPRECATED in ODBC3.0
  /* LOGF("1.16.SQL_ODBC_SAG_CLI_CONFORMANCE : ");
   retcode = SQLGetInfo(hdbc, SQL_ODBC_SAG_CLI_CONFORMANCE,
   (PTR) & rgbInfoValue, sizeof(rgbInfoValue), NULL);
   DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
   "SQLGetInfo 1.16");
   switch (rgbInfoValue) {
   case SQL_OSCC_NOT_COMPLIANT:
   strcpy(buffer, "SQL_OSSCC_NOT_COMPLIANT");
   break;
   case SQL_OSCC_COMPLIANT:
   strcpy(buffer, "SQL_OSSCC_COMPLIANT");
   break;
   default:
   strcpy(buffer, "!!!");
   }
   LOGF("Value = '%s'", buffer);*/

  /* *** 17.SQL_ODBC_VER */
  LOGF("1.17.SQL_ODBC_VER : ");
  retcode = SQLGetInfo(hdbc, SQL_ODBC_VER, (PTR) & rgbInfoValueChar,
      sizeof(rgbInfoValueChar), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 1.17");
  LOGF("Value = '%s'", rgbInfoValueChar);

  /* *** 18.SQL_POS_OPERATIONS (ODBC 2.0) !*/ //DEPRECATED in ODBC3.0
  /*LOGF("1.18.SQL_POS_OPERATIONS (ODBC 2.0) : ");
   retcode = SQLGetInfo(hdbc, SQL_POS_OPERATIONS, (PTR) & rgbInfoValue,
   sizeof(rgbInfoValue), NULL);
   DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
   "SQLGetInfo 1.18");
   strcpy(buffer, " ");
   if (rgbInfoValue & SQL_POS_POSITION) strcat(buffer, "SQL_POS_POSITION |");
   if (rgbInfoValue & SQL_POS_REFRESH) strcat(buffer, "SQL_POS_REFRESH |");
   if (rgbInfoValue & SQL_POS_UPDATE) strcat(buffer, "SQL_POS_UPDATE |");
   if (rgbInfoValue & SQL_POS_DELETE) strcat(buffer, "SQL_POS_DELETE |");
   if (rgbInfoValue & SQL_POS_ADD) strcat(buffer, "SQL_POS_ADD");
   LOGF("Value = '%s'", buffer);*/

  /* *** 19.SQL_ROW_UPDATES !*/
  LOGF("1.19.SQL_ROW_UPDATES : ");
  retcode = SQLGetInfo(hdbc, SQL_ROW_UPDATES, (PTR) & rgbInfoValueChar,
      sizeof(rgbInfoValueChar), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode, "SQLGetInfo 1.19");
  LOGF("Value = '%s'", rgbInfoValueChar);

  /* *** 20.SQL_SEARCH_PATTERN_ESCAPE !*/
  LOGF("1.20.SQL_SEARCH_PATTERN_ESCAPE : ");
  retcode = SQLGetInfo(hdbc, SQL_SEARCH_PATTERN_ESCAPE,
      (PTR) & rgbInfoValueChar, sizeof(rgbInfoValueChar), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode, "SQLGetInfo 1.20");
  LOGF("Value = '%s'", rgbInfoValueChar);

  /* *** 21.SQL_SERVER_NAME !*/
  LOGF("1.21.SQL_SERVER_NAME : ");
  retcode = SQLGetInfo(hdbc, SQL_SERVER_NAME, (PTR) & rgbInfoValueChar,
      sizeof(rgbInfoValueChar), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode, "SQLGetInfo 1.21");
  LOGF("Value = '%s'", rgbInfoValueChar);

  free(rgbInfoValueHandle);
#endif
}
/*
 -------------------------------------------------------------------------
 | 2.PutDBMSProductInformation:
 |
 |
 | Returns:
 |
 -------------------------------------------------------------------------
 */
void PutDBMSProductInformation(SQLHENV henv, SQLHDBC hdbc, SQLHSTMT hstmt)
{
  RETCODE retcode;

  CHAR rgbInfoValueChar[MAX_RGB_VALUE];
  /* ---------------------------------------------------------------------- */
  /* ***** 2.) DBMS Product Information */
  /* ------------------------------------------------------------------------- */
#if defined(GETINFO_ALL) || defined(GETINFO_OPT2)
  LOGF(" 2.) DBMS Product Information -->");

  /* *** 1.SQL_DATABASE_NAME !*/
  LOGF("2.1.SQL_DATABASE_NAME : ");
  retcode = SQLGetInfo(hdbc, SQL_DATABASE_NAME, (PTR) & rgbInfoValueChar,
      sizeof(rgbInfoValueChar), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 2.1");
  LOGF("Value = '%s'", rgbInfoValueChar);

  /* *** 2.SQL_DBMS_NAME !*/
  LOGF("2.2.SQL_DBMS_NAME : ");
  retcode = SQLGetInfo(hdbc, SQL_DBMS_NAME, (PTR) & rgbInfoValueChar,
      sizeof(rgbInfoValueChar), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 2.2");
  LOGF("Value = '%s'", rgbInfoValueChar);

  /* *** 3.SQL_DBMS_VER !*/
  LOGF("2.3.SQL_DBMS_VER : ");
  retcode = SQLGetInfo(hdbc, SQL_DBMS_VER, (PTR) & rgbInfoValueChar,
      sizeof(rgbInfoValueChar), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 2.3");
  LOGF("Value = '%s'", rgbInfoValueChar);
#endif
}
/*
 -------------------------------------------------------------------------
 | 3.PutDataSourceInformation:
 |
 |
 | Returns:
 |
 -------------------------------------------------------------------------
 */
void PutDataSourceInformation(SQLHENV henv, SQLHDBC hdbc, SQLHSTMT hstmt)
{
  RETCODE retcode;
  CHAR buffer[MAX_NAME_LEN * 5];

  CHAR rgbInfoValueChar[MAX_RGB_VALUE];
  SQLINTEGER rgbInfoValue;
  /* ---------------------------------------------------------------------- */
  /* ***** 3.) Data Source Information */
  /* ---------------------------------------------------------------------- */
#if defined(GETINFO_ALL) || defined(GETINFO_OPT3)
  LOGF(" 3.) Data Source Information -->");

  /* *** 1.SQL_ACCESSIBLE_PROCEDURES */
  LOGF("3.1.SQL_ACCESSIBLE_PROCEDURES : ");
  retcode = SQLGetInfo(hdbc, SQL_ACCESSIBLE_PROCEDURES,
      (PTR) & rgbInfoValueChar, sizeof(rgbInfoValueChar), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 3.1");
  LOGF("Value = '%s'", rgbInfoValueChar);

  /* *** 2.SQL_ACCESSIBLE_TABLES !*/
  LOGF("3.2.SQL_ACCESSIBLE_TABLES : ");
  retcode = SQLGetInfo(hdbc, SQL_ACCESSIBLE_TABLES, (PTR) & rgbInfoValueChar,
      sizeof(rgbInfoValueChar), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 3.2");
  LOGF("Value = '%s'", rgbInfoValueChar);

  /* *** 3.SQL_BOOKMARK_PERSISTENCE (ODBC 2.0) !*/
  LOGF("3.3.SQL_BOOKMARK_PERSISTENCE (ODBC 2.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_BOOKMARK_PERSISTENCE, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 3.3");
  strcpy(buffer, " ");
  if (rgbInfoValue & SQL_BP_CLOSE) strcat(buffer, "SQL_BP_CLOSE |");
  if (rgbInfoValue & SQL_BP_DELETE) strcat(buffer, "SQL_BP_DELETE |");
  if (rgbInfoValue & SQL_BP_DROP) strcat(buffer, "SQL_BP_DROP |");
  if (rgbInfoValue & SQL_BP_SCROLL) strcat(buffer, "SQL_BP_SCROLL |");
  if (rgbInfoValue & SQL_BP_TRANSACTION) strcat(buffer,
      "SQL_BP_TRANSACTION |");
  if (rgbInfoValue & SQL_BP_UPDATE) strcat(buffer, "SQL_BP_UPDATE |");
  if (rgbInfoValue & SQL_BP_OTHER_HSTMT) strcat(buffer, "SQL_BP_OTHER_HSTMT");
  LOGF("Value = '%s'", buffer);

  /* *** 4.SQL_CONCAT_NULL_BEHAVIOR! */
  LOGF("3.4.SQL_CONCAT_NULL_BEHAVIOR : ");
  retcode = SQLGetInfo(hdbc, SQL_CONCAT_NULL_BEHAVIOR, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 3.4");
  switch (rgbInfoValue) {
    case SQL_CB_NULL:
      strcpy(buffer, "SQL_CB_NULL");
      break;
    case SQL_CB_NON_NULL:
      strcpy(buffer, "SQL_CB_NON_NULL");
      break;
    default:
      strcpy(buffer, "!!!");
  }
  LOGF("Value = '%s'", buffer);

  /* *** 5.SQL_CURCOR_COMMIT_BEHAVIOR !*/
  LOGF("3.5.SQL_CURSOR_COMMIT_BEHAVIOR : ");
  retcode = SQLGetInfo(hdbc, SQL_CURSOR_COMMIT_BEHAVIOR, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 3.5");
  switch (rgbInfoValue) {
    case SQL_CB_DELETE:
      strcpy(buffer, "SQL_CB_DELETE");
      break;
    case SQL_CB_CLOSE:
      strcpy(buffer, "SQL_CB_CLOSE");
      break;
    case SQL_CB_PRESERVE:
      strcpy(buffer, "SQL_CB_PRESERVE");
      break;
    default:
      strcpy(buffer, "!!!");
  }
  LOGF("Value = '%s'", buffer);

  /* *** 6.SQL_CURSOR_ROLLBACK_BEHAVIOR !*/
  LOGF("3.6.SQL_CURSOR_ROLLBACK_BEHAVIOR : ");
  retcode = SQLGetInfo(hdbc, SQL_CURSOR_ROLLBACK_BEHAVIOR,
      (PTR) & rgbInfoValue, sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 3.6");
  switch (rgbInfoValue) {
    case SQL_CB_DELETE:
      strcpy(buffer, "SQL_CB_DELETE");
      break;
    case SQL_CB_CLOSE:
      strcpy(buffer, "SQL_CB_CLOSE");
      break;
    case SQL_CB_PRESERVE:
      strcpy(buffer, "SQL_CB_PRESERVE");
      break;
    default:
      strcpy(buffer, "!!!");
  }
  LOGF("Value = '%s'", buffer);

  /* *** 7.SQL_DATA_SOURCE_READ_ONLY !*/
  LOGF("3.7.SQL_DATA_SOURCE_READ_ONLY : ");
  retcode = SQLGetInfo(hdbc, SQL_DATA_SOURCE_READ_ONLY,
      (PTR) & rgbInfoValueChar, sizeof(rgbInfoValueChar), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 3.7");
  LOGF("Value = '%s'", rgbInfoValueChar);

  /* *** 8.SQL_DEFAULT_TXN_ISOLATION !*/
  LOGF("3.8.SQL_DEFAULT_TXN_ISOLATION : ");
  retcode = SQLGetInfo(hdbc, SQL_DEFAULT_TXN_ISOLATION, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 3.8");
  strcpy(buffer, " ");
  if (rgbInfoValue & SQL_TXN_READ_UNCOMMITTED) strcat(buffer,
      "SQL_TXN_READ_UNCOMMITTED |");
  if (rgbInfoValue & SQL_TXN_READ_COMMITTED) strcat(buffer,
      "SQL_TXN_READ_COMMITTED |");
  if (rgbInfoValue & SQL_TXN_REPEATABLE_READ) strcat(buffer,
      "SQL_TXN_REPEATABLE_READ |");
  if (rgbInfoValue & SQL_TXN_SERIALIZABLE) strcat(buffer,
      "SQL_TXN_SERIALIZABLE |");
  /* if (rgbInfoValue & SQL_TXN_VERSIONING)       strcat(buffer,"SQL_TXN_VERSIONING"); */
  LOGF("Value = '%s'", buffer);

  /* *** 9.SQL_MULT_RESULT_SETS !*/
  LOGF("3.9.SQL_MULT_RESULT_SETS : ");
  retcode = SQLGetInfo(hdbc, SQL_MULT_RESULT_SETS, (PTR) & rgbInfoValueChar,
      sizeof(rgbInfoValueChar), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 3.9");
  LOGF("Value = '%s'", rgbInfoValueChar);

  /* *** 10.SQL_MULTIPLE_ACTIVE_TXN !*/
  LOGF("3.10.SQL_MULTIPLE_ACTIVE_TXN : ");
  retcode = SQLGetInfo(hdbc, SQL_MULTIPLE_ACTIVE_TXN, (PTR) & rgbInfoValueChar,
      sizeof(rgbInfoValueChar), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 3.10");
  LOGF("Value = '%s'", rgbInfoValueChar);

  /* *** 11.SQL_NEED_LONG_DATA_LEN (ODBC 2.0) !*/
  LOGF("3.11.SQL_NEED_LONG_DATA_LEN (ODBC 2.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_NEED_LONG_DATA_LEN, (PTR) & rgbInfoValueChar,
      sizeof(rgbInfoValueChar), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 3.11");
  LOGF("Value = '%s'", rgbInfoValueChar);

  /* *** 12.SQL_NULL_COLLATION (ODBC 2.0) !*/
  LOGF("3.12.SQL_NULL_COLLATION (ODBC 2.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_NULL_COLLATION, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 3.12");
  switch (rgbInfoValue) {
    case SQL_NC_END:
      strcpy(buffer, "SQL_NC_END");
      break;
    case SQL_NC_HIGH:
      strcpy(buffer, "SQL_NC_HIGH");
      break;
    case SQL_NC_LOW:
      strcpy(buffer, "SQL_NC_LOW");
      break;
    case SQL_NC_START:
      strcpy(buffer, "SQL_NC_START");
      break;
    default:
      strcpy(buffer, "!!!");
  }
  LOGF("Value = '%s'", buffer);

  /* *** 13.SQL_OWNER_TERM !*/
  LOGF("3.13.SQL_OWNER_TERM : ");
  retcode = SQLGetInfo(hdbc, SQL_OWNER_TERM, (PTR) & rgbInfoValueChar,
      sizeof(rgbInfoValueChar), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 3.13");
  LOGF("Value = '%s'", rgbInfoValueChar);

  /* *** 14.SQL_PROCEDURE_TERM */
  LOGF("3.14.SQL_PROCEDURE_TERM : ");
  retcode = SQLGetInfo(hdbc, SQL_PROCEDURE_TERM, (PTR) & rgbInfoValueChar,
      sizeof(rgbInfoValueChar), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 3.14");
  LOGF("Value = '%s'", rgbInfoValueChar);

  /* *** 15.SQL_QUALIFIER_TERM !*/
  LOGF("3.15.SQL_QUALIFIER_TERM : ");
  retcode = SQLGetInfo(hdbc, SQL_QUALIFIER_TERM, (PTR) & rgbInfoValueChar,
      sizeof(rgbInfoValueChar), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 3.15");
  LOGF("Value = '%s'", rgbInfoValueChar);

  /* *** 16.SQL_SCROLL_CONCURRENCY !*/
  LOGF("3.16.SQL_SCROLL_CONCURRENCY : ");
  retcode = SQLGetInfo(hdbc, SQL_SCROLL_CONCURRENCY, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 3.16");
  strcpy(buffer, " ");
  if (rgbInfoValue & SQL_SCCO_READ_ONLY) strcat(buffer,
      "SQL_SCCO_READ_ONLY |");
  if (rgbInfoValue & SQL_SCCO_LOCK) strcat(buffer, "SQL_SCCO_LOCK |");
  if (rgbInfoValue & SQL_SCCO_OPT_ROWVER) strcat(buffer,
      "SQL_SCCO_OPT_ROWVER |");
  if (rgbInfoValue & SQL_SCCO_OPT_VALUES) strcat(buffer,
      "SQL_SCCO_OPT_VALUES |");
  LOGF("Value = '%s'", buffer);

  /* *** 17.SQL_SCROLL_OPTIONS !*/
  LOGF("3.17.SQL_SCROLL_OPTIONS : ");
  retcode = SQLGetInfo(hdbc, SQL_SCROLL_OPTIONS, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 3.17");
  strcpy(buffer, " ");
  if (rgbInfoValue & SQL_SO_FORWARD_ONLY) strcat(buffer,
      "SQL_SCCO_FORWARD_ONLY |");
  if (rgbInfoValue & SQL_SO_STATIC) strcat(buffer, "SQL_SO_FORWARD_ONLY |");
  if (rgbInfoValue & SQL_SO_KEYSET_DRIVEN) strcat(buffer,
      "SQL_SO_KEYSET_DRIVEN |");
  if (rgbInfoValue & SQL_SO_DYNAMIC) strcat(buffer, "SQL_SO_DYNAMIC |");
  if (rgbInfoValue & SQL_SO_MIXED) strcat(buffer, "SQL_SO_MIXED");
  LOGF("Value = '%s'", buffer);

  /* *** 18.SQL_STATIC_SENSITIVITY (ODBC 2.0) !*/
  LOGF("3.18.SQL_STATIC_SENSITIVITY (ODBC 2.0): ");
  retcode = SQLGetInfo(hdbc, SQL_STATIC_SENSITIVITY, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 3.18");
  strcpy(buffer, " ");
  if (rgbInfoValue & SQL_SS_ADDITIONS) strcat(buffer, "SQL_ADDITIONS |");
  if (rgbInfoValue & SQL_SS_DELETIONS) strcat(buffer, "SQL_DELETIONS |");
  if (rgbInfoValue & SQL_SS_UPDATES) strcat(buffer, "SQL_UPDATES");
  LOGF("Value = '%s'", buffer);

  /* *** 19.SQL_TABLE_TERM !*/
  LOGF("3.19.SQL_TABLE_TERM : ");
  retcode = SQLGetInfo(hdbc, SQL_TABLE_TERM, (PTR) & rgbInfoValueChar,
      sizeof(rgbInfoValueChar), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 3.19");
  LOGF("Value = '%s'", rgbInfoValueChar);

  /* *** 20.SQL_TXN_CAPABLE !*/
  LOGF("3.20.SQL_TXN_CAPABLE: ");
  retcode = SQLGetInfo(hdbc, SQL_TXN_CAPABLE, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 3.20");
  switch (rgbInfoValue) {
    case SQL_TC_NONE:
      strcpy(buffer, "SQL_ADDITIONS");
      break;
    case SQL_TC_DML:
      strcpy(buffer, "SQL_TC_DML");
      break;
    case SQL_TC_DDL_COMMIT:
      strcpy(buffer, "SQL_TC_DDL_COMMIT");
      break;
    case SQL_TC_DDL_IGNORE:
      strcpy(buffer, "SQL_TC_DLL_IGNORE");
      break;
    case SQL_TC_ALL:
      strcpy(buffer, "SQL_TC_ALL");
      break;
    default:
      strcpy(buffer, "!!!");
  }
  LOGF("Value = '%s'", buffer);

  /* *** 21.SQL_TXN_ISOLATION_OPTION !*/
  LOGF("3.21.SQL_TXN_ISOLATION_OPTION : ");
  retcode = SQLGetInfo(hdbc, SQL_TXN_ISOLATION_OPTION, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 3.21");
  strcpy(buffer, " ");
  if (rgbInfoValue & SQL_TXN_READ_UNCOMMITTED) strcat(buffer,
      "SQL_TXN_READ_UNCOMMITTED |");
  if (rgbInfoValue & SQL_TXN_READ_COMMITTED) strcat(buffer,
      "SQL_TXN_READ_COMMITTED |");
  if (rgbInfoValue & SQL_TXN_REPEATABLE_READ) strcat(buffer,
      "SQL_TXN_REPEATABLE_READ |");
  if (rgbInfoValue & SQL_TXN_SERIALIZABLE) strcat(buffer,
      "SQL_TXN_SERIALIZABLE |");
  /* if (rgbInfoValue & SQL_TXN_VERSIONING)       strcat(buffer,"SQL_TXN_VERSIONING"); */
  LOGF("Value = '%s'", buffer);

  /* *** 22.SQL_USER_NAME !*/
  LOGF("3.22.SQL_USER_NAME : ");
  retcode = SQLGetInfo(hdbc, SQL_USER_NAME, (PTR) & rgbInfoValueChar,
      sizeof(rgbInfoValueChar), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 3.22");
  LOGF("Value = '%s'", rgbInfoValueChar);
#endif
}
/*
 -------------------------------------------------------------------------
 | 4.PutSuppotedSQL:
 |
 |
 | Returns:
 |
 -------------------------------------------------------------------------
 */
void PutSupportedSQL(SQLHENV henv, SQLHDBC hdbc, SQLHSTMT hstmt)
{
  RETCODE retcode;
  CHAR buffer[MAX_NAME_LEN * 5];

  CHAR rgbInfoValueChar[MAX_RGB_VALUE * 3];
  SQLINTEGER rgbInfoValue;
  /* ---------------------------------------------------------------------- */
  /* ***** 4.) Supported SQL */
  /* ---------------------------------------------------------------------- */
#if defined(GETINFO_ALL) || defined(GETINFO_OPT4)
  LOGF(" 4.) Supported SQL -->");

  /* *** 1.SQL_ALTER_TABLE (ODBC 2.0) !*/
  LOGF("4.1.SQL_ALTER_TABLE (ODBC 2.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_ALTER_TABLE, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 4.1");
  strcpy(buffer, " ");
  if (rgbInfoValue & SQL_AT_ADD_COLUMN) strcat(buffer, "SQL_AT_ADD_COLUMN |");
  if (rgbInfoValue & SQL_AT_DROP_COLUMN) strcat(buffer, "SQL_AT_DROP_COLUMN");
  LOGF("Value = '%s'", buffer);

  /* *** 2.SQL_COLUMN_ALIAS (ODBC 2.0) !*/
  LOGF("4.2.SQL_COLUMN_ALIAS (ODBC 2.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_COLUMN_ALIAS, (PTR) & rgbInfoValueChar,
      sizeof(rgbInfoValueChar), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 4.2");
  LOGF("Value = '%s'", rgbInfoValueChar);

  /* *** 3.SQL_CORRELATION_NAME !*/
  LOGF("4.3.SQL_CORRELATION_NAME : ");
  retcode = SQLGetInfo(hdbc, SQL_CORRELATION_NAME, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 4.3");
  switch (rgbInfoValue) {
    case SQL_CN_NONE:
    strcpy(buffer, "SQL_CN_NONE");
    break;
    case SQL_CN_DIFFERENT:
    strcpy(buffer, "SQL_CN_DIFFERENT");
    break;
    case SQL_CN_ANY:
    strcpy(buffer, "SQL_CN_ANY");
    break;
    default:
    strcpy(buffer, "!!!");
  }
  LOGF("Value = '%s'", buffer);

  /* *** 4.SQL_EXPRESSIONS_IN_ORDERBY !*/
  LOGF("4.4.SQL_EXPRESSIONS_IN_ORDERBY : ");
  retcode = SQLGetInfo(hdbc, SQL_EXPRESSIONS_IN_ORDERBY,
      (PTR) & rgbInfoValueChar, sizeof(rgbInfoValueChar), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 4.4");
  LOGF("Value = '%s'", rgbInfoValueChar);

  /* *** 5.SQL_GROUP_BY   (ODBC 2.0) !*/
  LOGF("4.5.SQL_GROUP_BY (ODBC 2.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_GROUP_BY, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 4.5");
  switch (rgbInfoValue) {
    case SQL_GB_NOT_SUPPORTED:
    strcpy(buffer, "SQL_GB_NOT_SUPPORTED");
    break;
    case SQL_GB_GROUP_BY_EQUALS_SELECT:
    strcpy(buffer, "SQL_GB_GROUP_BY_EQUALS_SELECT");
    break;
    case SQL_GB_GROUP_BY_CONTAINS_SELECT:
    strcpy(buffer, "SQL_GB_GROUP_BY_CONTAINS_SELECT");
    break;
    case SQL_GB_NO_RELATION:
    strcpy(buffer, "SQL_GB_NO_RELATION");
    break;
    default:
    strcpy(buffer, "!!!");
  }
  LOGF("Value = '%s'", buffer);

  /* *** 6.SQL_IDENTIFIER_CASE !*/
  LOGF("4.6.SQL_IDENTIFIER_CASE : ");
  retcode = SQLGetInfo(hdbc, SQL_IDENTIFIER_CASE, (PTR) & rgbInfoValueChar,
      sizeof(rgbInfoValueChar), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 4.6");
  switch (rgbInfoValue) {
    case SQL_IC_UPPER:
    strcpy(buffer, "SQL_IC_UPPER");
    break;
    case SQL_IC_LOWER:
    strcpy(buffer, "SQL_IC_LOWER");
    break;
    case SQL_IC_SENSITIVE:
    strcpy(buffer, "SQL_IC_SENSITIVE");
    break;
    case SQL_IC_MIXED:
    strcpy(buffer, "SQL_IC_MIXED");
    break;
    default:
    strcpy(buffer, "!!!");
  }
  LOGF("Value = '%s'", buffer);

  /* *** 7.SQL_IDENTIFIER_QUOTE_CHAR !*/
  LOGF("4.7.SQL_IDENTIFIER_QUOTE_CHAR : ");
  retcode = SQLGetInfo(hdbc, SQL_IDENTIFIER_QUOTE_CHAR,
      (PTR) & rgbInfoValueChar, sizeof(rgbInfoValueChar), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 4.7");
  LOGF("Value = '%s'", rgbInfoValueChar);

  /* *** 8.SQL_KEYWORDS (ODBC 2.0) !*/
  LOGF("4.8.SQL_KEYWORDS (ODBC 2.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_KEYWORDS, (PTR) & rgbInfoValueChar,
      sizeof(rgbInfoValueChar), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 4.8");
  sprintf(buffer, "\tValue = ");
  LOG(buffer);

  /* *** 9.SQL_LIKE_ESCAPE_CLAUSE  (ODBC 2.0) !*/
  LOGF("4.9.SQL_LIKE_ESCAPE_CLAUSE (ODBC 2.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_LIKE_ESCAPE_CLAUSE, (PTR) & rgbInfoValueChar,
      sizeof(rgbInfoValueChar), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 4.9");
  LOGF("Value = '%s'", rgbInfoValueChar);

  /* *** 10.SQL_NON_NULLABLE_COLUMNS !*/
  LOGF("4.10.SQL_NON_NULLABLE_COLUMNS : ");
  retcode = SQLGetInfo(hdbc, SQL_NON_NULLABLE_COLUMNS, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 4.10");
  switch (rgbInfoValue) {
    case SQL_NNC_NULL:
    strcpy(buffer, "SQL_NNC_NULL");
    break;
    case SQL_NNC_NON_NULL:
    strcpy(buffer, "SQL_NNC_NON_NULL");
    break;
    default:
    strcpy(buffer, "!!!");
  }
  LOGF("Value = '%s'", buffer);

  /* *** 11.SQL_ODBC_SQL_CONFORMANCE !*/
  LOGF("4.11.SQL_ODBC_SQL_CONFORMANCE : ");
  retcode = SQLGetInfo(hdbc, SQL_ODBC_SQL_CONFORMANCE, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 4.11");
  switch (rgbInfoValue) {
    case SQL_OSC_MINIMUM:
    strcpy(buffer, "SQL_OSC_MINIMUM");
    break;
    case SQL_OSC_CORE:
    strcpy(buffer, "SQL_OSC_CORE");
    break;
    case SQL_OSC_EXTENDED:
    strcpy(buffer, "SQL_OSC_EXTENDED");
    break;
    default:
    strcpy(buffer, "!!!");
  }
  LOGF("Value = '%s'", buffer);

  /* *** 12.SQL_ODBC_SQL_OPT_IEF !*/
  LOGF("4.12.SQL_ODBC_SQL_OPT_IEF : ");
  retcode = SQLGetInfo(hdbc, SQL_ODBC_SQL_OPT_IEF, (PTR) & rgbInfoValueChar,
      sizeof(rgbInfoValueChar), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 4.12");
  LOGF("Value = '%s'", rgbInfoValueChar);

  /* *** 13.SQL_ORDER_BY_COLUMNS_IN_SELECT (ODBC 2.0) !*/
  LOGF("4.13.SQL_ORDER_BY_COLUMNS_IN_SELECT (ODBC 2.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_ORDER_BY_COLUMNS_IN_SELECT,
      (PTR) & rgbInfoValueChar, sizeof(rgbInfoValueChar), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 4.13");
  LOGF("Value = '%s'", rgbInfoValueChar);

  /* *** 14.SQL_OUTER_JOINS !*/
  LOGF("4.14.SQL_OUTER_JOINS : ");
  retcode = SQLGetInfo(hdbc, SQL_OUTER_JOINS, (PTR) & rgbInfoValueChar,
      sizeof(rgbInfoValueChar), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 4.14");
  LOGF("Value = '%s'", rgbInfoValueChar);

  /* *** 15.SQL_OWNER_USAGE (ODBC 2.0) !*/
  LOGF("4.15.SQL_OWNER_USAGE (ODBC 2.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_OWNER_USAGE, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 4.15");
  strcpy(buffer, " ");
  if (rgbInfoValue & SQL_OU_DML_STATEMENTS) strcat(buffer,
      "SQL_OU_DML_STATEMENTS |");
  if (rgbInfoValue & SQL_OU_PROCEDURE_INVOCATION) strcat(buffer,
      "SQL_OU_PRECEDURE_INVOCATION |");
  if (rgbInfoValue & SQL_OU_TABLE_DEFINITION) strcat(buffer,
      "SQL_OU_TABLE_DEFINITION |");
  if (rgbInfoValue & SQL_OU_INDEX_DEFINITION) strcat(buffer,
      "SQL_OU_INDEX_DEFINITION |");
  if (rgbInfoValue & SQL_OU_PRIVILEGE_DEFINITION) strcat(buffer,
      "SQL_OU_PRIVILEGE_DEFINITION");
  LOGF("Value = '%s'", buffer);

  /* *** 16.SQL_POSITIONED_STATEMENTS (ODBC 2.0) !*/
  LOGF("4.16.SQL_POSITIONED_STATEMENTS (ODBC 2.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_POSITIONED_STATEMENTS, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 4.16");
  strcpy(buffer, " ");
  if (rgbInfoValue & SQL_PS_POSITIONED_DELETE) strcat(buffer,
      "SQL_PS_POSITIONED_DELETE |");
  if (rgbInfoValue & SQL_PS_POSITIONED_UPDATE) strcat(buffer,
      "SQL_PS_POSITIONED_UPDATE |");
  if (rgbInfoValue & SQL_PS_SELECT_FOR_UPDATE) strcat(buffer,
      "SQL_PS_SELECT_FOR_UPDATE");
  LOGF("Value = '%s'", buffer);

  /* *** 17.SQL_PROCEDURES */
  LOGF("4.17.SQL_PROCEDURES : ");
  retcode = SQLGetInfo(hdbc, SQL_PROCEDURES, (PTR) & rgbInfoValueChar,
      sizeof(rgbInfoValueChar), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 4.17");
  LOGF("Value = '%s'", rgbInfoValueChar);

  /* *** 18.SQL_QUALIFIER_LOCATION (ODBC 2.0) !*/
  LOGF("4.18.SQL_QUALIFIER_LOCATION (ODBC 2.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_QUALIFIER_LOCATION, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 4.18");
  switch (rgbInfoValue) {
    case SQL_QL_START:
    strcpy(buffer, "SQL_QL_START");
    break;
    case SQL_QL_END:
    strcpy(buffer, "SQL_QL_END");
    break;
    default:
    sprintf(buffer, "%d", rgbInfoValue);
  }
  LOGF("Value = '%s'", buffer);

  /* *** 19.SQL_QUALIFIER_NAME_SEPARATOR !*/
  LOGF("4.19.SQL_QUALIFIER_NAME_SEPARATOR : ");
  retcode = SQLGetInfo(hdbc, SQL_QUALIFIER_NAME_SEPARATOR,
      (PTR) & rgbInfoValueChar, sizeof(rgbInfoValueChar), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 4.19");
  LOGF("Value = '%s'", rgbInfoValueChar);

  /* *** 20.SQL_QUALIFIER_USAGE (ODBC 2.0) !*/
  LOGF("4.20.SQL_QUALIFIER_USAGE (ODBC 2.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_QUALIFIER_USAGE, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 4.20");
  strcpy(buffer, " ");
  if (rgbInfoValue & SQL_QU_DML_STATEMENTS) strcat(buffer,
      "SQL_QU_DML_STATEMENTS |");
  if (rgbInfoValue & SQL_QU_PROCEDURE_INVOCATION) strcat(buffer,
      "SQL_QU_PROCEDURE_INVOCATION |");
  if (rgbInfoValue & SQL_QU_TABLE_DEFINITION) strcat(buffer,
      "SQL_QU_TABLE_DEFINITION |");
  if (rgbInfoValue & SQL_QU_INDEX_DEFINITION) strcat(buffer,
      "SQL_QU_INDEX_DEFINITION |");
  if (rgbInfoValue & SQL_QU_PRIVILEGE_DEFINITION) strcat(buffer,
      "SQL_QU_PRIVILEGE_DEFINITION");
  LOGF("Value = '%s'", buffer);

  /* *** 21.SQL_QUOTED_IDENTIFIER_CASE                 (ODBC 2.0) */
  LOGF("4.21.SQL_QUOTED_IDENTIFIER_CASE (ODBC 2.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_QUOTED_IDENTIFIER_CASE, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 4.21");
  switch (rgbInfoValue) {
    case SQL_IC_UPPER:
    strcpy(buffer, "SQL_IC_UPPER");
    break;
    case SQL_IC_LOWER:
    strcpy(buffer, "SQL_IC_LOWER");
    break;
    case SQL_IC_SENSITIVE:
    strcpy(buffer, "SQL_IC_SENSITIVE");
    break;
    case SQL_IC_MIXED:
    strcpy(buffer, "SQL_IC_MIXED");
    break;
    default:
    strcpy(buffer, "!!!");
  }
  LOGF("Value = '%s'", buffer);

  /* *** 22.SQL_SPECIAL_CHARACTERS (ODBC 2.0) !*/
  LOGF("4.22.SQL_SPECIAL_CHARACTERS (ODBC 2.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_SPECIAL_CHARACTERS, (PTR) & rgbInfoValueChar,
      sizeof(rgbInfoValueChar), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 4.22");
  LOGF("Value = '%s'", rgbInfoValueChar);

  /* *** 23.SQL_SUBQUERIES (ODBC 2.0) !*/
  LOGF("4.23.SQL_SUBQUERIES (ODBC 2.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_SUBQUERIES, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 4.23");
  strcpy(buffer, " ");
  if (rgbInfoValue & SQL_SQ_CORRELATED_SUBQUERIES) strcat(buffer,
      "SQL_SQ_CORRELATED_SUBQUERIES |");
  if (rgbInfoValue & SQL_SQ_COMPARISON) strcat(buffer, "SQL_SQ_COMPARISON |");
  if (rgbInfoValue & SQL_SQ_EXISTS) strcat(buffer, "SQL_SQ_EXISTS |");
  if (rgbInfoValue & SQL_SQ_IN) strcat(buffer, "SQL_SQ_IN |");
  if (rgbInfoValue & SQL_SQ_QUANTIFIED) strcat(buffer, "SQL_SQ_QUANTIFIED");
  LOGF("Value = '%s'", buffer);

  /* *** 24.SQL_UNION (ODBC 2.0) !*/
  LOGF("4.24.SQL_UNION (ODBC 2.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_UNION, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 4.24");
  strcpy(buffer, " ");
  if (rgbInfoValue & SQL_U_UNION) strcat(buffer, "SQL_U_UNION |");
  if (rgbInfoValue & SQL_U_UNION_ALL) strcat(buffer, "SQL_U_UNION_ALL");
  LOGF("Value = '%s'", buffer);
#endif
}
/*
 -------------------------------------------------------------------------
 | 5.PutSQLLimits:
 |
 |
 | Returns:
 |
 -------------------------------------------------------------------------
 */
void PutSQLLimits(SQLHENV henv, SQLHDBC hdbc, SQLHSTMT hstmt)
{
  RETCODE retcode;

  CHAR rgbInfoValueChar[MAX_RGB_VALUE];
  SQLINTEGER rgbInfoValue;
  /* ---------------------------------------------------------------------- */
  /* ***** 5.) SQL Limits */
  /* ---------------------------------------------------------------------- */
#if defined(GETINFO_ALL) || defined(GETINFO_OPT5)
  LOGF(" 5.) SQL Limits -->");

  /* *** 1.SQL_MAX_BINARY_LITERAL_LEN (ODBC 2.0) !*/
  LOGF("5.1.SQL_MAX_BINARY_LITERAL_LEN (ODBC 2.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_MAX_BINARY_LITERAL_LEN, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 5.1");
  LOGF("Value = '%d'", rgbInfoValue);

  /* *** 2.SQL_MAX_CHAR_LITERAL_LEN (ODBC 2.0) !*/
  LOGF("5.2.SQL_MAX_CHAR_LITERAL_LEN (ODBC 2.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_MAX_CHAR_LITERAL_LEN, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 5.2");
  LOGF("Value = '%d'", rgbInfoValue);

  /* *** 3.SQL_MAX_COLUMN_NAME_LEN !*/
  LOGF("5.3.SQL_MAX_COLUMN_NAME_LEN : ");
  retcode = SQLGetInfo(hdbc, SQL_MAX_COLUMN_NAME_LEN, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 5.3");
  LOGF("Value = '%d'", rgbInfoValue);

  /* *** 4.SQL_MAX_COLUMNS_IN_GROUP_BY (ODBC 2.0) !*/
  LOGF("5.4.SQL_MAX_COLUMNS_IN_GROUP_BY (ODBC 2.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_MAX_BINARY_LITERAL_LEN, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 5.4");
  LOGF("Value = '%d'", rgbInfoValue);

  /* *** 5.SQL_MAX_COLUMNS_IN_ORDER_BY (ODBC 2.0) !*/
  LOGF("5.5.SQL_MAX_COLUMNS_IN_ORDER_BY (ODBC 2.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_MAX_COLUMNS_IN_ORDER_BY, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 5.5");
  LOGF("Value = '%d'", rgbInfoValue);

  /* *** 6.SQL_MAX_COLUMNS_IN_INDEX (ODBC 2.0) !*/
  LOGF("5.6.SQL_MAX_COLUMNS_IN_INDEX (ODBC 2.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_MAX_COLUMNS_IN_INDEX, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 5.6");
  LOGF("Value = '%d'", rgbInfoValue);

  /* *** 7.SQL_MAX_COLUMNS_IN_SELECT (ODBC 2.0) !*/
  LOGF("5.7.SQL_MAX_COLUMNS_IN_SELECT (ODBC 2.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_MAX_COLUMNS_IN_SELECT, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 5.7");
  LOGF("Value = '%d'", rgbInfoValue);

  /* *** 8.SQL_MAX_COLUMNS_IN_TABLE (ODBC 2.0) !*/
  LOGF("5.8.SQL_MAX_COLUMNS_IN_TABLE (ODBC 2.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_MAX_COLUMNS_IN_TABLE, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 5.8");
  LOGF("Value = '%d'", rgbInfoValue);

  /* *** 9.SQL_MAX_CURSOR_NAME_LEN !*/
  LOGF("5.9.SQL_MAX_CURSOR_NAME_LEN : ");
  retcode = SQLGetInfo(hdbc, SQL_MAX_CURSOR_NAME_LEN, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 5.9");
  LOGF("Value = '%d'", rgbInfoValue);

  /* *** 10.SQL_MAX_INDEX_SIZE (OBDC 2.0) !*/
  LOGF("5.10.SQL_MAX_INDEX_SIZE (ODBC 2.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_MAX_INDEX_SIZE, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 5.10");
  LOGF("Value = '%d'", rgbInfoValue);

  /* *** 11.SQL_MAX_OWNER_NAME_LEN !*/
  LOGF("5.11.SQL_MAX_OWNER_NAME_LEN : ");
  retcode = SQLGetInfo(hdbc, SQL_MAX_OWNER_NAME_LEN, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 5.11");
  LOGF("Value = '%d'", rgbInfoValue);

  /* *** 12.SQL_MAX_PROCEDURE_NAME_LEN */
  LOGF("5.12.SQL_MAX_PROCEDURE_NAME_LEN : ");
  retcode = SQLGetInfo(hdbc, SQL_MAX_PROCEDURE_NAME_LEN, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 5.12");
  LOGF("Value = '%d'", rgbInfoValue);

  /* *** 13.SQL_MAX_QUALIFIER_NAME_LEN !*/
  LOGF("5.13.SQL_MAX_QUALIFIER_NAME_LEN : ");
  retcode = SQLGetInfo(hdbc, SQL_MAX_QUALIFIER_NAME_LEN, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 5.13");
  LOGF("Value = '%d'", rgbInfoValue);

  /* *** 14.SQL_MAX_ROW_SIZE (ODBC 2.0) !*/
  LOGF("5.14.SQL_MAX_ROW_SIZE (ODBC 2.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_MAX_ROW_SIZE, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 5.14");
  LOGF("Value = '%d'", rgbInfoValue);

  /* *** 15.SQL_MAX_ROW_SIZE_INCLUDES_LONG (ODBC 2.0) !*/
  LOGF("5.15.SQL_MAX_ROW_SIZE_INCLUDES_LONG (ODBC 2.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_MAX_ROW_SIZE_INCLUDES_LONG,
      (PTR) & rgbInfoValueChar, sizeof(rgbInfoValueChar), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 5.15");
  LOGF("Value = '%s'", rgbInfoValueChar);

  /* *** 16.SQL_MAX_STATEMENT_LEN (OBDC 2.0) !*/
  LOGF("5.16.SQL_MAX_STATEMENT_LEN (ODBC 2.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_MAX_STATEMENT_LEN, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 5.16");
  LOGF("Value = '%d'", rgbInfoValue);

  /* *** 17.SQL_MAX_TABLE_NAME_LEN !*/
  LOGF("5.17.SQL_MAX_TABLE_NAME_LEN : ");
  retcode = SQLGetInfo(hdbc, SQL_MAX_TABLE_NAME_LEN, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 5.17");
  LOGF("Value = '%d'", rgbInfoValue);

  /* *** 18.SQL_MAX_TABLES_IN_SELECT (ODBC 2.0) !*/
  LOGF("5.18.SQL_MAX_TABLES_IN_SELECT (ODBC 2.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_MAX_TABLES_IN_SELECT, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 5.18");
  LOGF("Value = '%d'", rgbInfoValue);

  /* *** 19.SQL_MAX_USER_NAME_LEN                 (ODBC 2.0) */
  LOGF("5.19.SQL_MAX_USER_NAME_LEN (ODBC 2.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_MAX_USER_NAME_LEN, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 5.19");
  LOGF("Value = '%d'", rgbInfoValue);
#endif
}
/*
 -------------------------------------------------------------------------
 | 6.PutScalarFunctionInformation
 |
 |
 | Returns:
 |
 -------------------------------------------------------------------------
 */
void PutScalarFunctionInformation(SQLHENV henv, SQLHDBC hdbc, SQLHSTMT hstmt)
{
  RETCODE retcode;
  CHAR buffer[MAX_NAME_LEN * 10];
  CHAR bufxx[MAX_NAME_LEN * 10];

  SQLINTEGER rgbInfoValue;
  /* ---------------------------------------------------------------------- */
  /* ***** 6.) Scalar Function Information */
  /* ---------------------------------------------------------------------- */
#if defined(GETINFO_ALL) || defined(GETINFO_OPT6)
  LOGF(" 6.) Scalar Function Information -->");

  /* *** 1.SQL_CONVERT_FUNCTIONS !*/
  LOGF("6.1.SQL_CONVERT_FUNCTIONS : ");
  retcode = SQLGetInfo(hdbc, SQL_CONVERT_FUNCTIONS, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 6.1");
  strcpy(buffer, " ");
  if (rgbInfoValue & SQL_FN_CVT_CONVERT) strcat(buffer,
      " SQL_FN_CVT_CONVERT ");
  LOGF("Value = '%s'", buffer);

  /* *** 2.SQL_NUMERIC_FUNCTIONS !*/
  LOGF("6.2.SQL_NUMERIC_FUNCTIONS : ");
  retcode = SQLGetInfo(hdbc, SQL_NUMERIC_FUNCTIONS, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 6.2");
  strcpy(buffer, " ");
  if (rgbInfoValue & SQL_FN_NUM_ABS) strcat(buffer, "SQL_FN_NUM_ABS |");
  if (rgbInfoValue & SQL_FN_NUM_ACOS) strcat(buffer, " SQL_FN_NUM_ACOS |");
  if (rgbInfoValue & SQL_FN_NUM_ASIN) strcat(buffer, " SQL_FN_NUM_ASIN |");
  if (rgbInfoValue & SQL_FN_NUM_ATAN) strcat(buffer, " SQL_FN_NUM_ATAN |");
  if (rgbInfoValue & SQL_FN_NUM_ATAN2) strcat(buffer, " SQL_FN_NUM_ATAN2 |");
  if (rgbInfoValue & SQL_FN_NUM_CEILING) strcat(buffer,
      " SQL_FN_NUM_CEILING |");
  if (rgbInfoValue & SQL_FN_NUM_COS) strcat(buffer, " SQL_FN_NUM_COS |");
  if (rgbInfoValue & SQL_FN_NUM_COT) strcat(buffer, " SQL_FN_NUM_COT |");
  if (rgbInfoValue & SQL_FN_NUM_DEGREES) strcat(buffer,
      " SQL_FN_NUM_DEGREES |");
  if (rgbInfoValue & SQL_FN_NUM_EXP) strcat(buffer, " SQL_FN_NUM_EXP |");
  if (rgbInfoValue & SQL_FN_NUM_FLOOR) strcat(buffer, " SQL_FN_NUM_FLOOR |");
  if (rgbInfoValue & SQL_FN_NUM_LOG) strcat(buffer, " SQL_FN_NUM_LOG |");
  if (rgbInfoValue & SQL_FN_NUM_LOG10) strcat(buffer, " SQL_FN_NUM_LOG10 |");
  if (rgbInfoValue & SQL_FN_NUM_MOD) strcat(buffer, " SQL_FN_NUM_MOD |");
  if (rgbInfoValue & SQL_FN_NUM_PI) strcat(buffer, " SQL_FN_NUM_PI |");
  if (rgbInfoValue & SQL_FN_NUM_POWER) strcat(buffer, " SQL_FN_NUM_POWER |");
  if (rgbInfoValue & SQL_FN_NUM_RADIANS) strcat(buffer,
      " SQL_FN_NUM_RADIANS |");
  if (rgbInfoValue & SQL_FN_NUM_RAND) strcat(buffer, " SQL_FN_NUM_RAND |");
  if (rgbInfoValue & SQL_FN_NUM_ROUND) strcat(buffer, " SQL_FN_NUM_ROUND |");
  if (rgbInfoValue & SQL_FN_NUM_SIGN) strcat(buffer, " SQL_FN_NUM_SIGN |");
  if (rgbInfoValue & SQL_FN_NUM_SQRT) strcat(buffer, " SQL_FN_NUM_SQRT |");
  if (rgbInfoValue & SQL_FN_NUM_TAN) strcat(buffer, " SQL_FN_NUM_TAN |");
  if (rgbInfoValue & SQL_FN_NUM_TRUNCATE) strcat(buffer,
      " SQL_FN_NUM_TRUNCATE");
  sprintf(bufxx, "\tValue = ");
  LOG(bufxx);

  /* *** 3.SQL_STRING_FUNCTIONS !*/
  LOGF("6.3.SQL_STRING_FUNCTIONS : ");
  retcode = SQLGetInfo(hdbc, SQL_STRING_FUNCTIONS, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 6.3");
  strcpy(buffer, " ");
  if (rgbInfoValue & SQL_FN_STR_ASCII) strcat(buffer, " SQL_FN_STR_ASCII |");
  if (rgbInfoValue & SQL_FN_STR_CHAR) strcat(buffer, " SQL_FN_STR_CHAR |");
  if (rgbInfoValue & SQL_FN_STR_CONCAT) strcat(buffer,
      " SQL_FN_STR_CONCAT |");
  if (rgbInfoValue & SQL_FN_STR_DIFFERENCE) strcat(buffer,
      " SQL_FN_STR_DIFFERENCE |");
  if (rgbInfoValue & SQL_FN_STR_INSERT) strcat(buffer,
      " SQL_FN_STR_INSERT |");
  if (rgbInfoValue & SQL_FN_STR_LCASE) strcat(buffer, " SQL_FN_STR_LCASE |");
  if (rgbInfoValue & SQL_FN_STR_LEFT) strcat(buffer, " SQL_FN_STR_LEFT |");
  if (rgbInfoValue & SQL_FN_STR_LENGTH) strcat(buffer,
      " SQL_FN_STR_LENGTH |");
  if (rgbInfoValue & SQL_FN_STR_LOCATE) strcat(buffer,
      " SQL_FN_STR_LOCATE |");
  if (rgbInfoValue & SQL_FN_STR_LOCATE_2) strcat(buffer,
      " SQL_FN_STR_LOCATE_2 |");
  if (rgbInfoValue & SQL_FN_STR_LTRIM) strcat(buffer, " SQL_FN_STR_LTRIM |");
  if (rgbInfoValue & SQL_FN_STR_REPEAT) strcat(buffer,
      " SQL_FN_STR_REPEAT |");
  if (rgbInfoValue & SQL_FN_STR_RIGHT) strcat(buffer, " SQL_FN_STR_RIGHT |");
  if (rgbInfoValue & SQL_FN_STR_RTRIM) strcat(buffer, " SQL_FN_STR_RTRIM |");
  if (rgbInfoValue & SQL_FN_STR_SOUNDEX) strcat(buffer,
      " SQL_FN_STR_SOUNDEX |");
  if (rgbInfoValue & SQL_FN_STR_SPACE) strcat(buffer, " SQL_FN_STR_SPACE |");
  if (rgbInfoValue & SQL_FN_STR_SUBSTRING) strcat(buffer,
      " SQL_FN_STR_SUBSTRING |");
  if (rgbInfoValue & SQL_FN_STR_UCASE) strcat(buffer, " SQL_FN_STR_UCASE");
  LOGF("Value = '%s'", buffer);

  /* *** 4.SQL_SYSTEM_FUNCTIONS !*/
  LOGF("6.4.SQL_SYSTEM_FUNCTIONS : ");
  retcode = SQLGetInfo(hdbc, SQL_SYSTEM_FUNCTIONS, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 6.4");
  strcpy(buffer, " ");
  if (rgbInfoValue & SQL_FN_SYS_DBNAME) strcat(buffer,
      " SQL_FN_SYS_DBNAME |");
  if (rgbInfoValue & SQL_FN_SYS_IFNULL) strcat(buffer,
      " SQL_FN_SYS_IFNULL |");
  if (rgbInfoValue & SQL_FN_SYS_USERNAME) strcat(buffer,
      " SQL_FN_SYS_USERNAME ");
  LOGF("Value = '%s'", buffer);

  /* *** 5.SQL_TIMEDATE_ADD_INTERVALS (ODBC 2.0) !*/
  LOGF("6.5.SQL_TIMEDATE_ADD_INTERVALS (ODBC 2.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_TIMEDATE_ADD_INTERVALS, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 6.5");
  strcpy(buffer, " ");
  if (rgbInfoValue & SQL_FN_TSI_FRAC_SECOND) strcat(buffer,
      " SQL_FN_TSI_FRAC_SECOND |");
  if (rgbInfoValue & SQL_FN_TSI_SECOND) strcat(buffer,
      " SQL_FN_TSI_SECOND |");
  if (rgbInfoValue & SQL_FN_TSI_MINUTE) strcat(buffer,
      " SQL_FN_TSI_MINUTE |");
  if (rgbInfoValue & SQL_FN_TSI_HOUR) strcat(buffer, " SQL_FN_TSI_HOUR |");
  if (rgbInfoValue & SQL_FN_TSI_DAY) strcat(buffer, " SQL_FN_TSI_DAY |");
  if (rgbInfoValue & SQL_FN_TSI_WEEK) strcat(buffer, " SQL_FN_TSI_WEEK |");
  if (rgbInfoValue & SQL_FN_TSI_MONTH) strcat(buffer, " SQL_FN_TSI_MONTH |");
  if (rgbInfoValue & SQL_FN_TSI_QUARTER) strcat(buffer,
      " SQL_FN_TSI_QUALIFIER |");
  if (rgbInfoValue & SQL_FN_TSI_YEAR) strcat(buffer, " SQL_FN_TSI_YEAR ");
  LOGF("Value = '%s'", buffer);

  /* *** 6.SQL_TIMEDATE_DIFF_INTERVALS (ODBC 2.0) !*/
  LOGF("6.6.SQL_TIMEDATE_DIFF_INTERVALS (ODBC 2.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_TIMEDATE_DIFF_INTERVALS, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 6.6");
  strcpy(buffer, " ");
  if (rgbInfoValue & SQL_FN_TSI_FRAC_SECOND) strcat(buffer,
      " SQL_FN_TSI_FRAC_SECOND |");
  if (rgbInfoValue & SQL_FN_TSI_SECOND) strcat(buffer,
      " SQL_FN_TSI_SECOND |");
  if (rgbInfoValue & SQL_FN_TSI_MINUTE) strcat(buffer,
      " SQL_FN_TSI_MINUTE |");
  if (rgbInfoValue & SQL_FN_TSI_HOUR) strcat(buffer, " SQL_FN_TSI_HOUR |");
  if (rgbInfoValue & SQL_FN_TSI_DAY) strcat(buffer, " SQL_FN_TSI_DAY |");
  if (rgbInfoValue & SQL_FN_TSI_WEEK) strcat(buffer, " SQL_FN_TSI_WEEK |");
  if (rgbInfoValue & SQL_FN_TSI_MONTH) strcat(buffer, " SQL_FN_TSI_MONTH |");
  if (rgbInfoValue & SQL_FN_TSI_QUARTER) strcat(buffer,
      " SQL_FN_TSI_QUALIFIER |");
  if (rgbInfoValue & SQL_FN_TSI_YEAR) strcat(buffer, " SQL_FN_TSI_YEAR ");

  LOGF("Value = '%s'", buffer);

  /* *** 7.SQL_TIMEDATE_FUNCTIONS !*/
  LOGF("6.7.SQL_TIMEDATE_FUNCTIONS : ");
  retcode = SQLGetInfo(hdbc, SQL_TIMEDATE_FUNCTIONS, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 6.7");
  strcpy(buffer, " ");
  if (rgbInfoValue & SQL_FN_TD_CURDATE) strcat(buffer,
      " SQL_FN_TD_CURDATE |");
  if (rgbInfoValue & SQL_FN_TD_CURTIME) strcat(buffer,
      " SQL_FN_TD_CURTIME |");
  if (rgbInfoValue & SQL_FN_TD_DAYNAME) strcat(buffer,
      " SQL_FN_TD_DAYNAME |");
  if (rgbInfoValue & SQL_FN_TD_DAYOFMONTH) strcat(buffer,
      " SQL_FN_TD_DAYOFMONTH |");
  if (rgbInfoValue & SQL_FN_TD_DAYOFWEEK) strcat(buffer,
      " SQL_FN_TD_DAYOFWEEK |");
  if (rgbInfoValue & SQL_FN_TD_DAYOFYEAR) strcat(buffer,
      " SQL_FN_TD_DAYOFYEAR |");
  if (rgbInfoValue & SQL_FN_TD_HOUR) strcat(buffer, " SQL_FN_TD_HOUR |");
  if (rgbInfoValue & SQL_FN_TD_MINUTE) strcat(buffer, " SQL_FN_TD_MINUTE |");
  if (rgbInfoValue & SQL_FN_TD_MONTH) strcat(buffer, " SQL_FN_TD_MONTH |");
  if (rgbInfoValue & SQL_FN_TD_MONTHNAME) strcat(buffer,
      " SQL_FN_TD_MONTHNAME |");
  if (rgbInfoValue & SQL_FN_TD_NOW) strcat(buffer, " SQL_FN_TD_NOW |");
  if (rgbInfoValue & SQL_FN_TD_QUARTER) strcat(buffer,
      " SQL_FN_TD_QUARTER |");
  if (rgbInfoValue & SQL_FN_TD_SECOND) strcat(buffer, " SQL_FN_TD_SECOND |");
  if (rgbInfoValue & SQL_FN_TD_TIMESTAMPADD) strcat(buffer,
      " SQL_FN_TD_TIMESTAMPADD |");
  if (rgbInfoValue & SQL_FN_TD_TIMESTAMPDIFF) strcat(buffer,
      " SQL_FN_TD_TIMESTAMPDIFF |");
  if (rgbInfoValue & SQL_FN_TD_WEEK) strcat(buffer, " SQL_FN_TD_WEEK |");
  if (rgbInfoValue & SQL_FN_TD_YEAR) strcat(buffer, " SQL_FN_TD_YEAR");
  sprintf(bufxx, "\tValue = ");
  LOG(bufxx);
#endif
}
/*
 -------------------------------------------------------------------------
 | 7.PutConversionInformation
 |
 |
 | Returns:
 |
 -------------------------------------------------------------------------
 */
RETCODE PutConversionInformation(SQLHENV henv, SQLHDBC hdbc, SQLHSTMT hstmt)
{
  RETCODE retcode = SQL_SUCCESS;
  CHAR buffer[MAX_NAME_LEN * 10];

  SQLINTEGER rgbInfoValue;
  /* ---------------------------------------------------------------------- */
  /* ***** 7.Conversion Information */
  /* ---------------------------------------------------------------------- */
#if defined(GETINFO_ALL) || defined(GETINFO_OPT7)
  LOGF(" 7.) Conversion Information -->");

  /* *** 1.SQL_CONVERT_BIGINT */
  LOGF("7.1.SQL_COVERT_BIGINT : ");
  retcode = SQLGetInfo(hdbc, SQL_CONVERT_BIGINT, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 7.1");

  CheckConversion(rgbInfoValue, buffer);
  LOGF("Value = '%s'", buffer);

  /* *** 2.SQL_CONVERT_BINARY */
  LOGF("7.2.SQL_COVERT_BINARY : ");
  retcode = SQLGetInfo(hdbc, SQL_CONVERT_BINARY, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 7.2");

  CheckConversion(rgbInfoValue, buffer);
  LOGF("Value = '%s'", buffer);

  /* *** 3.SQL_CONVERT_BIT  */
  LOGF("7.3.SQL_COVERT_BIT : ");
  retcode = SQLGetInfo(hdbc, SQL_CONVERT_BIT, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 7.3");

  CheckConversion(rgbInfoValue, buffer);
  LOGF("Value = '%s'", buffer);

  /* *** 4.SQL_CONVERT_CHAR */
  LOGF("7.4.SQL_COVERT_CHAR : ");
  retcode = SQLGetInfo(hdbc, SQL_CONVERT_CHAR, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 7.4");

  CheckConversion(rgbInfoValue, buffer);
  LOGF("Value = '%s'", buffer);

  /* *** 5.SQL_CONVERT_DATE */
  LOGF("7.5.SQL_COVERT_DATE : ");
  retcode = SQLGetInfo(hdbc, SQL_CONVERT_DATE, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 7.5");

  CheckConversion(rgbInfoValue, buffer);
  LOGF("Value = '%s'", buffer);

  /* *** 6.SQL_CONVERT_DECIMAL */
  LOGF("7.6.SQL_COVERT_DECIMAL : ");
  retcode = SQLGetInfo(hdbc, SQL_CONVERT_DECIMAL, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 7.6");

  CheckConversion(rgbInfoValue, buffer);
  LOGF("Value = '%s'", buffer);

  /* *** 7.SQL_CONVERT_DOUBLE */
  LOGF("7.7.SQL_COVERT_DOUBLE : ");
  retcode = SQLGetInfo(hdbc, SQL_CONVERT_DOUBLE, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 7.7");

  CheckConversion(rgbInfoValue, buffer);
  LOGF("Value = '%s'", buffer);

  /* *** 8.SQL_CONVERT_FLOAT */
  LOGF("7.8.SQL_COVERT_FLOAT : ");
  retcode = SQLGetInfo(hdbc, SQL_CONVERT_FLOAT, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 7.8");

  CheckConversion(rgbInfoValue, buffer);
  LOGF("Value = '%s'", buffer);

  /* *** 9.SQL_CONVERT_INTEGER */
  LOGF("7.9.SQL_COVERT_INTEGER : ");
  retcode = SQLGetInfo(hdbc, SQL_CONVERT_INTEGER, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 7.9");

  CheckConversion(rgbInfoValue, buffer);
  LOGF("Value = '%s'", buffer);

  /* *** 10.SQL_CONVERT_LONGVARBINARY */
  LOGF("7.10.SQL_COVERT_LONGVARBINARY : ");
  retcode = SQLGetInfo(hdbc, SQL_CONVERT_LONGVARBINARY, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 7.10");

  CheckConversion(rgbInfoValue, buffer);
  LOGF("Value = '%s'", buffer);

  /* *** 11.SQL_CONVERT_LONGVARCHAR */
  LOGF("7.11.SQL_COVERT_LONGVARCHAR : ");
  retcode = SQLGetInfo(hdbc, SQL_CONVERT_LONGVARCHAR, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 7.11");

  CheckConversion(rgbInfoValue, buffer);
  LOGF("Value = '%s'", buffer);

  /* *** 12.SQL_CONVERT_NUMERIC */
  LOGF("7.12.SQL_COVERT_NUMERIC : ");
  retcode = SQLGetInfo(hdbc, SQL_CONVERT_NUMERIC, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 7.12");

  CheckConversion(rgbInfoValue, buffer);
  LOGF("Value = '%s'", buffer);

  /* *** 13.SQL_CONVERT_REAL */
  LOGF("7.13.SQL_COVERT_REAL : ");
  retcode = SQLGetInfo(hdbc, SQL_CONVERT_REAL, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 7.13");

  CheckConversion(rgbInfoValue, buffer);
  LOGF("Value = '%s'", buffer);

  /* *** 14.SQL_CONVERT_SMALLINT */
  LOGF("7.14.SQL_COVERT_SMALLINT : ");
  retcode = SQLGetInfo(hdbc, SQL_CONVERT_SMALLINT, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 7.14");

  CheckConversion(rgbInfoValue, buffer);
  LOGF("Value = '%s'", buffer);

  /* *** 15.SQL_CONVERT_TIME */
  LOGF("7.15.SQL_COVERT_TIME : ");
  retcode = SQLGetInfo(hdbc, SQL_CONVERT_TIME, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 7.15");

  CheckConversion(rgbInfoValue, buffer);
  LOGF("Value = '%s'", buffer);

  /* *** 16.SQL_CONVERT_TIMESTAMP */
  LOGF("7.16.SQL_COVERT_TIMESTAMP : ");
  retcode = SQLGetInfo(hdbc, SQL_CONVERT_TIMESTAMP, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 7.16");

  CheckConversion(rgbInfoValue, buffer);
  LOGF("Value = '%s'", buffer);

  /* *** 17.SQL_CONVERT_TINYINT */
  LOGF("7.17.SQL_COVERT_TINYINT : ");
  retcode = SQLGetInfo(hdbc, SQL_CONVERT_TINYINT, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 7.17");

  CheckConversion(rgbInfoValue, buffer);
  LOGF("Value = '%s'", buffer);

  /* *** 18.SQL_CONVERT_VARBINARY */
  LOGF("7.18.SQL_COVERT_VARBINARY : ");
  retcode = SQLGetInfo(hdbc, SQL_CONVERT_VARBINARY, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 7.18");

  CheckConversion(rgbInfoValue, buffer);
  LOGF("Value = '%s'", buffer);

  /* *** 19.SQL_CONVERT_VARCHAR */
  LOGF("7.19.SQL_COVERT_VARCHAR : ");
  retcode = SQLGetInfo(hdbc, SQL_CONVERT_VARCHAR, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 7.19");

  CheckConversion(rgbInfoValue, buffer);
  LOGF("Value = '%s'", buffer);

#endif
  return retcode;
}
/*
 -------------------------------------------------------------------------
 | 8.PutODBC30Information
 |
 |
 | Returns:
 |
 -------------------------------------------------------------------------
 */
void PutODBC30Information(SQLHENV henv, SQLHDBC hdbc, SQLHSTMT hstmt)
{
  RETCODE retcode;
  CHAR buffer[MAX_NAME_LEN * 20];
  CHAR bufxx[MAX_NAME_LEN * 10];

  SQLINTEGER rgbInfoValue;
  CHAR rgbInfoValueChar[MAX_RGB_VALUE * 10];

#ifdef WIN16
  SQLINTEGER FAR * rgbInfoValueHandle;
#endif
#ifdef WIN32
  ULONG FAR * rgbInfoValueHandle;
#endif
#ifndef WIN32
  SQLLEN *rgbInfoValueHandle;
#endif
  SQLSMALLINT cbInfoValueMax;

  SQLHDESC hdesc;

#ifdef WIN16
  rgbInfoValueHandle = (SQLINTEGER *) malloc(sizeof(SQLINTEGER));
#endif
#ifdef WIN32
  rgbInfoValueHandle = (ULONG FAR *) malloc(sizeof(ULONG));
#endif

  /* ---------------------------------------------------------------------- */
  /* ***** 8.ODBC 3.0 Information */
  /* ---------------------------------------------------------------------- */
#if defined(GETINFO_ALL) || defined(GETINFO_OPT8)
  LOGF(" 8.) ODBC 3.0 Information -->");

  /* *** 1.   SQL_ACTIVE_ENVIRONMENTS (ODBC 3.0) !*/
  LOGF("8.1.SQL_SUBQUERIES (ODBC 3.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_ACTIVE_ENVIRONMENTS, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 8.1");
  LOGF("Value = '%d'", rgbInfoValue);

  /* *** 2.SQL_AGGREGATE_FUNCTIONS !*/
  LOGF("8.2.SQL_AGGREGATE_FUNCTIONS (ODBC 3.0): ");
  retcode = SQLGetInfo(hdbc, SQL_AGGREGATE_FUNCTIONS, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 8.2");
  /*strcpy(buffer, " ");
  if (rgbInfoValue & SQL_AF_ALL) strcat(buffer, " SQL_AF_ALL |");
  if (rgbInfoValue & SQL_AF_AVG) strcat(buffer, " SQL_AF_AVG |");
  if (rgbInfoValue & SQL_AF_COUNT) strcat(buffer, " SQL_AF_COUNT |");
  if (rgbInfoValue & SQL_AF_DISTINCT) strcat(buffer, " SQL_AF_DISTINCT |");
  if (rgbInfoValue & SQL_AF_MAX) strcat(buffer, " SQL_AF_MAX |");
  if (rgbInfoValue & SQL_AF_MIN) strcat(buffer, " SQL_AF_MIN |");
  if (rgbInfoValue & SQL_AF_SUM) strcat(buffer, " SQL_AF_SUM |");
  sprintf(bufxx, "\tValue = ");
  LOG(bufxx);*/

  /* *** 3.SQL_ALTER_DOMAIN (ODBC 3.0) !*/
  LOGF("8.3.SQL_ALTER_DOMAIN (ODBC 3.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_ALTER_DOMAIN, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 8.3");
 /* strcpy(buffer, " ");
  if (rgbInfoValue & SQL_AD_ADD_DOMAIN_CONSTRAINT) strcat(buffer,
      "SQL_AD_ADD_DOMAIN_CONSTRAINT |");
  if (rgbInfoValue & SQL_AD_ADD_DOMAIN_DEFAULT) strcat(buffer,
      "SQL_AD_ADD_DOMAIN_DEFAULT");
  if (rgbInfoValue & SQL_AD_DROP_DOMAIN_CONSTRAINT) strcat(buffer,
      "SQL_AD_DROP_DOMIAN_CONSTRAINT |");
  if (rgbInfoValue & SQL_AD_DROP_DOMAIN_DEFAULT) strcat(buffer,
      "SQL_AD_DROP_DOMAIN_DEFAULT");
  if (rgbInfoValue & SQL_AD_CONSTRAINT_NAME_DEFINITION) strcat(buffer,
      "SQL_AD_CONSTRAINT_NAME_DEFINITION");
  LOGF("Value = '%s'", buffer);*/

  /* *** 4.SQL_ASYNC_MODE   (ODBC 3.0) */
  LOGF("8.4.SQL_ASYNC_MODE (ODBC 3.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_ASYNC_MODE, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 8.4");
 /* switch (rgbInfoValue) {
    case SQL_AM_CONNECTION:
      strcpy(buffer, "SQL_AM_CONNECTION");
      break;
    case SQL_AM_STATEMENT:
      strcpy(buffer, "SQL_AM_STATEMENT");
      break;
    case SQL_AM_NONE:
      strcpy(buffer, "SQL_AM_NONE");
      break;
    default:
      strcpy(buffer, "???");
  }
  LOGF("Value = '%s'", buffer);*/

  /* *** 5.SQL_BATCH_ROW_COUNT   (ODBC 3.0) */
  LOGF("8.5.SQL_BATCH_ROW_COUNT (ODBC 3.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_BATCH_ROW_COUNT, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 8.5");
  switch (rgbInfoValue) {
    case SQL_BRC_ROLLED_UP:
      strcpy(buffer, "SQL_BRC_ROLLED_UP");
      break;
    case SQL_BRC_PROCEDURES:
      strcpy(buffer, "SQL_BRC_PROCEDURES");
      break;
    case SQL_BRC_EXPLICIT:
      strcpy(buffer, "SQL_BRC_EXPLICT");
      break;
    default:
      strcpy(buffer, "???");
  }
  LOGF("Value = '%s'", buffer);

  /* *** 6.SQL_BATCH_SUPPORT   (ODBC 3.0) */
  LOGF("8.6.SQL_BATCH_SUPPORT (ODBC 3.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_BATCH_SUPPORT, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 8.6");
 /* switch (rgbInfoValue) {
    case SQL_BS_SELECT_EXPLICIT:
      strcpy(buffer, "SQL_BS_SELECT_EXPLICIT");
      break;
    case SQL_BS_ROW_COUNT_EXPLICIT:
      strcpy(buffer, "SQL_BS_ROW_COUNT_EXPLICIT");
      break;
    case SQL_BS_SELECT_PROC:
      strcpy(buffer, "SQL_BS_SELECT_PROC");
      break;
    case SQL_BS_ROW_COUNT_PROC:
      strcpy(buffer, "SQL_BS_ROW_COUNT_PROC");
      break;
    default:
      strcpy(buffer, "???");
  }
  LOGF("Value = '%s'", buffer);*/

  /* *** 7.SQL_CATALOG_NAME !*/
  LOGF("8.7.SQL_CATALOG_NAME (ODBC 3.0): ");
  retcode = SQLGetInfo(hdbc, SQL_CATALOG_NAME, (PTR) & rgbInfoValueChar,
      sizeof(rgbInfoValueChar), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 8.7");
  LOGF("Value = '%s'", rgbInfoValueChar);

  /* *** 8.SQL_COLLATION_SEQ */
  LOGF("8.8.SQL_COLLATION_SEQ (ODBC 3.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_COLLATION_SEQ, (PTR) & rgbInfoValueChar,
      sizeof(rgbInfoValueChar), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 8.8");
  LOGF("Value = '%s'", rgbInfoValueChar);

  /* *** 9.SQL_CREATE_ASSERTION   (ODBC 3.0) */
  LOGF("8.9.SQL_CREATE_ASSERTION (ODBC 3.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_CREATE_ASSERTION, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 8.9");
  /*strcpy(buffer, " ");
  if (rgbInfoValue & SQL_CA_CONSTRAINT_INITIALLY_DEFERRED) strcat(buffer,
      "SQL_CA_CONSTRAINT_INITIALLY_DEFERRED |");
  if (rgbInfoValue & SQL_CA_CONSTRAINT_INITIALLY_IMMEDIATE) strcat(buffer,
      "SQL_CA_CONSTRAINT_INITIALLY_IMMEDIATE |");
  if (rgbInfoValue & SQL_CA_CONSTRAINT_DEFERRABLE) strcat(buffer,
      "SQL_CA_CONSTRAINT_DEFERRABLE |");
  if (rgbInfoValue & SQL_CA_CONSTRAINT_NON_DEFERRABLE) strcat(buffer,
      "SQL_CA_CONSTRAINT_NON_DEFERRABLE |");
  sprintf(bufxx, "\tValue = ");
  LOG(bufxx);*/

  /* *** 10.SQL_DROP_ASSERTION   (ODBC 3.0) */
  LOGF("8.10.SQL_DROP_ASSERTION (ODBC 3.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_DROP_ASSERTION, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 8.10");
  /*strcpy(buffer, " ");
  if (rgbInfoValue & SQL_DA_DROP_ASSERTION) strcat(buffer,
      "SQL_DA_DROP_ASSERTION |");
  sprintf(bufxx, "\tValue = ");
  LOG(bufxx);*/

  /* *** 11.SQL_CREATE_CHARACTER_SET   (ODBC 3.0) */
  LOGF("8.11.SQL_CREATE_CHARACTER_SET (ODBC 3.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_CREATE_CHARACTER_SET, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 8.11");
  /*strcpy(buffer, " ");
  if (rgbInfoValue & SQL_CCS_CREATE_CHARACTER_SET) strcat(buffer,
      "SQL_CCS_CREATE_CHARACTER_SET |");
  if (rgbInfoValue & SQL_CCS_COLLATE_CLAUSE) strcat(buffer,
      "SQL_CCS_COLLATE_CLAUSE |");
  if (rgbInfoValue & SQL_CCS_LIMITED_COLLATION) strcat(buffer,
      "SQL_CCS_LIMITED_COLLATION |");
  sprintf(bufxx, "\tValue = ");
  LOG(bufxx);*/

  /* *** 12.SQL_DROP_CHARACTER_SET   (ODBC 3.0) */
  LOGF("8.12.SQL_DROP_CHARACTER_SET (ODBC 3.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_DROP_CHARACTER_SET, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 8.12");
 /* strcpy(buffer, " ");
  if (rgbInfoValue & SQL_DCS_DROP_CHARACTER_SET) strcat(buffer,
      "SQL_DCS_DROP_CHARACTER_SET |");
  sprintf(bufxx, "\tValue = ");
  LOG(bufxx);*/

  /* *** 13.SQL_CREATE_COLLATION   (ODBC 3.0) */
  LOGF("8.13.SQL_CREATE_COLLATION (ODBC 3.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_CREATE_COLLATION, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 8.13");
 /* strcpy(buffer, " ");
  if (rgbInfoValue & SQL_CCOL_CREATE_COLLATION) strcat(buffer,
      "SQL_CCOL_CREATE_COLLATION |");
  sprintf(bufxx, "\tValue = ");
  LOG(bufxx);*/

  /* *** 14.SQL_DROP_COLLATION   (ODBC 3.0) */
  LOGF("8.14.SQL_DROP_COLLATION (ODBC 3.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_DROP_COLLATION, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 8.14");
  /*strcpy(buffer, " ");
  if (rgbInfoValue & SQL_DC_DROP_COLLATION) strcat(buffer,
      "SQL_DC_DROP_COLLATION |");
  sprintf(bufxx, "\tValue = ");
  LOG(bufxx);*/

  /* *** 15.SQL_CREATE_DOMAIN   (ODBC 3.0) */
  LOGF("8.15.SQL_CREATE_DOMAIN (ODBC 3.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_CREATE_DOMAIN, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 8.15");
  /*strcpy(buffer, " ");
  if (rgbInfoValue & SQL_CDO_CREATE_DOMAIN) strcat(buffer,
      "SQL_CDO_CREATE_DOMAIN |");
  if (rgbInfoValue & SQL_CDO_CONSTRAINT_NAME_DEFINITION) strcat(buffer,
      "SQL_CDO_CONSTRAINT_NAME_DEFINITION |");
  sprintf(bufxx, "\tValue = ");
  LOG(bufxx);*/

  /* *** 16.SQL_DROP_DOMAIN   (ODBC 3.0) */
  LOGF("8.16.SQL_DROP_DOMAIN (ODBC 3.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_DROP_DOMAIN, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 8.16");
  /*strcpy(buffer, " ");
  if (rgbInfoValue & SQL_DD_DROP_DOMAIN) strcat(buffer,
      "SQL_DD_DROP_DOMAIN |");
  if (rgbInfoValue & SQL_DD_CASCADE) strcat(buffer, "SQL_DD_CASCADE |");
  if (rgbInfoValue & SQL_DD_RESTRICT) strcat(buffer, "SQL_DD_RESTRICT |");
  sprintf(bufxx, "\tValue = ");
  LOG(bufxx);*/

  /* *** 17.SQL_CREATE_SCHEMA   (ODBC 3.0) */
  LOGF("8.17.SQL_CREATE_SCHEMA (ODBC 3.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_CREATE_SCHEMA, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 8.17");
  /*strcpy(buffer, " ");
  if (rgbInfoValue & SQL_CS_CREATE_SCHEMA) strcat(buffer,
      "SQL_CS_CREATE_SCHEMA |");
  if (rgbInfoValue & SQL_CS_AUTHORIZATION) strcat(buffer,
      "SQL_CS_AUTHORIZATION |");
  if (rgbInfoValue & SQL_CS_DEFAULT_CHARACTER_SET) strcat(buffer,
      "SQL_CS_DEFAULT_CHARACTER_SET |");
  sprintf(bufxx, "\tValue = ");
  LOG(bufxx);*/

  /* *** 18.SQL_DROP_SCHEMA   (ODBC 3.0) */
  LOGF("8.18.SQL_DROP_SCHEMA (ODBC 3.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_DROP_SCHEMA, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 8.18");
 /* strcpy(buffer, " ");
  if (rgbInfoValue & SQL_DS_DROP_SCHEMA) strcat(buffer,
      "SQL_DS_DROP_SCHEMA |");
  if (rgbInfoValue & SQL_DS_CASCADE) strcat(buffer, "SQL_DS_CASCADE |");
  if (rgbInfoValue & SQL_DS_RESTRICT) strcat(buffer, "SQL_DS_RESTRICT |");
  sprintf(bufxx, "\tValue = ");
  LOG(bufxx);*/

  /* *** 19.SQL_CREATE_TABLE   (ODBC 3.0) */
  LOGF("8.19.SQL_CREATE_TABLE (ODBC 3.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_CREATE_TABLE, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 8.19");
  /*strcpy(buffer, " ");
  if (rgbInfoValue & SQL_CT_CREATE_TABLE) strcat(buffer,
      "SQL_CT_CREATE_TABLE |");
  if (rgbInfoValue & SQL_CT_TABLE_CONSTRAINT) strcat(buffer,
      "SQL_CT_TABLE_CONSTRAINT |");
  if (rgbInfoValue & SQL_CT_CONSTRAINT_NAME_DEFINITION) strcat(buffer,
      "SQL_CT_CONSTRAINT_NAME_DEFINITION |");
  sprintf(bufxx, "\tValue = ");
  LOG(bufxx);*/

  /* *** 20.   SQL_DROP_TABLE   (ODBC 3.0) */
  LOGF("8.20.SQL_DROP_TABLE (ODBC 3.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_DROP_TABLE, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 8.20");
 /* strcpy(buffer, " ");
  if (rgbInfoValue & SQL_DT_DROP_TABLE) strcat(buffer, "SQL_DT_DROP_TABLE |");
  if (rgbInfoValue & SQL_DT_CASCADE) strcat(buffer, "SQL_DT_CASCADE |");
  if (rgbInfoValue & SQL_DT_RESTRICT) strcat(buffer, "SQL_DT_RESTRICT |");
  sprintf(bufxx, "\tValue = ");
  LOG(bufxx);*/

  /* *** 21.SQL_CREATE_TRANSLATION   (ODBC 3.0) */
  LOGF("8.21.SQL_CREATE_TRANSLATION (ODBC 3.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_CREATE_TRANSLATION, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 8.21");
  /*strcpy(buffer, " ");
  if (rgbInfoValue & SQL_CTR_CREATE_TRANSLATION) strcat(buffer,
      "SQL_CTR_CREATE_TRANSLATION |");
  sprintf(bufxx, "\tValue = ");
  LOG(bufxx);*/

  /* *** 22.SQL_DROP_TRANSLATION   (ODBC 3.0) */
  LOGF("8.22.SQL_DROP_TRANSLATION (ODBC 3.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_DROP_TRANSLATION, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 8.22");
 /* strcpy(buffer, " ");
  if (rgbInfoValue & SQL_DTR_DROP_TRANSLATION) strcat(buffer,
      "SQL_DTR_DROP_TRANSLATION |");
  sprintf(bufxx, "\tValue = ");
  LOG(bufxx);*/

  /* *** 23.SQL_CREATE_VIEW   (ODBC 3.0) */
  LOGF("8.23.SQL_CREATE_VIEW (ODBC 3.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_CREATE_VIEW, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 8.23");
 /* strcpy(buffer, " ");
  if (rgbInfoValue & SQL_CV_CREATE_VIEW) strcat(buffer,
      "SQL_CV_CREATE_VIEW |");
  if (rgbInfoValue & SQL_CV_CHECK_OPTION) strcat(buffer,
      "SQL_CV_CHECK_OPTION |");
  if (rgbInfoValue & SQL_CV_CASCADED) strcat(buffer, "SQL_CV_CASCADED |");
  if (rgbInfoValue & SQL_CV_LOCAL) strcat(buffer, "SQL_CV_LOCAL |");
  sprintf(bufxx, "\tValue = ");
  LOG(bufxx);*/

  /* *** 24.SQL_DROP_VIEW   (ODBC 3.0) */
  LOGF("8.24.SQL_DROP_VIEW (ODBC 3.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_DROP_VIEW, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 8.24");
  /*strcpy(buffer, " ");
  if (rgbInfoValue & SQL_DV_DROP_VIEW) strcat(buffer, "SQL_DV_DROP_VIEW |");
  if (rgbInfoValue & SQL_DV_CASCADE) strcat(buffer, "SQL_DV_CASCADE |");
  if (rgbInfoValue & SQL_DV_RESTRICT) strcat(buffer, "SQL_DV_RESTRICT |");
  sprintf(bufxx, "\tValue = ");
  LOG(bufxx);*/

  /* *** 25.SQL_CURSOR_SENSITIVITY   (ODBC 3.0) */
  LOGF("8.25.SQL_CURSOR_SENSITIVITY (ODBC 3.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_CURSOR_SENSITIVITY, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 8.25");
  /*switch (rgbInfoValue) {
    case SQL_INSENSITIVE:
      strcpy(buffer, "SQL_UNSENSITIVE");
      break;
    case SQL_UNSPECIFIED:
      strcpy(buffer, "SQL_UNSPECIFIED");
      break;
    case SQL_SENSITIVE:
      strcpy(buffer, "SQL_SENSITIVE");
      break;
    default:
      strcpy(buffer, "???");
  }
  LOGF("Value = '%s'", buffer);*/

  /* *** 26.SQL_DATETIME_LITERALS   (ODBC 3.0) */
  LOGF("8.26.SQL_DATETIME_LITERALS (ODBC 3.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_DATETIME_LITERALS, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 8.26");
 /* strcpy(buffer, " ");
  if (rgbInfoValue & SQL_DL_SQL92_DATE) strcat(buffer, "SQL_DL_SQL92_DATE |");
  if (rgbInfoValue & SQL_DL_SQL92_TIME) strcat(buffer, "SQL_DL_SQL92_TIME |");
  if (rgbInfoValue & SQL_DL_SQL92_TIMESTAMP) strcat(buffer,
      "SQL_DL_SQL92_TIMESTAMP |");
  if (rgbInfoValue & SQL_DL_SQL92_INTERVAL_YEAR) strcat(buffer,
      "SQL_DL_SQL92_INTERVAL_YEAR |");
  if (rgbInfoValue & SQL_DL_SQL92_INTERVAL_MONTH) strcat(buffer,
      "SQL_DL_SQL92_INTERVAL_MONTH |");
  if (rgbInfoValue & SQL_DL_SQL92_INTERVAL_DAY) strcat(buffer,
      "SQL_DL_SQL92_INTERVAL_DAY |");
  if (rgbInfoValue & SQL_DL_SQL92_INTERVAL_HOUR) strcat(buffer,
      "SQL_DL_SQL92_INTERVAL_HOUR |");
  if (rgbInfoValue & SQL_DL_SQL92_INTERVAL_MINUTE) strcat(buffer,
      "SQL_DL_SQL92_INTERVAL_MINUTE |");
  if (rgbInfoValue & SQL_DL_SQL92_INTERVAL_SECOND) strcat(buffer,
      "SQL_DL_SQL92_INTERVAL_SECOND |");
  if (rgbInfoValue & SQL_DL_SQL92_INTERVAL_YEAR_TO_MONTH) strcat(buffer,
      "SQL_DL_SQL92_INTERVAL_YEAR_TO_MONTH |");
  if (rgbInfoValue & SQL_DL_SQL92_INTERVAL_DAY_TO_HOUR) strcat(buffer,
      "SQL_DL_SQL92_INTERVAL_DAY_TO_HOUR |");
  if (rgbInfoValue & SQL_DL_SQL92_INTERVAL_DAY_TO_MINUTE) strcat(buffer,
      "SQL_DL_SQL92_INTERVAL_DAY_TO_MINUTE |");
  if (rgbInfoValue & SQL_DL_SQL92_INTERVAL_DAY_TO_SECOND) strcat(buffer,
      "SQL_DL_SQL92_INTERVAL_DAY_TO_SECOND |");
  if (rgbInfoValue & SQL_DL_SQL92_INTERVAL_HOUR_TO_MINUTE) strcat(buffer,
      "SQL_DL_SQL92_INTERVAL_HOUR_TO_MINUTE |");
  if (rgbInfoValue & SQL_DL_SQL92_INTERVAL_HOUR_TO_SECOND) strcat(buffer,
      "SQL_DL_SQL92_INTERVAL_HOUR_TO_SECOND |");
  if (rgbInfoValue & SQL_DL_SQL92_INTERVAL_MINUTE_TO_SECOND) strcat(buffer,
      "SQL_DL_SQL92_INTERVAL_HOUR_TO_MINUTE |");
  sprintf(bufxx, "\tValue = ");
  LOG(bufxx);*/

  /* *** 27.SQL_DDL_INDEX   (ODBC 3.0) */
  LOGF("8.27.SQL_DDL_INDEX (ODBC 3.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_DDL_INDEX, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 8.27");
  /*switch (rgbInfoValue) {
    case SQL_DI_CREATE_INDEX:
      strcpy(buffer, "SQL_DI_CREATE_INDEX");
      break;
    case SQL_DI_DROP_INDEX:
      strcpy(buffer, "SQL_DI_DROP_INDEX");
      break;
    default:
      strcpy(buffer, "???");
  }
  LOGF("Value = '%s'", buffer);*/

  /* *** 28.SQL_DESCRIBE_PARAMETER */
  LOGF("8.28.SQL_DESCRIBE_PARAMETER (ODBC 3.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_DESCRIBE_PARAMETER, (PTR) & rgbInfoValueChar,
      sizeof(rgbInfoValueChar), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 8.28");
 // LOGF("Value = '%s'", rgbInfoValueChar);

  /* *** 29.SQL_DM_VER */
  LOGF("8.29.SQL_DM_VER (ODBC 3.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_DM_VER, (PTR) & rgbInfoValueChar,
      sizeof(rgbInfoValueChar), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 8.29");
 // LOGF("Value = '%s'", rgbInfoValueChar);

  /* *** 30. SQL_DRIVER_HDESC !*/
  /*LOGF("8.30.SQL_DRIVER_HDESC : ");
  cbInfoValueMax = sizeof(SQLINTEGER);
#ifdef WIN32
  retcode = SQLGetStmtAttr(hstmt,SQL_ATTR_APP_PARAM_DESC, &hdesc, SQL_IS_INTEGER, NULL);
  DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_ERROR, retcode,"SQLGetStmtAttr");

  memcpy(rgbInfoValueHandle, &hdesc, sizeof(SQLHDESC));
  cbInfoValueMax= sizeof(ULONG);
#endif
  retcode = SQLGetInfo(hdbc, SQL_DRIVER_HDESC, (PTR)rgbInfoValueHandle,
      cbInfoValueMax, NULL);
  //if (retcode != SQL_SUCCESS) retcode--;
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 8.30");
  sprintf(buffer, "%p", *rgbInfoValueHandle);
  LOGF("Value = '%s'", buffer);*/

  /* *** 31.SQL_INDEX_KEYWORDS   (ODBC 3.0) */
  LOGF("8.31.SQL_INDEX_KEYWORDS (ODBC 3.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_INDEX_KEYWORDS, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 8.31");
 /* strcpy(buffer, " ");
  if (rgbInfoValue & SQL_IK_ASC) strcat(buffer, "SQL_IK_ASC |");
  if (rgbInfoValue & SQL_IK_DESC) strcat(buffer, "SQL_IK_DESC |");
  if (rgbInfoValue & SQL_IK_ALL) strcat(buffer, "SQL_IK_ALL |");
  sprintf(bufxx, "\tValue = ");
  LOG(bufxx);*/

  /* *** 32.SQL_INFO_SCHEMA_VIEWS   (ODBC 3.0) */
  LOGF("8.32.SQL_INFO_SCHEMA_VIEWS (ODBC 3.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_INFO_SCHEMA_VIEWS, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 8.32");
  /*strcpy(buffer, " ");
  if (rgbInfoValue & SQL_ISV_ASSERTIONS) strcat(buffer,
      "SQL_ISV_ASSERTIONS |");
  if (rgbInfoValue & SQL_ISV_CHARACTER_SETS) strcat(buffer,
      "SQL_ISV_CHARACTER_SETS |");
  if (rgbInfoValue & SQL_ISV_CHECK_CONSTRAINTS) strcat(buffer,
      "SQL_ISV_CHECK_CONSTRAINTS |");
  if (rgbInfoValue & SQL_ISV_COLLATIONS) strcat(buffer,
      "SQL_ISV_COLLATIONS |");
  if (rgbInfoValue & SQL_ISV_COLUMN_DOMAIN_USAGE) strcat(buffer,
      "SQL_ISV_COLUMN_DOMAIN_USAGE |");
  if (rgbInfoValue & SQL_ISV_COLUMN_PRIVILEGES) strcat(buffer,
      "SQL_ISV_COLUMN_PRIVILEGES |");
  if (rgbInfoValue & SQL_ISV_COLUMNS) strcat(buffer, "SQL_ISV_COLUMNS |");
  if (rgbInfoValue & SQL_ISV_CONSTRAINT_COLUMN_USAGE) strcat(buffer,
      "SQL_ISV_CONSTRAINT_COLUMN_USAGE |");
  if (rgbInfoValue & SQL_ISV_CONSTRAINT_TABLE_USAGE) strcat(buffer,
      "SQL_ISV_CONSTRAINT_TABLE_USAGE |");
  if (rgbInfoValue & SQL_ISV_DOMAIN_CONSTRAINTS) strcat(buffer,
      "SQL_ISV_DOMAIN_CONSTRAINTS |");
  if (rgbInfoValue & SQL_ISV_DOMAINS) strcat(buffer, "SQL_ISV_DOMAINS |");
  if (rgbInfoValue & SQL_ISV_KEY_COLUMN_USAGE) strcat(buffer,
      "SQL_ISV_KEY_COLUMN_USAGE |");
  if (rgbInfoValue & SQL_ISV_REFERENTIAL_CONSTRAINTS) strcat(buffer,
      "SQL_ISV_REFERENTIAL_CONSTRAINTS |");
  if (rgbInfoValue & SQL_ISV_SCHEMATA) strcat(buffer, "SQL_ISV_SCHEMATA |");
  if (rgbInfoValue & SQL_ISV_SQL_LANGUAGES) strcat(buffer,
      "SQL_ISV_SQL_LANGUAGES |");
  if (rgbInfoValue & SQL_ISV_TABLE_CONSTRAINTS) strcat(buffer,
      "SQL_ISV_TABLE_CONSTRAINTS |");
  if (rgbInfoValue & SQL_ISV_TABLE_PRIVILEGES) strcat(buffer,
      "SQL_ISV_TABLE_PRIVILEGES |");
  if (rgbInfoValue & SQL_ISV_TABLES) strcat(buffer, "SQL_ISV_TABLES |");
  if (rgbInfoValue & SQL_ISV_TRANSLATIONS) strcat(buffer,
      "SQL_ISV_TRANSLATIONS |");
  if (rgbInfoValue & SQL_ISV_USAGE_PRIVILEGES) strcat(buffer,
      "SQL_ISV_USAGE_PRIVILEGES |");
  if (rgbInfoValue & SQL_ISV_VIEW_COLUMN_USAGE) strcat(buffer,
      "SQL_ISV_VIEW_COLUMN_USAGE |");
  if (rgbInfoValue & SQL_ISV_VIEW_TABLE_USAGE) strcat(buffer,
      "SQL_ISV_VIEW_TABLE_USAGE |");
  if (rgbInfoValue & SQL_ISV_VIEWS) strcat(buffer, "SQL_ISV_VIEWS |");
  sprintf(bufxx, "\tValue = ");
  LOG(bufxx);*/

  /* *** 33.SQL_INSERT_STATEMENT   (ODBC 3.0) */
  LOGF("8.33.SQL_INSERT_STATEMENT (ODBC 3.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_INSERT_STATEMENT, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 8.33");
  /*strcpy(buffer, " ");
  if (rgbInfoValue & SQL_IS_INSERT_LITERALS) strcat(buffer,
      "SQL_IS_INSERT_LITERALS |");
  if (rgbInfoValue & SQL_IS_INSERT_SEARCHED) strcat(buffer,
      "SQL_IS_INSERT_SEARCHED |");
  if (rgbInfoValue & SQL_IS_SELECT_INTO) strcat(buffer,
      "SQL_IS_SELECT_INTO |");
  sprintf(bufxx, "\tValue = ");
  LOG(bufxx);*/

  /* *** 34.SQL_MAX_ASYNC_CONCURRENT_STATEMENTS (ODBC 3.0) !*/
  LOGF("8.34.SQL_MAX_ASYNC_CONCURRENT_STATEMENTS (ODBC 3.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_MAX_ASYNC_CONCURRENT_STATEMENTS,
      (PTR) & rgbInfoValue, sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 8.34");
  //LOGF("Value = '%d'", rgbInfoValue);

  /* *** 35.SQL_MAX_IDENTIFIER_LEN (ODBC 3.0) !*/
  LOGF("8.35.SQL_MAX_IDENTIFIER_LEN (ODBC 3.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_MAX_IDENTIFIER_LEN, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 8.35");
 // LOGF("Value = '%d'", rgbInfoValue);

  /* *** 36.SQL_ODBC_INTERFACE_CONFORMANCE   (ODBC 3.0) */
  LOGF("8.36.SQL_ODBC_INTERFACE_CONFORMANCE (ODBC 3.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_ODBC_INTERFACE_CONFORMANCE,
      (PTR) & rgbInfoValue, sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 8.36");
 /* switch (rgbInfoValue) {
    case SQL_OIC_CORE:
      strcpy(buffer, "SQL_OIC_CORE");
      break;
    case SQL_OIC_LEVEL1:
      strcpy(buffer, "SQL_OIC_LEVEL1");
      break;
    case SQL_OIC_LEVEL2:
      strcpy(buffer, "SQL_OIC_LEVEL2");
      break;
    default:
      strcpy(buffer, "???");
  }*/
  LOGF("Value = '%s'", buffer);

  /* *** 37.SQL_OJ_CAPABILITIES   (ODBC 3.0) */
  LOGF("8.37.SQL_OJ_CAPABILITIES (ODBC 3.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_OJ_CAPABILITIES, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 8.37");
 /* strcpy(buffer, " ");
  if (rgbInfoValue & SQL_OJ_LEFT) strcat(buffer, "SQL_OJ_LEFT |");
  if (rgbInfoValue & SQL_OJ_RIGHT) strcat(buffer, "SQL_OJ_RIGHT |");
  if (rgbInfoValue & SQL_OJ_FULL) strcat(buffer, "SQL_OJ_FULL |");
  if (rgbInfoValue & SQL_OJ_NOT_ORDERED) strcat(buffer,
      "SQL_OJ_NOT_ORDERED |");
  sprintf(bufxx, "\tValue = ");
  LOG(bufxx);*/

  /* *** 38.SQL_PARAM_ARRAY_ROW_COUNTS  (ODBC 3.0) */
  LOGF("8.38.SQL_PARAM_ARRAY_ROW_COUNTS (ODBC 3.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_PARAM_ARRAY_ROW_COUNTS, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
      "SQLGetInfo 8.38");
  switch (rgbInfoValue) {
    case SQL_PARC_BATCH:
      strcpy(buffer, "SQL_PARC_BATCH");
      break;
    case SQL_PARC_NO_BATCH:
      strcpy(buffer, "SQL_PARC_NO_BATCH");
      break;
    default:
      strcpy(buffer, "???");
  }
  LOGF("Value = '%s'", buffer);

  /* *** 39.SQL_PARAM_ARRAY_SELECTS  (ODBC 3.0) */
  LOGF("8.39.SQL_PARAM_ARRAY_SELECTS (ODBC 3.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_PARAM_ARRAY_SELECTS, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 8.39");
 /* switch (rgbInfoValue) {
    case SQL_PAS_BATCH:
      strcpy(buffer, "SQL_PAS_BATCH");
      break;
    case SQL_PAS_NO_BATCH:
      strcpy(buffer, "SQL_PAS_NO_BATCH");
      break;
    case SQL_PAS_NO_SELECT:
      strcpy(buffer, "SQL_PAS_NO_SELECT");
      break;
    default:
      strcpy(buffer, "???");
  }
  LOGF("Value = '%s'", buffer);*/

  /* *** 40.   SQL_SQL_CONFORMANCE  (ODBC 3.0) */
  LOGF("8.40.   SQL_SQL_CONFORMANCE (ODBC 3.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_SQL_CONFORMANCE, (PTR) & rgbInfoValue,
      sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 8.40");
 /* switch (rgbInfoValue) {
    case SQL_SC_SQL92_ENTRY:
      strcpy(buffer, "SQL_SC_SQL92_ENTRY");
      break;
    case SQL_SC_FIPS127_2_TRANSITIONAL:
      strcpy(buffer, "SQL_SC_FIPS127_2_TRANSITIONAL");
      break;
    case SQL_SC_SQL92_FULL:
      strcpy(buffer, "SQL_SC_SQL92_FULL");
      break;
    case SQL_SC_SQL92_INTERMEDIATE:
      strcpy(buffer, "SQL_SC_SQL92_INTERMEDIATE:");
      break;
    default:
      strcpy(buffer, "???");
  }
  LOGF("Value = '%s'", buffer);*/

  /* *** 41.SQL_STANDARD_CLI_CONFORMANCE  (ODBC 3.0) */
  LOGF("8.41.SQL_STANDARD_CLI_CONFORMANCE (ODBC 3.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_STANDARD_CLI_CONFORMANCE,
      (PTR) & rgbInfoValue, sizeof(rgbInfoValue), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 8.41");
 /* strcpy(buffer, " ");
  if (rgbInfoValue & SQL_SCC_XOPEN_CLI_VERSION1) strcat(buffer,
      "SQL_SCC_XOPEN_CLI_VERSION1 |");
  if (rgbInfoValue & SQL_SCC_ISO92_CLI) strcat(buffer, "SQL_SCC_ISO92_CLI |");
  sprintf(bufxx, "\tValue = ");
  LOG(bufxx);*/

  /* *** 42.SQL_XOPEN_CLI_YEAR */
  LOGF("8.42.SQL_XOPEN_CLI_YEAR (ODBC 3.0) : ");
  retcode = SQLGetInfo(hdbc, SQL_XOPEN_CLI_YEAR, (PTR) & rgbInfoValueChar,
      sizeof(rgbInfoValueChar), NULL);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
      "SQLGetInfo 8.42");
 // LOGF("Value = '%s'", rgbInfoValueChar);

  /*
   SQL_SQL92_DATETIME_FUNCTIONS
   SQL_SQL92_FOREIGN_KEY_DELETE_RULE
   SQL_SQL92_FOREIGN_KEY_UPDATE_RULE
   SQL_SQL92_GRANT
   SQL_SQL92_NUMERIC_VALUE_FUNCTIONS
   SQL_SQL92_PREDICATES
   SQL_SQL92_RELATIONAL_JOIN_OPERATORS
   SQL_SQL92_REVOKE
   SQL_SQL92_ROW_VALUE_CONSTRUCTOR
   SQL_SQL92_STRING_FUNCTIONS
   SQL_SQL92_VALUE_EXPRESSIONS

   SQL_DYNAMIC_CURSOR_ATTRIBUTES1   SQLUINTEGER
   SQL_DYNAMIC_CURSOR_ATTRIBUTES2   SQLUINTEGER
   SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1    SQLUINTEGER
   SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2    SQLUINTEGER
   SQL_KEYSET_CURSOR_ATTRIBUTES1    SQLUINTEGER
   SQL_KEYSET_CURSOR_ATTRIBUTES2    SQLUINTEGER
   SQL_STATIC_CURSOR_ATTRIBUTES1    SQLUINTEGER
   SQL_STATIC_CURSOR_ATTRIBUTES2    SQLUINTEGER
   */
#endif
}
/*
 --------------------------------------------------------------------------------
 | CheckConversion:
 |
 |
 | Returns:
 |
 --------------------------------------------------------------------------------
 */

BEGIN_TEST(testSQLGetInfo1)
{
      retcode = ::SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
      ASSERT(retcode == SQL_SUCCESS, "SQLAllocHandle call failed");
      ASSERT(henv != NULL, "SQLAllocHandle failed to return valid env handle");

      retcode = ::SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);
      ASSERT(retcode == SQL_SUCCESS, "SQLAllocHandle call failed");
      ASSERT(hdbc != NULL, "SQLAllocHandle failed to return valid DBC handle");

      retcode = ::SQLGetInfo(SQL_NULL_HANDLE, 0, NULL, 0, NULL);
      ASSERT(retcode == SQL_INVALID_HANDLE,
          "SQLGetInfo returned invalid handle");
      retcode = ::SQLGetInfo(hdbc, 999, NULL, 0, NULL);
      ASSERT(retcode == SQL_ERROR, "SQLGetInfo should return sql error");
      SQLCHAR infoBuffer[1024];

      retcode = ::SQLGetInfo(hdbc, SQL_DRIVER_NAME, infoBuffer, 1024, NULL);
      ASSERT(retcode != SQL_ERROR, "SQLGetInfo returned successfully");
      ASSERT(strcmp((char*)infoBuffer,"Pivotal GemFire XD ODBC Driver") == 0,
          "SQLGetInfo should retrun driver name Pivotal GemFire XD ODBC Driver");
      retcode = ::SQLGetInfo(hdbc, SQL_DRIVER_VER, infoBuffer, 1024, NULL);
      ASSERT(retcode != SQL_ERROR, "SQLGetInfo returned successfully");
      ASSERT(strcmp((char*)infoBuffer,"01.00.02.0001") == 0,
          "SQLGetInfo rshould retrun driver version 01.00.02.0001");

      retcode = ::SQLGetInfo(hdbc, SQL_DRIVER_ODBC_VER, infoBuffer, 1024,
          NULL);
      ASSERT(retcode != SQL_ERROR, "SQLGetInfo returned successfully");
      ASSERT(strcmp((char*)infoBuffer,"03.51") == 0,
          "SQLGetInfo rshould retrun odbc driver version 03.51");

      retcode = ::SQLGetInfo(hdbc, SQL_ODBC_VER, infoBuffer, 1024, NULL);
      ASSERT(retcode != SQL_ERROR, "SQLGetInfo returned successfully");
      ASSERT(strcmp((char*)infoBuffer,"03.51.0000") == 0,
          "SQLGetInfo rshould retrun odbc version 03.51.0000");

      retcode = SQLDriverConnect(hdbc, NULL, (SQLCHAR*)GFXDCONNSTRING,
                       SQL_NTS, NULL, 0, NULL, SQL_DRIVER_NOPROMPT );
      ASSERT(retcode == SQL_SUCCESS, "SQLConnect call failed");

      retcode = ::SQLGetInfo(hdbc, SQL_EXPRESSIONS_IN_ORDERBY, infoBuffer,
          1024, NULL);
      ASSERT(retcode != SQL_ERROR, "SQLGetInfo returned successfully");
      ASSERT(strcmp((char*)infoBuffer,"Y") == 0,
          "SQLGetInfo rshould retrun Y");
      SQLUINTEGER infoVal = 0;
      retcode = ::SQLGetInfo(hdbc, SQL_SCROLL_OPTIONS, &infoVal, 0, NULL);
      ASSERT(retcode != SQL_ERROR, "SQLGetInfo returned successfully");
      ASSERT(infoVal == 20, "SQLGetInfo returned successfully");

      retcode = ::SQLGetInfo(hdbc, SQL_DYNAMIC_CURSOR_ATTRIBUTES1, &infoVal, 0,
          NULL);
      ASSERT(retcode != SQL_ERROR, "SQLGetInfo returned successfully");
      ASSERT(
          infoVal == (SQL_CA1_LOCK_NO_CHANGE | SQL_CA1_POSITIONED_UPDATE | SQL_CA1_POSITIONED_DELETE | SQL_CA1_SELECT_FOR_UPDATE),
          "SQLGetInfo returned successfully");

      retcode = ::SQLGetInfo(hdbc, SQL_DYNAMIC_CURSOR_ATTRIBUTES2, &infoVal, 0,
          NULL);
      ASSERT(retcode != SQL_ERROR, "SQLGetInfo returned successfully");
      ASSERT(
          infoVal == (SQL_CA2_SIMULATE_UNIQUE | SQL_CA2_SENSITIVITY_ADDITIONS | SQL_CA2_SENSITIVITY_DELETIONS | SQL_CA2_SENSITIVITY_UPDATES),
          "SQLGetInfo returned successfully");

      retcode = ::SQLGetInfo(hdbc, SQL_STATIC_CURSOR_ATTRIBUTES1, &infoVal, 0,
          NULL);
      ASSERT(retcode != SQL_ERROR, "SQLGetInfo returned successfully");
      ASSERT(
          infoVal == (SQL_CA1_LOCK_NO_CHANGE | SQL_CA1_POSITIONED_UPDATE | SQL_CA1_POSITIONED_DELETE | SQL_CA1_SELECT_FOR_UPDATE),
          "SQLGetInfo returned successfully");
      retcode = ::SQLGetInfo(hdbc, SQL_STATIC_CURSOR_ATTRIBUTES2, &infoVal, 0,
          NULL);
      ASSERT(retcode != SQL_ERROR, "SQLGetInfo returned successfully");
      ASSERT(infoVal == SQL_CA2_SIMULATE_UNIQUE,
          "SQLGetInfo returned successfully");

      retcode = ::SQLGetInfo(hdbc, SQL_BATCH_SUPPORT, &infoVal, 0, NULL);
      ASSERT(retcode != SQL_ERROR, "SQLGetInfo returned successfully");
      ASSERT(infoVal == SQL_BS_ROW_COUNT_EXPLICIT,
          "SQLGetInfo returned successfully");
      retcode = ::SQLGetInfo(hdbc, SQL_BATCH_ROW_COUNT, &infoVal, 0, NULL);
      ASSERT(retcode != SQL_ERROR, "SQLGetInfo returned successfully");
      ASSERT(infoVal == SQL_BRC_EXPLICIT, "SQLGetInfo returned successfully");

      retcode = ::SQLGetInfo(hdbc, SQL_PARAM_ARRAY_ROW_COUNTS, &infoVal, 0,
          NULL);
      ASSERT(retcode != SQL_ERROR, "SQLGetInfo returned successfully");
      ASSERT(infoVal == SQL_PARC_BATCH, "SQLGetInfo returned successfully");

      retcode = ::SQLGetInfo(hdbc, SQL_CURSOR_COMMIT_BEHAVIOR, &infoVal, 0,
          NULL);
      ASSERT(retcode != SQL_ERROR, "SQLGetInfo returned successfully");
      ASSERT(infoVal == SQL_CB_CLOSE, "SQLGetInfo returned successfully");

      retcode = ::SQLGetInfo(hdbc, SQL_CURSOR_ROLLBACK_BEHAVIOR, &infoVal, 0,
          NULL);
      ASSERT(retcode != SQL_ERROR, "SQLGetInfo returned successfully");
      ASSERT(infoVal == SQL_CB_DELETE, "SQLGetInfo returned successfully");

      retcode = ::SQLGetInfo(hdbc, SQL_GETDATA_EXTENSIONS, &infoVal, 0, NULL);
      ASSERT(retcode != SQL_ERROR, "SQLGetInfo returned successfully");
      ASSERT(
          infoVal == SQL_GD_ANY_COLUMN | SQL_GD_ANY_ORDER | SQL_GD_BLOCK | SQL_GD_BOUND,
          "SQLGetInfo returned successfully");
      retcode = ::SQLDisconnect(hdbc);
      ASSERT(retcode == SQL_SUCCESS, "SQLDisconnect  call failed");

      retcode = ::SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
      ASSERT(retcode == SQL_SUCCESS, "SQLFreeHandle call failed");

      retcode = ::SQLFreeHandle(SQL_HANDLE_ENV, henv);
      ASSERT(retcode == SQL_SUCCESS, "SQLFreeHandle call failed");
    }END_TEST(testSQLGetInfo1)

BEGIN_TEST(testSQLGetInfo2)
{
  /* ---------------------------------------------------------------------har- */
  //init sql handles (stmt, dbc, env)
      INIT_SQLHANDLES

      /* ---------------------------------------------------------------------- */
      /* ***** 1.) Driver Information */
      /* ---------------------------------------------------------------------- */
      PutDriverInformation(henv, hdbc, hstmt);
      /* ---------------------------------------------------------------------- */

      /* ***** 2.) DBMS Product Information */
      /* ---------------------------------------------------------------------- */
      PutDBMSProductInformation(henv, hdbc, hstmt);
      /* ---------------------------------------------------------------------- */

      /* ***** 3.) Data Source Information */
      /* ---------------------------------------------------------------------- */
      PutDataSourceInformation(henv, hdbc, hstmt);
      /* ---------------------------------------------------------------------- */

      /* ***** 4.) Supported SQL */
      /* ---------------------------------------------------------------------- */
      PutSupportedSQL(henv, hdbc, hstmt);
      /* ---------------------------------------------------------------------- */

      /* ***** 5.) SQL Limits */
      /* ---------------------------------------------------------------------- */
      PutSQLLimits(henv, hdbc, hstmt);
      /* ---------------------------------------------------------------------- */

      /* ***** 6.) Scalar Function Information */
      /* ---------------------------------------------------------------------- */
      PutScalarFunctionInformation(henv, hdbc, hstmt);
      /* ---------------------------------------------------------------------- */

      /* ***** 7.) Conversion Information */
      /* ---------------------------------------------------------------------- */
      PutConversionInformation(henv, hdbc, hstmt);
      /* ---------------------------------------------------------------------- */

      /* ***** 8.) ODBC 3.0 Information */
      /* ---------------------------------------------------------------------- */
      PutODBC30Information(henv, hdbc, hstmt);

      /* ------------------------------------------------------------------har- */
      //free sql handles (stmt, dbc, env)
      FREE_SQLHANDLES
}
END_TEST(testSQLGetInfo2)
