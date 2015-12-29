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

//*-------------------------------------------------------------------------

#define TESTNAME "SQLNativeSql"

#define MAX_NAME_LEN 80
#define ERROR_TEXT_LEN 511

#define STR_LEN 128+1
#define REM_LEN 254+1

#define NULL_VALUE "<NULL>"

//*-------------------------------------------------------------------------

BEGIN_TEST(testSQLNativeSql1)
{
      retcode = ::SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
      ASSERT(retcode == SQL_SUCCESS, "SQLAllocHandle call failed");
      ASSERT(henv != NULL, "SQLAllocHandle failed to return valid env handle");

      retcode = ::SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);
      ASSERT(retcode == SQL_SUCCESS, "SQLAllocHandle call failed");
      ASSERT(hdbc != NULL, "SQLAllocHandle failed to return valid DBC handle");

      retcode = ::SQLNativeSql(SQL_NULL_HANDLE, NULL, 0, NULL, 0, NULL);
      ASSERT(retcode == SQL_INVALID_HANDLE,
          "SQLNativeSql should return Invalid Handle");

      retcode = ::SQLNativeSql(SQL_NULL_HANDLE, (SQLCHAR*)"abc", 0, NULL, 0,
          NULL);
      ASSERT(retcode == SQL_INVALID_HANDLE,
          "SQLNativeSql should return Invalid Handle");

      retcode = ::SQLNativeSql(hdbc, NULL, 0, NULL, 0, NULL);
      ASSERT(retcode == SQL_ERROR, "SQLNativeSql should return sql error");

      retcode = ::SQLNativeSql(hdbc, (SQLCHAR*)"abc", 0, NULL, 0, NULL);
      ASSERT(retcode == SQL_ERROR, "SQLNativeSql should return sql error");

      retcode = SQLDriverConnect(hdbc, NULL, (SQLCHAR*)GFXDCONNSTRING,
                       SQL_NTS, NULL, 0, NULL, SQL_DRIVER_NOPROMPT );
      ASSERT(retcode == SQL_SUCCESS, "SQLConnect call failed");

      SQLCHAR buffer[1024];
      retcode = ::SQLNativeSql(hdbc, (SQLCHAR*)"SELECT "
          "{ fn CONVERT (empid, SQL_SMALLINT) } FROM employee",
          strlen("SELECT { fn CONVERT (empid, SQL_SMALLINT) } FROM employee"),
          buffer, 1024, NULL);
      ASSERT(retcode == SQL_SUCCESS, "SQLNativeSql call failed");

      //this call is working now not sure how ODBC driver handles this
      retcode = ::SQLNativeSql(hdbc, (SQLCHAR*)"INVALIDSQL",
          strlen("INVALIDSQL"), buffer, 1024, NULL);
      ASSERT(retcode == SQL_SUCCESS, "SQLNativeSql call failed");

      retcode = ::SQLDisconnect(hdbc);
      ASSERT(retcode == SQL_SUCCESS, "SQLDisconnect  call failed");

      retcode = ::SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
      ASSERT(retcode == SQL_SUCCESS, "SQLFreeHandle call failed");

      retcode = ::SQLFreeHandle(SQL_HANDLE_ENV, henv);
      ASSERT(retcode == SQL_SUCCESS, "SQLFreeHandle call failed");

}
END_TEST(testSQLNativeSql1)

BEGIN_TEST(testSQLNativeSql2)
{
  /* ------------------------------------------------------------------------- */
  /* Declare storage locations for result set data */
      CHAR szSqlStrIn[STR_LEN], szSqlStr[STR_LEN];

      SQLINTEGER pcbSqlStr;

      SQLINTEGER cbSqlStrIn, cbSqlStrMax;

      //CHAR sqlstate[10];
      //DWORD esq_sql_code;
      //CHAR error_txt[ERROR_TEXT_LEN + 1];
      //SQLSMALLINT len_error_txt = ERROR_TEXT_LEN;
      //SQLSMALLINT used_error_txt;
      //CHAR buffer[1024];
      /* ---------------------------------------------------------------------har- */

      //init sql handles (stmt, dbc, env)
      INIT_SQLHANDLES

      /* ------------------------------------------------------------------------- */
      /* ***************************************************************** */
      /* *** I. SQLNativSql ********************************************** */
      /* ***************************************************************** */
      LOGF("I.) SQLNativeSql -> '");

      strcpy(szSqlStrIn, "CREATE TABLE TEST (KEY INTEGER, NAME CHAR(30))");
      cbSqlStrMax = strlen(szSqlStrIn) + 1;
      cbSqlStrIn = SQL_NTS;
      LOGF("IN => SqlStrIn   : %s' \t cbSqlStrIn : %d  \t cbSqlStrMax: %d ", szSqlStrIn, cbSqlStrIn, cbSqlStrMax);

      retcode = SQLNativeSql(hdbc, (SQLCHAR*)szSqlStrIn, cbSqlStrIn, (SQLCHAR*)szSqlStr,
          cbSqlStrMax, &pcbSqlStr);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLNativeSql");

      LOGF("OUT => SqlStr : '%s'\t pcbSqlStr : %d '", szSqlStr, pcbSqlStr);

      /* ***************************************************************** */
      /* *** II. SQLNativSql ********************************************* */
      /* ***************************************************************** */
      LOGF("II.) SQLNativeSql -> (Data trucated)'");

      strcpy(szSqlStrIn, "CREATE TABLE TEST (KEY INTEGER, NAME CHAR(30))");
      cbSqlStrMax = strlen(szSqlStrIn) - 5;
      cbSqlStrIn = SQL_NTS;
      LOGF("IN => SqlStrIn   : %s' \t cbSqlStrIn : %d  \t cbSqlStrMax: %d ", szSqlStrIn, cbSqlStrIn, cbSqlStrMax);

      retcode = SQLNativeSql(hdbc, (SQLCHAR*)szSqlStrIn, cbSqlStrIn, (SQLCHAR*)szSqlStr,
          cbSqlStrMax, &pcbSqlStr);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLNativeSql");


      LOGF("OUT => SqlStr : '%s'\t pcbSqlStr : %d '", szSqlStr, pcbSqlStr);

      /* ***************************************************************** */
      /* *** III. SQLNativSql ******************************************** */
      /* ***************************************************************** */
      LOGF("III.) SQLNativeSql -> (Error)'");

      strcpy(szSqlStrIn, "CREATE TABLE TEST (KEY INTEGER, NAME CHAR(30))");
      cbSqlStrMax = SQL_NTS;
      cbSqlStrIn = SQL_NTS;
      LOGF("IN => SqlStrIn   : %s' \t cbSqlStrIn : %d  \t cbSqlStrMax: %d ", szSqlStrIn, cbSqlStrIn, cbSqlStrMax);

      retcode = SQLNativeSql(hdbc, (SQLCHAR*)szSqlStrIn, cbSqlStrIn, (SQLCHAR*)szSqlStr,
          cbSqlStrMax, &pcbSqlStr);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,"SQLNativeSql");

      /* ---------------------------------------------------------------------har- */
      //free sql handles (stmt, dbc, env)
      FREE_SQLHANDLES
}
END_TEST(testSQLNativeSql2)
