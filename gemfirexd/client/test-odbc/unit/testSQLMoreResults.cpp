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

#define TESTNAME "SQLMoreResults"
#define TABLE "TABSQLMORER"
#define SQLPROC "PROCE_MORERESULTS"

#define MAX_NAME_LEN 100
#define MAX_SP_LEN 8192

//*-------------------------------------------------------------------------

BEGIN_TEST(testSQLMoreResults1)
{
  /* ------------------------------------------------------------------------- */
      CHAR tabname[MAX_NAME_LEN + 1];
      CHAR create[MAX_NAME_LEN + 1];
      CHAR drop[MAX_NAME_LEN + 1];
      CHAR insert[MAX_NAME_LEN + 1];
      CHAR select[MAX_NAME_LEN + 1];
      CHAR buffer[MAX_NAME_LEN + 1];
      CHAR spcreate[MAX_SP_LEN + 1];

      CHAR name[MAX_NAME_LEN];
      SQLINTEGER id;
      SQLLEN cb_name = SQL_NTS, cb_id = SQL_NTS;
      /* ---------------------------------------------------------------------har- */
      //init sql handles (stmt, dbc, env)
      INIT_SQLHANDLES

      /* ------------------------------------------------------------------------- */

      /* --- Create Table --------------------------------------------- */
      strcpy(tabname, TABLE);
      strcpy(create, "CREATE TABLE ");
      strcat(create, tabname);
      strcat(create, " (ID INTEGER, NAME CHAR(80))");
      LOGF("Create Stmt = '%s'\r\n", create);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)create, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      /* --- Insert Table --------------------------------------------- */
      /* 1. */
      strcpy(tabname, TABLE);
      strcpy(insert, "INSERT INTO ");
      strcat(insert, tabname);
      strcat(insert, " VALUES (?, ?)");
      LOGF("Insert Stmt= '%s'\r\n", insert);

      retcode = SQLPrepare(hstmt, (SQLCHAR*)insert, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLPrepare");

      retcode = SQLBindParameter(hstmt, 1, SQL_PARAM_INPUT, SQL_C_LONG,
          SQL_INTEGER, 0, 0, &id, 0, &cb_id);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLBindParameter");

      retcode = SQLBindParameter(hstmt, 2, SQL_PARAM_INPUT, SQL_C_CHAR,
          SQL_VARCHAR, 0, 0, &name, 0, &cb_name);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLBindParameter");

      id = 1;
      strcpy(name, "ODBC-USER");
      LOGF("Insert Values = 1->'%ld' - 2->'%s'\r\n", id, name);

      retcode = SQLExecute(hstmt);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecute");

      /* --- Select Table --------------------------------------------- */
      /* 1. */
      strcpy(tabname, TABLE);
      strcpy(select, "SELECT * FROM ");
      strcat(select, tabname);
      LOGF("Select Stmt= '%s'\r\n", select);

      retcode = SQLPrepare(hstmt, (SQLCHAR*)select, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLPrepare");

      retcode = SQLBindCol(hstmt, 1, SQL_C_LONG, &id, 0, &cb_id);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLBindCol");

      retcode = SQLBindCol(hstmt, 2, SQL_C_CHAR, name, MAX_NAME_LEN, &cb_name);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLBindCol");

      retcode = SQLExecute(hstmt);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecute");

      retcode = SQLMoreResults(hstmt);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_NO_DATA_FOUND, retcode,
          "SQLMoreResults");

      switch (retcode) {
        case SQL_SUCCESS:
          strcpy(buffer, "SQL_SUCCESS");
          break;
        case SQL_NO_DATA_FOUND:
          strcpy(buffer, "SQL_NO_DATA_FOUND");
          break;
      }
      LOGF("SQLMoreResults returns : '%s'\r\n", buffer);

      retcode = SQLFreeStmt(hstmt, SQL_CLOSE);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLFreeStmt");

      /* --- Drop Table ----------------------------------------------- */
      strcpy(tabname, TABLE);
      strcpy(drop, "DROP TABLE ");
      strcat(drop, tabname);
      LOGF("Drop Stmt= '%s'\r\n", drop);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)drop, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");
      /* ---------------------------------------------------------------------har- */
      //free sql handles (stmt, dbc, env)
      FREE_SQLHANDLES
    }
    END_TEST(testSQLMoreResults1)

//*-------------------------------------------------------------------------

BEGIN_TEST(testSQLMoreResults2)
{
  /* ------------------------------------------------------------------------- */
      CHAR spcreate[MAX_SP_LEN + 1];
      /* ---------------------------------------------------------------------har- */
      //init sql handles (stmt, dbc, env)
      INIT_SQLHANDLES

      /* ------------------------------------------------------------------------- */

      retcode = SQLExecDirect(hstmt,
          (SQLCHAR*)"DROP TABLE IF EXISTS EMPLOYEE_TABLE", SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      retcode =
          SQLExecDirect(hstmt,
              (SQLCHAR*)"CREATE TABLE EMPLOYEE_TABLE (EMPID INTEGER, EMPNAME CHAR(80))",
              SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      /* --- Insert values --------------------------------------------- */
      retcode = SQLExecDirect(hstmt,
          (SQLCHAR*)"INSERT INTO EMPLOYEE_TABLE VALUES (1, 'user1')", SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      retcode = SQLExecDirect(hstmt,
          (SQLCHAR*)"INSERT INTO EMPLOYEE_TABLE VALUES (2, 'user2')", SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      /* --- drop if exists and create store procedure-------------------------------- */
      retcode = SQLExecDirect(hstmt,
          (SQLCHAR*)"DROP PROCEDURE IF EXISTS PROC_MORERESULTS", SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      strcpy(spcreate,
          "CREATE PROCEDURE PROC_MORERESULTS() LANGUAGE JAVA PARAMETER STYLE JAVA "
              "READS SQL DATA DYNAMIC RESULT SETS 2 EXTERNAL NAME "
              "'tests.TestProcedures.multipleResultSets'");
      LOGF("Drop SP Stmt = '%s'\r\n", spcreate);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)spcreate, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)"call PROC_MORERESULTS()",
          SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      do {
        SQLCHAR buff[10];
        retcode = SQLFetch(hstmt);
        DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
            "SQLFetch");

        SQLLEN rowCount = -1;
        retcode = SQLRowCount(hstmt, &rowCount);
        DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
            "SQLExecDirect");
        ASSERT(rowCount != 2, "SQLRowCount returned invalid rowcount");

        retcode = SQLGetData(hstmt, 1, SQL_C_CHAR, buff, 10, NULL);
        DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
            "SQLExecDirect");
        LOGF("SQLGetData : 1. returned value is %s", buff);
        ASSERT((strcmp((const char* )buff, "2") == 0),
            "SQLGetData returned invalid value");

        retcode = SQLGetData(hstmt, 2, SQL_C_CHAR, buff, 10, NULL);
        DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
            "SQLExecDirect");
        LOGF("SQLGetData : 2. returned value is %s", buff);
        ASSERT((strcmp((const char* )buff, "user2") == 0),
            "SQLGetData returned invalid value");

        retcode = SQLMoreResults(hstmt);

      }
      while (retcode != SQL_NO_DATA);

      retcode = SQLFreeStmt(hstmt, SQL_CLOSE);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLFreeStmt");

      /* --- Drop procedure ----------------------------------------------- */
      retcode = SQLExecDirect(hstmt,
          (SQLCHAR*)"DROP PROCEDURE IF EXISTS PROC_MORERESULTS", SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      /* --- Drop Table ----------------------------------------------- */
      retcode = SQLExecDirect(hstmt,
          (SQLCHAR*)"DROP TABLE IF EXISTS EMPLOYEE_TABLE", SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");
      /* ---------------------------------------------------------------------har- */
      //free sql handles (stmt, dbc, env)
      FREE_SQLHANDLES
}
END_TEST(testSQLMoreResults2)

BEGIN_TEST(testSQLMoreResults3)
{
  /* ------------------------------------------------------------------------- */
      CHAR name[MAX_NAME_LEN];
      SQLINTEGER id;
      SQLLEN cb_name = SQL_NTS, cb_id = SQL_NTS;
      /* ---------------------------------------------------------------------har- */
      //init sql handles (stmt, dbc, env)
      INIT_SQLHANDLES

      /* ------------------------------------------------------------------------- */
      /* --- drop if exists and create store procedure-------------------------------- */
      retcode = SQLExecDirect(hstmt,
          (SQLCHAR*)"DROP PROCEDURE IF EXISTS PROC_OUTPARAMS", SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      retcode =
          SQLExecDirect(hstmt,
              (SQLCHAR*)"CREATE PROCEDURE PROC_INOUTPARAMS(INOUT name VARCHAR(25), "
                  "OUT total INT) LANGUAGE JAVA PARAMETER STYLE JAVA EXTERNAL NAME "
                  "'tests.TestProcedures.inoutParamProc'", SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      retcode = SQLPrepare(hstmt, (SQLCHAR*)"call PROC_INOUTPARAMS(?,?)",
          SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLPrepare");

      retcode = SQLBindParameter(hstmt, 1, SQL_PARAM_INPUT_OUTPUT, SQL_C_CHAR,
          SQL_VARCHAR, 0, 0, &name, 100, &cb_name);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLBindParameter");

      retcode = SQLBindParameter(hstmt, 2, SQL_PARAM_OUTPUT, SQL_C_LONG,
          SQL_INTEGER, 0, 0, &id, 0, &cb_id);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLBindParameter");

      id = 1;
      strcpy(name, "ODBC-USER");

      retcode = SQLExecute(hstmt);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecute");

      LOGF("Values after execute = 1->'%ld' - 2->'%s'\r\n", id, name);

      ASSERT(id==10,"Value of id should be 10");
      ASSERT(strcmp("ODBC-USER-Modified",name) == 0,"Value of name should be ODBC-USER-Modified");

      /* ---------------------------------------------------------------------har- */
      //free sql handles (stmt, dbc, env)
      FREE_SQLHANDLES
}
END_TEST(testSQLMoreResults3)

