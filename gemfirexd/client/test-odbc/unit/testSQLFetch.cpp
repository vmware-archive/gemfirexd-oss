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
#define TESTNAME "SQLFetch"
#define TABLE "TABFETCH"

#define MAX_NAME_LEN 120
#define MAX_DATE     10
#define MAX_ROWS     10

//*-------------------------------------------------------------------------

BEGIN_TEST(testSQLFetch)
{
      /* ------------------------------------------------------------------------- */
      CHAR create[MAX_NAME_LEN];
      CHAR drop[MAX_NAME_LEN + 1];
      CHAR insert[MAX_NAME_LEN + 1];
      CHAR select[MAX_NAME_LEN + 1];

      CHAR tabname[MAX_NAME_LEN + 1];

      SQLSMALLINT i;
      /* ---------------------------------------------------------------------har- */

      //init sql handles (stmt, dbc, env)
      INIT_SQLHANDLES

      /* ---------------------------------------------------------------------har- */

      /* --- Create Table --------------------------------------------- */
      strcpy(tabname, TABLE);
      strcpy(create, "CREATE TABLE ");
      strcat(create, tabname);
      strcat(create, " ( TYP_CHAR CHAR(120)) ");
      LOGF("Create Stmt = '%s'", create);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)create, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      /* --- Insert Table --------------------------------------------- */
      /* --- 1. ---*/
      strcpy(insert, "INSERT INTO ");
      strcat(insert, tabname);
      strcat(insert, " ( TYP_CHAR ) ");
      strcat(insert, " VALUES ('<< Dies ist ein SQLFETCH-Test >>') ");
      LOGF("Insert Stmt = '%s'", insert);

      i = 1;
      while (i < MAX_ROWS) {
        retcode = SQLExecDirect(hstmt, (SQLCHAR*)insert, SQL_NTS);
        DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
            "SQLExecDirect");

        i++;
      }

      /* --- Select Table --------------------------------------------- */
      /* --- 1. --- */
      strcpy(select, "SELECT ");
      strcat(select, " TYP_CHAR ");
      strcat(select, " FROM ");
      strcat(select, tabname);
      LOGF("Select Stmt= '%s'", select);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)select, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      i = 1;
      while (1) {
        retcode = SQLFetch(hstmt);
        if (retcode == SQL_NO_DATA_FOUND) break;
        DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
            "SQLFetch");
      }

      retcode = SQLFreeStmt(hstmt, SQL_UNBIND);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLFreeStmt");

      /* --- Drop Table ----------------------------------------------- */
      strcpy(drop, "DROP TABLE ");
      strcat(drop, tabname);
      LOGF("Drop Stmt= '%s'", drop);

      retcode = SQLFreeStmt(hstmt, SQL_CLOSE);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLFreeStmt");

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)drop, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");
      /* ---------------------------------------------------------------------har- */
      //free sql handles (stmt, dbc, env)
      FREE_SQLHANDLES

}
END_TEST(testSQLFetch)
