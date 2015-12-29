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
#define TESTNAME "SQLSetCursorName"
#define TABLE "SETCURSOR"
#define TESTCUR "SQL_TEST_CUR"

#define MAX_NAME_LEN 50
#define MAX_CUR_NAME_LEN 18

//*-------------------------------------------------------------------------

BEGIN_TEST(testSQLSetCursorName)
{
  /* ------------------------------------------------------------------------- */
      CHAR create[MAX_NAME_LEN + MAX_NAME_LEN + 1];
      CHAR drop[MAX_NAME_LEN + 1];

      CHAR tabname[MAX_NAME_LEN];

      CHAR szCursor[MAX_CUR_NAME_LEN];
      SQLSMALLINT cbCursorMax;
      SQLSMALLINT cbCursor;
      SQLSMALLINT pcbCursor;

      /* ---------------------------------------------------------------------har- */

      //init sql handles (stmt, dbc, env)
      INIT_SQLHANDLES

      /* ---------------------------------------------------------------------har- */

      /* --- SQLSetCursorName -------------------------------------------- */
      strcpy(szCursor, TESTCUR);
      cbCursor = strlen(szCursor);
      retcode = SQLSetCursorName(hstmt, (SQLCHAR*)szCursor, cbCursor);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLSetCursorName");

      printf("\tSetCursorName = '%s'\r\n", szCursor);

      /* --- Create Table --------------------------------------------- */
      strcpy(tabname, TABLE);
      strcpy(create, "CREATE TABLE ");
      strcat(create, tabname);
      strcat(create, " (TYP_CHAR CHAR(60) )");
      printf("\tCreate Stmt = '%s'\r\n", create);

      retcode = SQLPrepare(hstmt, (SQLCHAR*)create, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLPrepare");

      retcode = SQLExecute(hstmt);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecute");

      /* --- SQLGetCursorName -------------------------------------------- */
      cbCursorMax = MAX_CUR_NAME_LEN;
      retcode = SQLGetCursorName(hstmt, (SQLCHAR*)szCursor, cbCursorMax, &pcbCursor);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLGetCursorName");

      printf("\tGetCursorName = '%s'\r\n", szCursor);

      /* --- Drop Table ----------------------------------------------- */
      strcpy(drop, "DROP TABLE ");
      strcat(drop, tabname);
      printf("\tDrop Stmt= '%s'\r\n", drop);

      retcode = SQLPrepare(hstmt, (SQLCHAR*)drop, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLPrepare");

      retcode = SQLExecute(hstmt);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecute");
      /* ---------------------------------------------------------------------har- */
      //free sql handles (stmt, dbc, env)
      FREE_SQLHANDLES
}
END_TEST(testSQLSetCursorName)
