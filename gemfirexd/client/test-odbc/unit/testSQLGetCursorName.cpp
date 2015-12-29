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
#define TESTNAME "SQLGetCursorName"
#define TABLE "GETCURSOR"

#define MAX_NAME_LEN 50
#define MAX_CUR_NAME_LEN 18

//*-------------------------------------------------------------------------

BEGIN_TEST(testSQLGetCursorName)
{
  /* ------------------------------------------------------------------------- */
      CHAR create[MAX_NAME_LEN + MAX_NAME_LEN + 1];
      CHAR update[MAX_NAME_LEN + MAX_NAME_LEN + 1];
      CHAR drop[MAX_NAME_LEN + 1];

      CHAR tabname[MAX_NAME_LEN];

      CHAR szCursor[MAX_CUR_NAME_LEN];
      SQLSMALLINT cbCursorMax;
      SQLSMALLINT pcbCursor;
      /* ---------------------------------------------------------------------har- */

      //init sql handles (stmt, dbc, env)
      INIT_SQLHANDLES
      /* ---------------------------------------------------------------------har- */

      /* --- Create Table --------------------------------------------- */
      strcpy(tabname, TABLE);
      strcpy(create, "CREATE TABLE ");
      strcat(create, tabname);
      strcat(create, " (TYP_CHAR CHAR(60) )");
      LOGF("tCreate Stmt = '%s'", create);

      retcode = SQLPrepare(hstmt, (SQLCHAR*)create, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLPrepare");

      retcode = SQLExecute(hstmt);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecute");

      /* --- SQLGetCursorName ------------------------------------------ */
      cbCursorMax = MAX_CUR_NAME_LEN;
      retcode = SQLGetCursorName(hstmt, (SQLCHAR*)szCursor, cbCursorMax,
          &pcbCursor);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLGetCursorName");

      LOGF("tGetCursorName = '%s'", szCursor);

      /* --- Use a cursor name are positioned update and delete -------- */

      /* --- Update Table --------------------------------------------- */
      strcpy(tabname, TABLE);
      strcpy(update, "UPDATE ");
      strcat(update, tabname);
      strcat(update, " SET TYP_CHAR=\"Smith, John D.\" ");
      strcat(update, " WHERE CURRENT OF ");
      strcat(update, szCursor);
      LOGF("tUpdate Stmt = '%s'", update);
      /*
       retcode = SQLPrepare(hstmt, update, SQL_NTS);
       DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,"SQLPrepare");

       retcode = SQLExecute(hstmt);
       DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,"SQLExecute");
       */
      /* --- Drop Table ----------------------------------------------- */
      strcpy(drop, "DROP TABLE ");
      strcat(drop, tabname);
      LOGF("tDrop Stmt= '%s'", drop);

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
END_TEST(testSQLGetCursorName)
