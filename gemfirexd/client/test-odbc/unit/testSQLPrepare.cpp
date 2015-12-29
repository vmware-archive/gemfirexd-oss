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

#define TESTNAME "SQLPrepare"
#define TABLE "SQLPREPARE"

#define MAX_NAME_LEN 50

//*-------------------------------------------------------------------------

BEGIN_TEST(testSQLPrepare)
{
  /* ------------------------------------------------------------------------- */
      CHAR tabname[MAX_NAME_LEN + 1];
      CHAR create[MAX_NAME_LEN + 1];
      CHAR insert[MAX_NAME_LEN + 1];
      CHAR drop[MAX_NAME_LEN + 1];
      /* ---------------------------------------------------------------------har- */

      //init sql handles (stmt, dbc, env)
      INIT_SQLHANDLES
      /* ------------------------------------------------------------------------- */

      /* --- Create Table --------------------------------------------- */
      strcpy(tabname, TABLE);
      strcpy(create, "CREATE TABLE ");
      strcat(create, tabname);
      strcat(create, " (ID INTEGER, NAME VARCHAR(80), AGE SMALLINT)");
      printf("\tCreate Stmt = '%s'\r\n", create);

      retcode = SQLPrepare(hstmt, (SQLCHAR*)create, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLPrepare");

      retcode = SQLExecute(hstmt);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecute");

      /* --- Insert Table --------------------------------------------- */
      strcpy(tabname, TABLE);
      strcpy(insert, "INSERT INTO ");
      strcat(insert, tabname);
      strcat(insert, " VALUES (?, ?, ?)");
      printf("\tInsert Stmt= '%s'\r\n", insert);

      retcode = SQLPrepare(hstmt, (SQLCHAR*)insert, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLPrepare");

      /* ---------------------------------------------------------------------har- */
      /* --- Drop Table ----------------------------------------------- */
      strcpy(tabname, TABLE);
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
END_TEST(testSQLPrepare)

