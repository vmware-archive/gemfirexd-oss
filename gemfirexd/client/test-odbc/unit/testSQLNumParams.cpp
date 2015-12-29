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

#define TESTNAME "SQLNumParams"
#define TABLE "NUMPARAMS"

#define MAX_NAME_LEN 50

//*-------------------------------------------------------------------------

BEGIN_TEST(testSQLNumParams)
{
  /* ------------------------------------------------------------------------- */
      CHAR tabname[MAX_NAME_LEN + 1];
      CHAR create[MAX_NAME_LEN + 1];
      CHAR drop[MAX_NAME_LEN + 1];
      CHAR insert[MAX_NAME_LEN + 1];
      CHAR buffer[MAX_NAME_LEN + 1];

      CHAR name[MAX_NAME_LEN];
      SQLINTEGER id;
      SQLLEN cb_name = SQL_NTS, cb_id = SQL_NTS;

      SQLSMALLINT pcpar;
      /* ---------------------------------------------------------------------har- */

      //init sql handles (stmt, dbc, env)
      INIT_SQLHANDLES

      /* ------------------------------------------------------------------------- */

      /* --- Create Table --------------------------------------------- */
      strcpy(tabname, TABLE);
      strcpy(create, "CREATE TABLE ");
      strcat(create, tabname);
      strcat(create, " (ID INTEGER, NAME CHAR(80))");
      printf("\tCreate Stmt = '%s'\r\n", create);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)create, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      /* --- Insert Table --------------------------------------------- */
      /* 1. */
      strcpy(tabname, TABLE);
      strcpy(insert, "INSERT INTO ");
      strcat(insert, tabname);
      strcat(insert, " VALUES (?, ?)");
      printf("\tInsert Stmt= '%s'\r\n", insert);

      retcode = SQLPrepare(hstmt, (SQLCHAR*)insert, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLPrepare");

      retcode = SQLNumParams(hstmt, &pcpar);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLNumParams");

      sprintf(buffer, "1. Result of SQLNumParams : %d", pcpar);
      printf("\tInsert Stmt= '%s'\r\n", buffer);

      retcode = SQLBindParameter(hstmt, 1, SQL_PARAM_INPUT, SQL_C_LONG,
          SQL_INTEGER, 0, 0, &id, 0, &cb_id);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLBindParameter");

      retcode = SQLBindParameter(hstmt, 2, SQL_PARAM_INPUT, SQL_C_CHAR,
          SQL_VARCHAR, 0, 0, &name, 0, &cb_name);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLBindParameter");

      id = 500;
      strcpy(name, "Babbage");
      printf("\tInsert Values = 1->'%ld' - 2->'%s'\r\n", id, name);

      retcode = SQLExecute(hstmt);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecute");

      /* --- Drop Table ----------------------------------------------- */
#ifdef DROP_TABLE
      strcpy(tabname, TABLE);
      strcpy(drop, "DROP TABLE ");
      strcat(drop, tabname);
      printf("\tDrop Stmt= '%s'\r\n", drop);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)drop, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");
#endif
      /* ---------------------------------------------------------------------har- */
      //free sql handles (stmt, dbc, env)
      FREE_SQLHANDLES

}
END_TEST(testSQLNumParams)
