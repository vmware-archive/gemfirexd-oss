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
#define TESTNAME "SQLGetDiagRec"

#define TABLE    "TABERROR"
#define SQLSTMT1 "SELECT * FROM DUAL"

#define MAX_NAME_LEN 50
#define ERROR_TEXT_LEN 511
#define MAX_LONG     120

//*-------------------------------------------------------------------------

BEGIN_TEST(testSQLGetDiagRec)
{
  /* ------------------------------------------------------------------------- */
      CHAR create[MAX_NAME_LEN + MAX_NAME_LEN + 1];
      CHAR drop[MAX_NAME_LEN + 1];

      CHAR tabname[MAX_NAME_LEN];

      SQLCHAR Sqlstate[MAX_NAME_LEN];
      SQLINTEGER NativeError;
      SQLCHAR MessageText[ERROR_TEXT_LEN + 1];
      SQLSMALLINT BufferLength = ERROR_TEXT_LEN;
      SQLSMALLINT TextLength;
      SQLSMALLINT RecNum;

      CHAR buffer[1024];

      /* ------------------------------------------------------------------------- */
      //init sql handles (stmt, dbc, env)
      INIT_SQLHANDLES

      /* ----------------------------------------------------------------- */

      /* --- Create Table ------------------------------------------------ */
      strcpy(tabname, TABLE);
      strcpy(create, "CREATE TABLE ");
      strcat(create, tabname);
      strcat(create, " (TYP_CHAR CHAR(60) )");
      LOGF("Create Stmt = '%s'", create);

      retcode = SQLPrepare(hstmt, (SQLCHAR*)create, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLPrepare");

      retcode = SQLExecute(hstmt);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecute");

      /* --- SQLError   ------------------------------------------------- */
      retcode = SQLPrepare(hstmt, (SQLCHAR*)create, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLPrepare");

      retcode = SQLExecute(hstmt);
      LOGF("\t SQLExecute -> retcode: %d", retcode);
      if (retcode != SQL_SUCCESS) {
        RecNum = 1;
        while (retcode != SQL_NO_DATA_FOUND) {
          retcode = SQLGetDiagRec(SQL_HANDLE_STMT, hstmt, RecNum, Sqlstate,
              &NativeError, MessageText, BufferLength, &TextLength);
          LOGF("\t SQLGetDiagRec -> retcode: %d", retcode);

          if (retcode == SQL_SUCCESS) {
            sprintf(buffer, "Sqlstate  : %s", Sqlstate);
            LOGF("SQLError -> %s", buffer);
            sprintf(buffer, "NativeError : %ld", NativeError);
            LOGF("SQLError -> %s", buffer);
            sprintf(buffer, "MessageText : %s", MessageText);
            LOGF("SQLError -> %s", buffer);
            sprintf(buffer, "BufferLength  : %d", BufferLength);
            LOGF("SQLError -> %s", buffer);
            sprintf(buffer, "TextLength  : %d", TextLength);
            LOGF("SQLError -> %s", buffer);
          }
          RecNum++;
        }
      }

      /* --- Drop Table ------------------------------------------------- */
      strcpy(drop, "DROP TABLE ");
      strcat(drop, tabname);
      LOGF("Drop Stmt= '%s'", drop);

      retcode = SQLPrepare(hstmt, (SQLCHAR*)drop, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLPrepare");

      retcode = SQLExecute(hstmt);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecute");

      /* - Disconnect ---------------------------------------------------- */
      //free sql handles (stmt, dbc, env)
      FREE_SQLHANDLES

}
END_TEST(testSQLGetDiagRec)
