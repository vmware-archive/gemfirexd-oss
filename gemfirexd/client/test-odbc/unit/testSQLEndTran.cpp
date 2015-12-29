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
#define TESTNAME "SQLEndTran"

#define TABLE1  "TABENDTRAN"

#define SQLSTMT1 "SELECT * FROM DUAL"

#define MAX_ROWS 5      /* Max. Zeilen      */
#define MAX_NAME_LEN 256

//*-------------------------------------------------------------------------

BEGIN_TEST(testSQLEndTran)
{
      CHAR create[MAX_NAME_LEN + 1];
      CHAR drop[MAX_NAME_LEN + 1];
      CHAR insert[MAX_NAME_LEN + 1];
      CHAR select[MAX_NAME_LEN + 1];
      CHAR buffer[MAX_NAME_LEN + 1];

      CHAR szCharData[MAX_NAME_LEN];
      SQLSMALLINT i, j, count = 0;
      SQLINTEGER pAutoCommit;

      SQLINTEGER BufferLength;
      SQLINTEGER StringLengthPtr = 0;

      /* ------------------------------------------------------------------------- */
      /* - Connect ------------------------------------------------------- */
      //init sql handles (stmt, dbc, env)
      INIT_SQLHANDLES

      /* ----------------------------------------------------------------- */
      /* --- SQLSetConnectOption ----------------------------------------- */
      /* *** SQL_AUTOCOMMIT ------------------ *** */
      BufferLength = SQL_IS_UINTEGER;
      retcode = SQLGetConnectAttr(hdbc, SQL_AUTOCOMMIT, &pAutoCommit,
          BufferLength, &StringLengthPtr);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLGetConnectAttr");

      if (pAutoCommit == SQL_AUTOCOMMIT_ON)
        strcpy(buffer, "SQL_AUTOCOMMIT_ON");
      else if (pAutoCommit == SQL_AUTOCOMMIT_OFF) strcpy(buffer,
          "SQL_AUTOCOMMIT_OFF");
      LOGF("SQL_AUTOCOMMIT  : '%s' ", buffer);

      if (pAutoCommit == SQL_AUTOCOMMIT_ON) {
        LOGF("Set SQL_AUTOCOMMIT to OFF ");
        pAutoCommit = SQL_AUTOCOMMIT_ON;

        retcode = SQLSetConnectAttr(hdbc, SQL_AUTOCOMMIT, SQL_AUTOCOMMIT_OFF,
            BufferLength);
        DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
            "SQLSetConnectAttr");

        retcode = SQLGetConnectAttr(hdbc, SQL_AUTOCOMMIT, &pAutoCommit,
            BufferLength, &StringLengthPtr);
        DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
            "SQLGetConnectAttr");

        if (pAutoCommit == SQL_AUTOCOMMIT_ON)
          strcpy(buffer, "SQL_AUTOCOMMIT_ON");
        else if (pAutoCommit == SQL_AUTOCOMMIT_OFF) strcpy(buffer,
            "SQL_AUTOCOMMIT_OFF");
        LOGF(" SQL_AUTOCOMMIT : '%s' ", buffer);
      }

      /* --- Create Table 1. --------------------------------------------- */
      strcpy(create, "CREATE TABLE ");
      strcat(create, TABLE1);
      strcat(create, " ( NAME CHAR(50), AGE INTEGER, ADRESSE CHAR(80))");
      LOGF("Create Stmt 1. = '%s'", create);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)create, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      retcode = SQLEndTran(SQL_HANDLE_DBC, (SQLHANDLE)hdbc, SQL_ROLLBACK);
      DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
          "SQLEndTran (SQL_ROLLBACK)");

      /*retcode = SQLExecDirect(hstmt, (SQLCHAR*)create, SQL_NTS);
       DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
       "SQLExecDirect");*/

      retcode = SQLEndTran(SQL_HANDLE_DBC, (SQLHANDLE)hdbc, SQL_COMMIT);
      DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
          "SQLEndTran (SQL_COMMIT)");

      /* --- Insert Table 1. ------------------------------------------- */
      strcpy(insert, "INSERT INTO ");
      strcat(insert, TABLE1);
      strcat(insert, " VALUES ('Heinrich', 44, 'Test street 96, Berlin')");

      for (j = 0; j < MAX_ROWS; j++) {
        LOGF("Insert Stmt 1. = <%d.> '%s'", j + 1, insert);

        retcode = SQLExecDirect(hstmt, (SQLCHAR*)insert, SQL_NTS);
        DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
            "SQLExecDirect");

        retcode = SQLCloseCursor(hstmt);
        DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_ERROR, retcode,
            "SQLCloseCursor");
      }

      /* --- Select Table 1. ------------------------------------------- */
      strcpy(select, "SELECT * FROM ");
      strcat(select, TABLE1);
      LOGF("Select Stmt 1. = '%s'", select);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)select, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");
      count = 1;
      i = 1;
      while (retcode != SQL_NO_DATA_FOUND) {
        retcode = SQLFetch(hstmt);
        if (retcode != SQL_NO_DATA_FOUND) {
          DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
                      "SQLFetch");
          retcode = SQLGetData(hstmt, i, SQL_C_CHAR, szCharData,
              sizeof(szCharData), NULL);
          DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
              "SQLGetData");
          LOGF(" Column %d (Pos.%d) = '%s'", i, count, szCharData);
          count++;
        }
      }
      retcode = SQLCloseCursor(hstmt);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLCloseCursor");

      /* --- Drop Table 1. -------------------------------------------- */
      strcpy(drop, "DROP TABLE ");
      strcat(drop, TABLE1);
      LOGF("Drop Stmt 1.= '%s'", drop);

      retcode = SQLCloseCursor(hstmt);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_ERROR, retcode,
          "SQLCloseCursor");

      /*retcode = SQLExecDirect(hstmt, (SQLCHAR*)drop, SQL_NTS);
       DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
       "SQLExecDirect");*/

      retcode = SQLEndTran(SQL_HANDLE_DBC, (SQLHANDLE)hdbc, SQL_ROLLBACK);
      DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
          "SQLEndTran (SQL_ROLLBACK)");

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)drop, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      retcode = SQLEndTran(SQL_HANDLE_DBC, (SQLHANDLE)hdbc, SQL_COMMIT);
      DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
          "SQLEndTran (SQL_COMMIT)");

      /* - Disconnect ---------------------------------------------------- */
      //free sql handles (stmt, dbc, env)
      FREE_SQLHANDLES
}
END_TEST(testSQLEndTran)
