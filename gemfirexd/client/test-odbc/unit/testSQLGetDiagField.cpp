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

#define TESTNAME "SQLGetDiagField"
#define TABLE    "TABERRORFIELD"
#define SQLSTMT1 "SELECT * FROM DUAL"

#define MAX_NAME_LEN 50
#define ERROR_TEXT_LEN 511
#define MAX_LONG     120

//*-------------------------------------------------------------------------

BEGIN_TEST(testSQLGetDiagField)
{
  /* ------------------------------------------------------------------------- */
      CHAR create[MAX_NAME_LEN + MAX_NAME_LEN + 1];
      CHAR drop[MAX_NAME_LEN + 1];
      CHAR tabname[MAX_NAME_LEN];
      /*
       SQLCHAR     Sqlstate[MAX_NAME_LEN];
       SQLINTEGER    NativeError;
       SQLCHAR     MessageText[ERROR_TEXT_LEN+1];
       SQLSMALLINT     TextLength;
       */
      SQLSMALLINT BufferLength = ERROR_TEXT_LEN;
      SQLSMALLINT StringLength;
      SQLCHAR szCharPtr[MAX_NAME_LEN];
      SQLINTEGER szIntPtr;
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
          /* ***** SQL_DIAG_SQLSTATE ---------- */
          BufferLength = ERROR_TEXT_LEN;
          retcode = SQLGetDiagField(SQL_HANDLE_STMT, hstmt, RecNum,
              SQL_DIAG_SQLSTATE, &szCharPtr, BufferLength, &StringLength);
          LOGF("\t SQLGetDiagField -> retcode: %d", retcode);

          if (retcode == SQL_SUCCESS) {
            /* ***** SQL_DIAG_SQLSTATE ---------- */
            sprintf(buffer, "SQL_DIAG_SQLSTATE  : (%d) %s", StringLength,
                szCharPtr);
            LOGF("\t\t -> %s", buffer);

            /* ***** SQL_DIAG_NATIVE ------------ */
            BufferLength = 0;
            retcode = SQLGetDiagField(SQL_HANDLE_STMT, hstmt, RecNum,
                SQL_DIAG_NATIVE, &szIntPtr, BufferLength, &StringLength);
            LOGF("\t SQLGetDiagField -> retcode: %d", retcode);
            sprintf(buffer, "SQL_DIAG_NATIVE    : (%d) %ld", StringLength,
                szIntPtr);
            LOGF("\t\t -> %s", buffer);

            /* ***** SQL_DIAG_MESSAGE TEXT ---------- */
            BufferLength = ERROR_TEXT_LEN;
            retcode = SQLGetDiagField(SQL_HANDLE_STMT, hstmt, RecNum,
                SQL_DIAG_MESSAGE_TEXT, &szCharPtr, BufferLength,
                &StringLength);
            LOGF("\t SQLGetDiagField -> retcode: %d", retcode);
            sprintf(buffer, "SQL_DIAG_MESSAGE_TEXT  : (%d) %s", StringLength,
                szCharPtr);
            LOGF("\t\t -> %s", buffer);

            /* ***** SQL_DIAG_CLASS_ORIGIN ---------- */
            BufferLength = ERROR_TEXT_LEN;
            retcode = SQLGetDiagField(SQL_HANDLE_STMT, hstmt, RecNum,
                SQL_DIAG_CLASS_ORIGIN, &szCharPtr, BufferLength,
                &StringLength);
            LOGF("\t SQLGetDiagField -> retcode: %d", retcode);
            sprintf(buffer, "SQL_DIAG_CLASS_ORIGIN  : (%d) %s", StringLength,
                szCharPtr);
            LOGF("\t\t -> %s", buffer);

            /* ***** SQL_DIAG_COLUMN_NUMBER ------------ */
            BufferLength = 0;
            retcode = SQLGetDiagField(SQL_HANDLE_STMT, hstmt, RecNum,
                SQL_DIAG_COLUMN_NUMBER, &szIntPtr, BufferLength,
                &StringLength);
            LOGF("\t SQLGetDiagField -> retcode: %d", retcode);
            sprintf(buffer, "SQL_DIAG_COLUMN_NUMBER : (%d) ", StringLength);
            switch (szIntPtr) {
              case (SQL_COLUMN_NUMBER_UNKNOWN):
                strcat(buffer, "SQL_COLUMN_NUMBER_UNKNOWN");
                break;
              case (SQL_NO_COLUMN_NUMBER):
                strcat(buffer, "SQL_NO_COLUMN_NUMBER");
                break;
              default:
                strcat(buffer, "?????");
            }
            LOGF("\t\t -> %s", buffer);

            /* ***** SQL_DIAG_CONNECTION_NAME ---------- */
            BufferLength = ERROR_TEXT_LEN;
            retcode = SQLGetDiagField(SQL_HANDLE_STMT, hstmt, RecNum,
                SQL_DIAG_CONNECTION_NAME, &szCharPtr, BufferLength,
                &StringLength);
            LOGF("\t SQLGetDiagField -> retcode: %d", retcode);
            sprintf(buffer, "SQL_DIAG_CONNECTION_NAME: (%d) %s", StringLength,
                szCharPtr);
            LOGF("\t\t -> %s", buffer);

            /* ***** SQL_DIAG_ROW_NUMBER ------------ */
            BufferLength = 0;
            retcode = SQLGetDiagField(SQL_HANDLE_STMT, hstmt, RecNum,
                SQL_DIAG_ROW_NUMBER, &szIntPtr, BufferLength, &StringLength);
            LOGF("\t SQLGetDiagField -> retcode: %d", retcode);
            sprintf(buffer, "SQL_DIAG_ROW_NUMBER  : (%d) ", StringLength);
            switch (szIntPtr) {
              case (SQL_ROW_NUMBER_UNKNOWN):
                strcat(buffer, "SQL_ROW_NUMBER_UNKNOWN");
                break;
              case (SQL_NO_ROW_NUMBER):
                strcat(buffer, "SQL_NO_ROW_NUMBER");
                break;
              default:
                strcat(buffer, "?????");
            }
            LOGF("\t\t -> %s", buffer);

            /* ***** SQL_DIAG_SERVER_NAME ---------- */
            BufferLength = ERROR_TEXT_LEN;
            retcode = SQLGetDiagField(SQL_HANDLE_STMT, hstmt, RecNum,
                SQL_DIAG_SERVER_NAME, &szCharPtr, BufferLength, &StringLength);
            LOGF("\t SQLGetDiagField -> retcode: %d", retcode);
            sprintf(buffer, "SQL_DIAG_SERVER_NAME : (%d) %s", StringLength,
                szCharPtr);
            LOGF("\t\t -> %s", buffer);

            /* ***** SQL_DIAG_SUBCLASS_ORIGIN ---------- */
            BufferLength = ERROR_TEXT_LEN;
            retcode = SQLGetDiagField(SQL_HANDLE_STMT, hstmt, RecNum,
                SQL_DIAG_SUBCLASS_ORIGIN, &szCharPtr, BufferLength,
                &StringLength);
            LOGF("\t SQLGetDiagField -> retcode: %d", retcode);
            sprintf(buffer, "SQL_DIAG_SUBCLASS_ORIGIN: (%d) %s", StringLength,
                szCharPtr);
            LOGF("\t\t -> %s", buffer);

            /* ***** SQL_DIAG_CURSOR_ROW_COUNT ---------- */
            BufferLength = 0;
            retcode = SQLGetDiagField(SQL_HANDLE_STMT, hstmt, RecNum,
                SQL_DIAG_CURSOR_ROW_COUNT, &szIntPtr, BufferLength,
                &StringLength);
            LOGF("\t SQLGetDiagField -> retcode: %d", retcode);
            sprintf(buffer, "SQL_DIAG_CURSOR_ROW_COUNT: (%d) %d", StringLength,
                szIntPtr);
            LOGF("\t\t -> %s", buffer);

            /* ***** SQL_DIAG_DYNAMIC_FUNCTION ---------- */
            BufferLength = ERROR_TEXT_LEN;
            retcode = SQLGetDiagField(SQL_HANDLE_STMT, hstmt, RecNum,
                SQL_DIAG_DYNAMIC_FUNCTION, &szCharPtr, BufferLength,
                &StringLength);
            LOGF("\t SQLGetDiagField -> retcode: %d", retcode);
            sprintf(buffer, "SQL_DIAG_DYNAMIC_FUNCTION: ");
            LOGF("\t\t -> %s", buffer);

            /* ***** SQL_DIAG_DYNAMIC_FUNCTION_CODE ---------- */
            BufferLength = 0;
            retcode = SQLGetDiagField(SQL_HANDLE_STMT, hstmt, RecNum,
                SQL_DIAG_DYNAMIC_FUNCTION_CODE, &szIntPtr, BufferLength,
                &StringLength);
            LOGF("\t SQLGetDiagField -> retcode: %d", retcode);
            sprintf(buffer, "SQL_DIAG_DYNAMIC_FUNCTION_CODE:");
            LOGF("\t\t -> %s", buffer);

            /* ***** SQL_DIAG_NUMBER ---------- */
            BufferLength = 0;
            retcode = SQLGetDiagField(SQL_HANDLE_STMT, hstmt, RecNum,
                SQL_DIAG_NUMBER, &szIntPtr, BufferLength, &StringLength);
            LOGF("\t SQLGetDiagField -> retcode: %d", retcode);
            sprintf(buffer, "SQL_DIAG_NUMBER    : (%d) %ld", StringLength,
                szIntPtr);
            LOGF("\t\t -> %s", buffer);

            /* ***** SQL_DIAG_RETURNCODE ---------- */
            BufferLength = 0;
            retcode = SQLGetDiagField(SQL_HANDLE_STMT, hstmt, RecNum,
                SQL_DIAG_NUMBER, &szIntPtr, BufferLength, &StringLength);
            LOGF("\t SQLGetDiagField -> retcode: %d", retcode);
            sprintf(buffer, "SQL_DIAG_RETURNCODE    : (%d) %ld", StringLength,
                szIntPtr);
            LOGF("\t\t -> %s", buffer);

            /* ***** SQL_DIAG_ROW_COUNT ---------- */
            BufferLength = 0;
            retcode = SQLGetDiagField(SQL_HANDLE_STMT, hstmt, RecNum,
                SQL_DIAG_ROW_COUNT, &szIntPtr, BufferLength, &StringLength);
            LOGF("\t SQLGetDiagField -> retcode: %d", retcode);
            sprintf(buffer, "SQL_DIAG_ROW_COUNT   : (%d) %d", StringLength,
                szIntPtr);
            LOGF("\t\t -> %s", buffer);
          }

          if (retcode == SQL_ERROR)
            break;
          else
            RecNum++;
        }
      }

      /* --- Drop Table ------------------------------------------------- */
      strcpy(drop, "DROP TABLE "TABLE);
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
END_TEST(testSQLGetDiagField)
