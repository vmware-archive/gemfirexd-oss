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

#define TESTNAME "SQLFetchScroll"

#define TABLE "FETCHSCROLL"

#define MAX_NAME_LEN  1024
#define MAX_STR_LEN 255
#define MAX_ROWS  40
#define ROW_SIZE  8
#define ROW_POS   20

#define TXTCOPY "SQLFetchScroll String "

/* ------------------------------------------------------------------------- */

/* ------------------------------------------------------------------------- */

void DoPrintArray(CHAR fType[MAX_NAME_LEN], SQLINTEGER crow,
    SQLSMALLINT sNumber_rc[ROW_SIZE], CHAR szString_rc[ROW_SIZE][MAX_STR_LEN])
{
  CHAR buffer[MAX_NAME_LEN + 1];
  SQLINTEGER irow;

  printf("\t%s -> Rows fetch : %d.\n", fType, crow);
  printf("\tRow | Number | String\n");
  for (irow = 0; irow < crow; irow++) {
    sprintf(buffer, "\t %d. : ", irow + 1);
    printf("%s", buffer);
    sprintf(buffer, "      %d               %s", sNumber_rc[irow],
        szString_rc[irow]);
    printf("%s", buffer);
    printf("\n");
  }
}
/* ------------------------------------------------------------------------- */

BEGIN_TEST(testSQLFetchScroll)
{
  /* ------------------------------------------------------------------------- */
      CHAR create[MAX_NAME_LEN];
      CHAR drop[MAX_NAME_LEN + 1];
      CHAR insert[MAX_NAME_LEN + 1];
      CHAR select[MAX_NAME_LEN + 1];
      CHAR buffer[MAX_NAME_LEN + 1];

      CHAR szString[MAX_STR_LEN];
      SQLSMALLINT sNumber;
      SQLLEN cbNumber = SQL_NTS;
      SQLLEN cbString = SQL_NTS;

      CHAR szString_rc[ROW_SIZE][MAX_STR_LEN];
      SQLSMALLINT sNumber_rc[ROW_SIZE];
      SQLLEN cbNumber_rc[ROW_SIZE] = { SQL_NTS, SQL_NTS, SQL_NTS, SQL_NTS,
                                       SQL_NTS, SQL_NTS, SQL_NTS, SQL_NTS };
      SQLLEN cbString_rc[ROW_SIZE] = { SQL_NTS, SQL_NTS, SQL_NTS, SQL_NTS,
                                       SQL_NTS, SQL_NTS, SQL_NTS, SQL_NTS };

      SQLINTEGER irow;
      SQLSMALLINT cAbort = 0;
      SQLINTEGER BufferLength;

      /* ------------------------------------------------------------------------- */
      //init sql handles (stmt, dbc, env)
      INIT_SQLHANDLES

      /* ----------------------------------------------------------------- */

      /* --- Create Table --------------------------------------------- */
      strcpy(create, "CREATE TABLE ");
      strcat(create, TABLE);
      strcat(create, " ( NUM SMALLINT, STRING CHAR(30)) ");
      LOGF("Create Stmt = '%s'", create);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)create, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      /* --- Insert Table --------------------------------------------- */
      /* --- 1. ---*/
      strcpy(insert, "INSERT INTO ");
      strcat(insert, TABLE);
      strcat(insert, " VALUES (?, ?) ");
      LOGF("Insert Stmt = '%s'", insert);

      retcode = SQLPrepare(hstmt, (SQLCHAR*)insert, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLPrepare");

      retcode = SQLBindParameter(hstmt, 1, SQL_PARAM_INPUT, SQL_C_SHORT,
          SQL_INTEGER, 0, 0, &sNumber, 0, &cbNumber);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLBindParameter");

      retcode = SQLBindParameter(hstmt, 2, SQL_PARAM_INPUT, SQL_C_CHAR,
          SQL_CHAR, MAX_STR_LEN, 0, szString, MAX_STR_LEN, &cbString);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLBindParameter");

      irow = 1;
      cbString = SQL_NTS;
      cbNumber = SQL_NTS;
      LOGF("Insert Values ->");
      while (irow < MAX_ROWS + 1) {
        sNumber = (SQLSMALLINT)irow;
        strcpy(szString, TXTCOPY);
        sprintf(buffer, "%d.", irow);
        strcat(szString, buffer);

        retcode = SQLExecute(hstmt);
        DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
            "SQLExecute");
        irow++;
      }
      LOGF("Insert into table (%s) -> %d. Values", TABLE, irow - 1);

      retcode = SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLFreeHandle (HSTMT)");

      /* --- Select Table --------------------------------------------- */
      /* --- 1. --- */
      retcode = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLAllocHandle (HSTMT)");

      BufferLength = SQL_IS_UINTEGER;
      retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_CONCURRENCY ,
          (SQLUINTEGER *)SQL_CONCUR_READ_ONLY, BufferLength);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLSetStmtAttr(SQL_ATTR_CONCURRENCY )");

      BufferLength = SQL_IS_UINTEGER;
      retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_CURSOR_TYPE ,
          (SQLUINTEGER *)SQL_CURSOR_KEYSET_DRIVEN, BufferLength);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLSetStmtAttr(SQL_ATTR_CURSOR_TYPE )");

      BufferLength = SQL_IS_UINTEGER;
      retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_CURSOR_SENSITIVITY ,
               (SQLUINTEGER *)SQL_INSENSITIVE, BufferLength);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
               "SQLSetStmtAttr(SQL_ATTR_CURSOR_SENSITIVITY : SQL_INSENSITIVE )");


      /*BufferLength = SQL_IS_UINTEGER;
      retcode = SQLSetStmtAttr(hstmt, SQL_ROWSET_SIZE, (SQLUINTEGER *)ROW_SIZE,
          BufferLength);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLSetStmtAttr(SQL_ROWSET_SIZE)");*/
      //TODO: verify SQLSetStmtAttr(SQL_ROWSET_SIZE) needs to be supported

      /* ***** neues in ODBC 3.0 */
      SQLSetStmtAttr(hstmt, SQL_ATTR_CURSOR_SCROLLABLE, (SQLPOINTER)SQL_SCROLLABLE , 0);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
                "SQLSetStmtAttr(SQL_ATTR_CURSOR_SCROLLABLE)");

      strcpy(select, "SELECT ");
      strcat(select, " NUM, STRING ");
      strcat(select, " FROM ");
      strcat(select, TABLE);
      LOGF("Select Stmt= '%s'", select);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)select, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      if (retcode == SQL_SUCCESS) {
        retcode = SQLBindCol(hstmt, 1, SQL_C_SSHORT, &sNumber_rc,
            sizeof(SQLSMALLINT), cbNumber_rc);
        DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
            "SQLBindCol");
        retcode = SQLBindCol(hstmt, 2, SQL_C_CHAR, szString_rc, MAX_STR_LEN,
            cbString_rc);
        DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
            "SQLBindCol");

        /* ***** SQL_FETCH_NEXT *** ----------------------------------------- */
        while (1) {
          retcode = SQLFetchScroll(hstmt, SQL_FETCH_NEXT, 1);

          if(retcode != SQL_NO_DATA)
            DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
              "SQLFetchScroll");

          if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO) {
            DoPrintArray((CHAR*)"SQL_FETCH_NEXT", 1, sNumber_rc,
                szString_rc);
          }
          else {
            break;
          }
        }

        /* ***** SQL_FETCH_FIRST *** ---------------------------------------- */
        retcode = SQLFetchScroll(hstmt, SQL_FETCH_FIRST, 1);
        DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
            "SQLFetchScroll");

        if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO) {
          DoPrintArray((CHAR*)"SQL_FETCH_FIRST", 1, sNumber_rc,
              szString_rc);
        }
        /* ***** SQL_FETCH_PRIOR *** ---------------------------------------- */
        retcode = SQLFetchScroll(hstmt, SQL_FETCH_NEXT, 1);
        DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
            "SQLFetchScroll");
        retcode = SQLFetchScroll(hstmt, SQL_FETCH_NEXT, 1);
        DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
            "SQLFetchScroll");
        retcode = SQLFetchScroll(hstmt, SQL_FETCH_PRIOR, 1);
        DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
            "SQLFetchScroll");

        if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO) {
          DoPrintArray((CHAR*)"SQL_FETCH_PRIOR", 1, sNumber_rc,
              szString_rc);
        }
        /* ***** SQL_FETCH_LAST  *** ---------------------------------------- */
        retcode = SQLFetchScroll(hstmt, SQL_FETCH_LAST, 1);
        DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
            "SQLFetchScroll");

        if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO) {
          DoPrintArray((CHAR*)"SQL_FETCH_LAST", 1, sNumber_rc,
              szString_rc);
        }
        /* ***** SQL_FETCH_ABSOLUTE *** ------------------------------------- */
        retcode = SQLFetchScroll(hstmt, SQL_FETCH_ABSOLUTE, ROW_POS);
        DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
            "SQLFetchScroll");

        if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO) {
          DoPrintArray((CHAR*)"SQL_FETCH_ABSOLUTE", 1, sNumber_rc,
              szString_rc);
        }
        /* ***** SQL_FETCH_RELATIVE *** ------------------------------------- */
        retcode = SQLFetchScroll(hstmt, SQL_FETCH_RELATIVE, 0);
        DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
            "SQLFetchScroll");

        if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO) {
          DoPrintArray((CHAR*)"SQL_FETCH_RELATIVE", 1, sNumber_rc,
              szString_rc);
        }
      }
      retcode = SQLCloseCursor(hstmt);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLCloseCursor");

      /* --- Drop Table ----------------------------------------------- */
      strcpy(drop, "DROP TABLE ");
      strcat(drop, TABLE);
      LOGF("Drop Stmt= '%s'", drop);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)drop, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      /* - Disconnect ---------------------------------------------------- */
      //free sql handles (stmt, dbc, env)
      FREE_SQLHANDLES
}
END_TEST(testSQLFetchScroll)
