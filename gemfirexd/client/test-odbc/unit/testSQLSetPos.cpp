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
#define TESTNAME "SQLSetPos"
#define TABLE "SETPOS"

#define MAX_NAME_LEN  1024
#define MAX_STR_LEN 255
#define MAX_ROWS  40
#define ROW_SIZE  8
#define SETPOS1   2
#define SETPOS2   4

#define TXTCOPY "SQLSetPos String "

//*-------------------------------------------------------------------------

/* ------------------------------------------------------------------------- */

void DoPrintArrayPos(CHAR fType[MAX_NAME_LEN], UDWORD crow,
    UWORD rgfRowStatus[ROW_SIZE], SWORD sNumber_rc[ROW_SIZE],
    CHAR szString_rc[ROW_SIZE][MAX_STR_LEN])
{
  CHAR buffer[MAX_NAME_LEN + 1];
  UDWORD irow;

  printf("\t%s -> Rows fetch : %d.\n", fType, crow);
  printf("\tRow | RowStatus | Number | String\n");
  for (irow = 0; irow < crow; irow++) {
    sprintf(buffer, "\t %d.          %d", irow + 1, rgfRowStatus[irow]);
    printf("%s", buffer);
    if (rgfRowStatus[irow] != SQL_ROW_DELETED
        && rgfRowStatus[irow] != SQL_ROW_ERROR) {
      sprintf(buffer, "                      %d              %s",
          sNumber_rc[irow], szString_rc[irow]);
      printf("%s", buffer);
    }
    printf("\r\n");
  }
}
/* ------------------------------------------------------------------------- */

BEGIN_TEST(testSQLSetPos)
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

      SQLUSMALLINT rgfRowStatus[ROW_SIZE];

      SQLUINTEGER irow, crow = 1;
      /* ---------------------------------------------------------------------har- */

      //init sql handles (stmt, dbc, env)
      INIT_SQLHANDLES

      /* ---------------------------------------------------------------------har- */

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
            "SQLExecute?");
        irow++;
      }
      LOGF("Insert into table (%s) -> %d. Values", TABLE, irow - 1);

      retcode = SQLFreeStmt(hstmt, SQL_DROP);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLFreeStmt");
      /* --- Select Table --------------------------------------------- */
      /* --- 1. --- */
      retcode = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLAllocStmt");

      retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_CURSOR_SENSITIVITY,
           (SQLPOINTER)SQL_INSENSITIVE, 0);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLSetStmtAttr");

      /*retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_CONCURRENCY ,
                 (SQLPOINTER)SQL_CONCUR_LOCK, NULL);
            DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
                "SQLSetStmtAttr");*/

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
        retcode = SQLFetchScroll(hstmt, SQL_FETCH_NEXT, 1);
        DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
            "SQLFetchScroll");

        if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO) {
          DoPrintArrayPos((CHAR*)"SQL_FETCH_NEXT", crow, rgfRowStatus, sNumber_rc,
              szString_rc);
        }
        /* ***** SQLSetPos (SQL_POSITION) *** ------------------------------- */
        retcode = SQLSetPos(hstmt, SETPOS1, SQL_POSITION, SQL_LOCK_NO_CHANGE);
        DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
            "SQLSetPos (SQL_POSITION)");

        if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO) {
          printf("-> Set position to row : %d", SETPOS1);
          DoPrintArrayPos((CHAR*)"SQL_POSITION", crow, rgfRowStatus, sNumber_rc,
              szString_rc);
        }

        //updatable and cursor sensitive are not supported in jdbc
        /* ***** SQLSetPos (SQL_REFRESH) *** -------------------------------- */
        /*retcode = SQLSetPos(hstmt, SETPOS2, SQL_REFRESH, SQL_LOCK_NO_CHANGE);
        DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
            "SQLSetPos (SQL_REFRESH)");

        if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO) {
          printf("-> Set position to row : %d", SETPOS2);
          DoPrintArrayPos((CHAR*)"SQL_REFRESH", crow, rgfRowStatus, sNumber_rc,
              szString_rc);
        }*/
        /* ------------------------------------------------------------------ */
      }
      retcode = SQLFreeStmt(hstmt, SQL_CLOSE);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLFreeStmt");

      /* --- Drop Table ----------------------------------------------- */
      strcpy(drop, "DROP TABLE ");
      strcat(drop, TABLE);
      LOGF("Drop Stmt= '%s'", drop);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)drop, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      /* ---------------------------------------------------------------------har- */
      //free sql handles (stmt, dbc, env)
      FREE_SQLHANDLES
    }END_TEST(testSQLSetPos)
