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
#define TESTNAME "SQLPutData"
#define TABLE "SPUTDATA"

#define MAX_NAME_LEN 1024

#define PARAM1 1
#define PARAM2 2
#define PARAM3 3
//*-------------------------------------------------------------------------

BEGIN_TEST(testSQLPutData)
{
  /* ------------------------------------------------------------------------- */
      CHAR create[MAX_NAME_LEN + MAX_NAME_LEN + 1];
      CHAR select[MAX_NAME_LEN + MAX_NAME_LEN + 1];
      CHAR insert[MAX_NAME_LEN + MAX_NAME_LEN + 1];
      CHAR drop[MAX_NAME_LEN + 1];
      CHAR tabname[MAX_NAME_LEN];

      SQLLEN cbValue;
      SQLLEN pcbValue;
      PTR pToken;

      CHAR rc_sName[MAX_NAME_LEN + 1];
      SQL_DATE_STRUCT dsDate, rc_dsDate;
      SQLSMALLINT sAge, rc_sAge, cParam = 0;
      /* ---------------------------------------------------------------------har- */
      //init sql handles (stmt, dbc, env)
      INIT_SQLHANDLES

      /* ---------------------------------------------------------------------har- */

      /* --- Create Table --------------------------------------------- */
      strcpy(tabname, TABLE);
      strcpy(create, "CREATE TABLE ");
      strcat(create, tabname);
      strcat(create, " (NAME CHAR(30), AGE SMALLINT, BDATE DATE )");
      printf("\tCreate Stmt = '%s'\r\n", create);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)create, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      /* --- Insert Table --------------------------------------------- */
      /* --- 1. ---*/
      strcpy(insert, "INSERT INTO ");
      strcat(insert, tabname);
      strcat(insert, " (NAME, AGE, BDATE ) ");
      strcat(insert, " VALUES (?, ?, ?)");
      printf("\tInsert Stmt = '%s'\r\n", insert);

      retcode = SQLPrepare(hstmt, (SQLCHAR*)insert, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLPrepare");

      /* ----------------------------------------------------------------- */
      /* Specify data types and declare params as SQL_DATA_AT_EXEC for     */
      /* Name, Age, Date                 */

      cbValue = SQL_DATA_AT_EXEC;

      retcode = SQLBindParameter(hstmt, 1, SQL_PARAM_INPUT, SQL_C_CHAR,
          SQL_CHAR, MAX_NAME_LEN, 0, (SQLSMALLINT *)PARAM1, 0, &cbValue);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLBindParameter");

      retcode = SQLBindParameter(hstmt, 2, SQL_PARAM_INPUT, SQL_C_SHORT,
          SQL_SMALLINT, 0, 0, (SQLSMALLINT *)PARAM2, 0, &cbValue);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLBindParameter");

      retcode = SQLBindParameter(hstmt, 3, SQL_PARAM_INPUT, SQL_C_TYPE_DATE,
          SQL_TYPE_DATE, 0, 0, (SQLSMALLINT *)PARAM3, 0, &cbValue);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLBindParameter");

      retcode = SQLExecute(hstmt);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_NEED_DATA, retcode,
          "SQLExecute");

      if (retcode == SQL_NEED_DATA) {
        /* Call SQLParamData to begin SQL_DATA_AT_EXEC parameter     */
        /* processing and retrieve pToken for first SQL_DATA_AT_EXEC */
        /* parameter */

        retcode = SQLParamData(hstmt, &pToken);
        DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_NEED_DATA, retcode,
            "SQLParamData");

        while (retcode == SQL_NEED_DATA) {
          cParam++;
          switch (cParam) {
            case 1: /* Send Name parameter data */
              retcode = SQLPutData(hstmt, (SQLPOINTER)"Smith, ", SQL_NTS);
              DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
                  "SQLPutData");
              retcode = SQLPutData(hstmt, (SQLPOINTER)"John D.", SQL_NTS);
              DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
                  "SQLPutData");
              break;
            case 2: /* Send Age parameter data */
              sAge = 40;
              retcode = SQLPutData(hstmt, &sAge, sizeof(SQLSMALLINT));
              DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
                  "SQLPutData");
              break;
            case 3: /* Send Date parameter data. */
              dsDate.year = 1954;
              dsDate.month = 12;
              dsDate.day = 12;
              retcode = SQLPutData(hstmt, &dsDate, sizeof(SQL_DATE_STRUCT));
              DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
                  "SQLPutData");
              break;
          }

          /* Call SQParamData to declare all data has been sent for    */
          /* this SQL_DATA_AT_EXEC parameter, retrieve pToken for next */
          /* SQL_DATA_AT_EXEC parameter (if one exists), and get return*/
          /* code to determine if another SQL_DATA_AT_EXEC paramter exits */

          retcode = SQLParamData(hstmt, &pToken);
        }
        DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
            "SQLParamData");

      }

      retcode = SQLFreeStmt(hstmt, SQL_RESET_PARAMS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
              "SQLFreeStmt");

      /* --- Select Table --------------------------------------------- */
      /* --- 1. --- */
      strcpy(select, "SELECT ");
      strcat(select, "NAME, AGE, BDATE ");
      strcat(select, " FROM ");
      strcat(select, tabname);
      printf("\tSelect Stmt= '%s'\r\n", select);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)select, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      pcbValue = SQL_NTS;
      retcode = SQLBindCol(hstmt, 1, SQL_C_CHAR, rc_sName, MAX_NAME_LEN,
          &pcbValue);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLBindCol");

      pcbValue = SQL_NTS;
      retcode = SQLBindCol(hstmt, 2, SQL_C_SHORT, &rc_sAge, 0, &pcbValue);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLBindCol");

      pcbValue = SQL_NTS;
      retcode = SQLBindCol(hstmt, 3, SQL_C_TYPE_DATE, &rc_dsDate, 0, &pcbValue);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLBindCol");

      retcode = SQLFetch(hstmt);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLFetch");

      LOGF(
          "Select Value -->\n\tName = '%s'\n\tAge = '%d'\n\tBirthday = '%d-%d-%d'\r\n",
          rc_sName, rc_sAge, rc_dsDate.year, rc_dsDate.month, rc_dsDate.day);

      retcode = SQLFreeStmt(hstmt, SQL_CLOSE);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLFreeStmt");

      /* --- Drop Table ----------------------------------------------- */
      strcpy(drop, "DROP TABLE ");
      strcat(drop, tabname);
      printf("\tDrop Stmt= '%s'\r\n", drop);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)drop, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");
      /* ---------------------------------------------------------------------har- */
      //free sql handles (stmt, dbc, env)
      FREE_SQLHANDLES
}
END_TEST(testSQLPutData)
