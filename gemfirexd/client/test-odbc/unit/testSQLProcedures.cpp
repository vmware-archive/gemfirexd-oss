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

#define TESTNAME "SQLPROCEDURES"

#define TABLE "TABSQLPROC"
#define USER1 "GFXDODBC"

#define MAX_NAME_LEN 80

#define STR_LEN 128+1
#define REM_LEN 254+1

#define NULL_VALUE "<NULL>"

//*-------------------------------------------------------------------------

/* ------------------------------------------------------------------------- */
RETCODE lstProceduresInfo(SQLHENV henv, SQLHDBC hdbc, SQLHSTMT hstmt)
{
  RETCODE retcode;
  SQLSMALLINT sErr = 0;
  /* ------------------------------------------------------------------------- */
  /* Declare storage locations for result set data */
  CHAR szProcQualifier[STR_LEN], szProcOwner[STR_LEN], szProcName[STR_LEN],
      szRemarks[STR_LEN];
  SQLSMALLINT NumInputParams, NumOutputParams, NumResultParams, ProcedureType;

  CHAR szProcedureType[STR_LEN], szNumInputParams[STR_LEN],
      szNumOutputParams[STR_LEN], szNumResultParams[STR_LEN];

  /* Declare storage locations for bytes available to return */
  SQLLEN cbProcQualifier, cbProcOwner, cbProcName, cbNumInputParams,
      cbNumOutputParams, cbNumResultParams, cbRemarks, cbProcedureType;

  SQLSMALLINT count = 0;
  /* ---------------------------------------------------------------------har- */
  /* Bind columns in result set to storage locations */
  SQLBindCol(hstmt, 1, SQL_C_CHAR, szProcQualifier, STR_LEN, &cbProcQualifier);
  SQLBindCol(hstmt, 2, SQL_C_CHAR, szProcOwner, STR_LEN, &cbProcOwner);
  SQLBindCol(hstmt, 3, SQL_C_CHAR, szProcName, STR_LEN, &cbProcName);
  SQLBindCol(hstmt, 4, SQL_C_SHORT, &NumInputParams, 0, &cbNumInputParams);
  SQLBindCol(hstmt, 5, SQL_C_SHORT, &NumOutputParams, 0, &cbNumOutputParams);
  SQLBindCol(hstmt, 6, SQL_C_SHORT, &NumResultParams, 0, &cbNumResultParams);
  SQLBindCol(hstmt, 7, SQL_C_CHAR, szRemarks, STR_LEN, &cbRemarks);
  SQLBindCol(hstmt, 8, SQL_C_SHORT, &ProcedureType, 0, &cbProcedureType);

  retcode = lst_ColumnNames(henv, hdbc, hstmt, 8);

  while (TRUE) {
    count++;

    retcode = SQLFetch(hstmt);
    /* DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,"SQLFetch");*/

    if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO) {
      /* Process fetched data */
      if (cbProcQualifier == SQL_NULL_DATA) strcpy(szProcQualifier,
          NULL_VALUE);
      if (cbProcOwner == SQL_NULL_DATA) strcpy(szProcOwner, NULL_VALUE);
      if (cbProcName == SQL_NULL_DATA) strcpy(szProcName, NULL_VALUE);
      if (cbNumInputParams == SQL_NULL_DATA)
        strcpy(szNumInputParams, NULL_VALUE);
      else
        sprintf(szNumInputParams, "%d", NumInputParams);
      if (cbNumOutputParams == SQL_NULL_DATA)
        strcpy(szNumOutputParams, NULL_VALUE);
      else
        sprintf(szNumOutputParams, "%d", NumOutputParams);
      if (cbNumResultParams == SQL_NULL_DATA)
        strcpy(szNumResultParams, NULL_VALUE);
      else
        sprintf(szNumResultParams, "%d", NumResultParams);
      if (cbRemarks == SQL_NULL_DATA) strcpy(szRemarks, NULL_VALUE);
      /*
       if (cbProcedureType == SQL_NULL_DATA) ProcedureType=NULL;
       */
      switch (ProcedureType) {
        case SQL_PT_UNKNOWN:
          strcpy(szProcedureType, "SQL_PT_UNKONWN");
          break;
        case SQL_PT_PROCEDURE:
          strcpy(szProcedureType, "SQL_PT_PROCEDURE");
          break;
        case SQL_PT_FUNCTION:
          strcpy(szProcedureType, "SQL_PT_FUNCTION");
          break;
        default:
          strcpy(szProcedureType, NULL_VALUE);
          break;
      }

      printf("\tProcedure %d : '%s','%s','%s','%s','%s','%s','%s','%s'\r\n",
          count, szProcQualifier, szProcOwner, szProcName, szNumInputParams,
          szNumOutputParams, szNumResultParams, szRemarks, szProcedureType);

    }
    else {
      /* DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,"SQLFetch"); */
      break;
    }
  }
  return TRUE;
}
/* ------------------------------------------------------------------------- */

BEGIN_TEST(testSQLProcedures)
{
  /* ------------------------------------------------------------------------- */

  //init sql handles (stmt, dbc, env)
      INIT_SQLHANDLES

      /* ------------------------------------------------------------------------- */
      /* ***************************************************************** */
      /* *** I. SQLProcedures ******************************************** */
      /* ***************************************************************** */
      printf("\tI.) SQLProcedures -> all procedures'\r\n");

      /* Get all the procedures. */

       retcode = SQLProcedures(hstmt,
       NULL, 0,
       (SQLCHAR*)"SYS", SQL_NTS,
       (SQLCHAR*)"%", SQL_NTS);
       DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,"SQLProcedures");

       if (retcode == SQL_SUCCESS) lstProceduresInfo(henv, hdbc, hstmt);

      retcode = SQLFreeStmt(hstmt, SQL_CLOSE);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLFreeStmt");

      /* ***************************************************************** */
      /* *** II. SQLProcedures ******************************************* */
      /* ***************************************************************** */
      printf("\tII.) SQLProcedures -> all procedures for USER: "USER1"\r\n");

      /* Get all the procedures. */

       retcode = SQLProcedures(hstmt,
       NULL, 0,
       (SQLCHAR*)USER1, SQL_NTS,
       (SQLCHAR*)"%", SQL_NTS);
       DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,"SQLProcedures");

       if (retcode == SQL_SUCCESS) lstProceduresInfo(henv, hdbc, hstmt);

      retcode = SQLFreeStmt(hstmt, SQL_CLOSE);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLFreeStmt");

      /* ---------------------------------------------------------------------har- */
      //free sql handles (stmt, dbc, env)
      FREE_SQLHANDLES
}
END_TEST(testSQLProcedures)
