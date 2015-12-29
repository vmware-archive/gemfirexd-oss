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

#define TESTNAME "SQLPROCEDURECOLUMNS"

#define TABLE "TABSQLPROCCOL"
#define USER1 "DOMAIN"
#define PROCN "SEL_TEST"

#define MAX_NAME_LEN 80

#define STR_LEN 128+1
#define REM_LEN 254+1

#define NULL_VALUE "<NULL>"

//*-------------------------------------------------------------------------

/* ------------------------------------------------------------------------- */
RETCODE lstProcedureColumnsInfo(SQLHENV henv, SQLHDBC hdbc, HSTMT hstmt)
{
  RETCODE retcode;
  SQLSMALLINT sErr = 0;
  /* ------------------------------------------------------------------------- */
  /* Declare storage locations for result set data */
  CHAR szProcQualifier[STR_LEN], szProcOwner[STR_LEN], szProcName[STR_LEN],
      szColumnName[STR_LEN], szTypeName[STR_LEN], szRemarks[STR_LEN];
  SQLSMALLINT ColumnType, DataType, Scale, Radix, Nullable;
  SQLINTEGER Precision, Length;

  CHAR szColumnType[STR_LEN], szDataType[STR_LEN], szPrecision[STR_LEN],
      szLength[STR_LEN], szScale[STR_LEN], szRadix[STR_LEN],
      szNullable[STR_LEN];

  /* Declare storage locations for bytes available to return */
  SQLLEN cbProcQualifier, cbProcOwner, cbProcName, cbColumnName,
      cbColumnType, cbDataType, cbTypeName, cbPrecision, cbLength, cbScale,
      cbRadix, cbNullable, cbRemarks;

  SQLSMALLINT count = 0;
  /* ---------------------------------------------------------------------har- */
  /* Bind columns in result set to storage locations */
  SQLBindCol(hstmt, 1, SQL_C_CHAR, szProcQualifier, STR_LEN, &cbProcQualifier);
  SQLBindCol(hstmt, 2, SQL_C_CHAR, szProcOwner, STR_LEN, &cbProcOwner);
  SQLBindCol(hstmt, 3, SQL_C_CHAR, szProcName, STR_LEN, &cbProcName);
  SQLBindCol(hstmt, 4, SQL_C_CHAR, szColumnName, STR_LEN, &cbColumnName);
  SQLBindCol(hstmt, 5, SQL_C_SHORT, &ColumnType, 0, &cbColumnType);
  SQLBindCol(hstmt, 6, SQL_C_SHORT, &DataType, 0, &cbDataType);
  SQLBindCol(hstmt, 7, SQL_C_CHAR, szTypeName, STR_LEN, &cbTypeName);
  SQLBindCol(hstmt, 8, SQL_C_LONG, &Precision, 0, &cbPrecision);
  SQLBindCol(hstmt, 9, SQL_C_LONG, &Length, 0, &cbLength);
  SQLBindCol(hstmt, 10, SQL_C_SHORT, &Scale, STR_LEN, &cbScale);
  SQLBindCol(hstmt, 11, SQL_C_SHORT, &Radix, STR_LEN, &cbRadix);
  SQLBindCol(hstmt, 12, SQL_C_SHORT, &Nullable, STR_LEN, &cbNullable);
  SQLBindCol(hstmt, 13, SQL_C_CHAR, szRemarks, STR_LEN, &cbRemarks);

  retcode = lst_ColumnNames(henv, hdbc, hstmt, 13);

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
      if (cbColumnName == SQL_NULL_DATA) strcpy(szColumnName, NULL_VALUE);
      if (cbDataType == SQL_NULL_DATA)
        strcpy(szDataType, NULL_VALUE);
      else
        sprintf(szDataType, "%d", DataType);
      if (cbTypeName == SQL_NULL_DATA) strcpy(szTypeName, NULL_VALUE);
      if (cbPrecision == SQL_NULL_DATA)
        strcpy(szPrecision, NULL_VALUE);
      else
        sprintf(szPrecision, "%d", Precision);
      if (cbLength == SQL_NULL_DATA)
        strcpy(szLength, NULL_VALUE);
      else
        sprintf(szLength, "%d", Length);
      if (cbScale == SQL_NULL_DATA)
        strcpy(szScale, NULL_VALUE);
      else
        sprintf(szScale, "%d", Scale);
      if (cbRadix == SQL_NULL_DATA)
        strcpy(szRadix, NULL_VALUE);
      else
        sprintf(szRadix, "%d", Radix);
      if (cbRemarks == SQL_NULL_DATA) strcpy(szRemarks, NULL_VALUE);
      /*
       if (cbColumnType == SQL_NULL_DATA) ColumnType=NULL;
       if (cbNullable == SQL_NULL_DATA) Nullable=NULL;
       */
      switch (ColumnType) {
        case SQL_PARAM_TYPE_UNKNOWN:
          strcpy(szColumnType, "SQL_PARAM_TYPE_UNKONWN");
          break;
        case SQL_PARAM_INPUT:
          strcpy(szColumnType, "SQL_PARAM_INPUT");
          break;
        case SQL_PARAM_INPUT_OUTPUT:
          strcpy(szColumnType, "SQL_PARAN_INPUT_OUTPUT");
          break;
        case SQL_PARAM_OUTPUT:
          strcpy(szColumnType, "SQL_PARAM_OUTPUT");
          break;
        case SQL_RETURN_VALUE:
          strcpy(szColumnType, "SQL_RETURN_VALUE");
          break;
        case SQL_RESULT_COL:
          strcpy(szColumnType, "SQL_RESULT_COL");
          break;
        default:
          strcpy(szColumnType, NULL_VALUE);
          break;
      }
      switch (Nullable) {
        case SQL_NO_NULLS:
          strcpy(szNullable, "SQL_NO_NULLS");
          break;
        case SQL_NULLABLE:
          strcpy(szNullable, "SQL_NULLABLE");
          break;
        case SQL_NULLABLE_UNKNOWN:
          strcpy(szNullable, "SQL_NULLABLE_UNKNOWN");
          break;
        default:
          strcpy(szNullable, NULL_VALUE);
          break;
      }
      printf(
          "\tProcedure %d : '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s'\r\n",
          count, szProcQualifier, szProcOwner, szProcName, szColumnName,
          szColumnType, szDataType, szTypeName, szPrecision, szLength, szScale,
          szRadix, szNullable, szRemarks);
    }
    else {
      /* DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,"GFXDetch"); */
      break;
    }
  }
  return TRUE;
}
/* ------------------------------------------------------------------------- */

BEGIN_TEST(testSQLProcedureColumns)
{
  /* ------------------------------------------------------------------------- */
  //init sql handles (stmt, dbc, env)
      INIT_SQLHANDLES

      /* ------------------------------------------------------------------------- */
      /* ***************************************************************** */
      /* *** I. SQLProcedureColumns ************************************** */
      /* ***************************************************************** */
      printf("\tI.) SQLProcedureColumns -> '\r\n");

      /* Get all the procedures columns. */
      retcode = SQLProcedureColumns(hstmt, NULL, 0, /* Proc qualifier */
      (SQLCHAR*)"%", SQL_NTS, /* Proc owner     */
      (SQLCHAR*)"%", SQL_NTS, /* Proc name      */
      (SQLCHAR*)"%", SQL_NTS); /* Column name    */
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLProcedureColumns");

      if (retcode == SQL_SUCCESS) lstProcedureColumnsInfo(henv, hdbc, hstmt);

      retcode = SQLFreeStmt(hstmt, SQL_CLOSE);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLFreeStmt");

      /* ***************************************************************** */
      /* *** II. SQLProcedureColumns ************************************* */
      /* ***************************************************************** */
      printf(
          "\tII.) SQLProcedureColumns -> for USER: "USER1" - PROCEDURE: "PROCN"\r\n");

      /* Get all the procedure columns. */
      retcode = SQLProcedureColumns(hstmt, NULL, 0, /* Proc qualifier */
      (SQLCHAR*)USER1, SQL_NTS, /* Proc owner     */
      (SQLCHAR*)PROCN, SQL_NTS, /* Proc name      */
      (SQLCHAR*)"%", SQL_NTS); /* Columns name   */

      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLProcedureColumns");

      if (retcode == SQL_SUCCESS) lstProcedureColumnsInfo(henv, hdbc, hstmt);

      retcode = SQLFreeStmt(hstmt, SQL_CLOSE);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLFreeStmt");

      /* ---------------------------------------------------------------------har- */
      //free sql handles (stmt, dbc, env)
      FREE_SQLHANDLES

}
END_TEST(testSQLProcedureColumns)
