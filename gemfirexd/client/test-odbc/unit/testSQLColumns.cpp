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

#define TESTNAME "SQLColumns"
#define TABLE "SCOLUMNS"

#define MAX_NAME_LEN 50

#define STR_LEN 128+1
#define REM_LEN 254+1

#define NULL_VALUE "<NULL>"

//*-------------------------------------------------------------------------

BEGIN_TEST(testSQLColumns)
{
  /* ------------------------------------------------------------------------- */
      CHAR create[MAX_NAME_LEN + MAX_NAME_LEN + MAX_NAME_LEN + MAX_NAME_LEN + 1];
      CHAR drop[MAX_NAME_LEN + 1];
      CHAR tabname[MAX_NAME_LEN];
      CHAR buffer[MAX_NAME_LEN];

      /* Declare storage locations for result set data */

      CHAR szQualifier[STR_LEN], szOwner[STR_LEN];
      CHAR szTableName[STR_LEN], szColName[STR_LEN];
      CHAR szTypeName[STR_LEN], szRemarks[REM_LEN];
      SQLINTEGER Precision, Length;
      SQLSMALLINT DataType, Scale, Radix, Nullable;

      /* Declare storage locations for bytes available to return */

      SQLLEN cbQualifier, cbOwner, cbTableName, cbColName;
      SQLLEN cbTypeName, cbRemarks, cbDataType, cbPrecision;

      SQLLEN cbLength, cbScale, cbRadix, cbNullable;

      SQLSMALLINT count = 0;
      /* ---------------------------------------------------------------------har- */
      //init sql handles (stmt, dbc, env)
      INIT_SQLHANDLES

      /* ------------------------------------------------------------------------- */
      /* --- Create Table --------------------------------------------- */
      strcpy(tabname, TABLE);
      strcpy(create, "CREATE TABLE ");
      strcat(create, tabname);
      strcat(create,
          " (TYP_CHAR CHAR(30), TYP_SMALLINT SMALLINT, TYP_INTEGER INTEGER ,");
      strcat(create, " TYP_FIXED DECIMAL(15,2), TYP_FLOAT FLOAT(15) , ");
      strcat(create, " TYP_DATE DATE, TYP_TIME TIME)");

      LOGF("Create Stmt = '%s'", create);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)create, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      /* ------------------------------------------------------------------------- */
      /* *** SQLColumns *** */
      /* All qualifiers, all owners, EMPLOYEE table, all columns */
      strcpy(tabname, TABLE);
      LOGF("-> Columns information to Table : '%s'", tabname);

      retcode = SQLColumns(hstmt, NULL, 0, NULL, 0, (SQLCHAR*)TABLE, SQL_NTS,
          NULL, 0);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLColumns");

      if (retcode == SQL_SUCCESS) {
        /* Bind columns in result set to storage locations */

        SQLBindCol(hstmt, 1, SQL_C_CHAR, szQualifier, STR_LEN, &cbQualifier);
        SQLBindCol(hstmt, 2, SQL_C_CHAR, szOwner, STR_LEN, &cbOwner);
        SQLBindCol(hstmt, 3, SQL_C_CHAR, szTableName, STR_LEN, &cbTableName);
        SQLBindCol(hstmt, 4, SQL_C_CHAR, szColName, STR_LEN, &cbColName);
        SQLBindCol(hstmt, 5, SQL_C_SHORT, &DataType, 0, &cbDataType);
        SQLBindCol(hstmt, 6, SQL_C_CHAR, szTypeName, STR_LEN, &cbTypeName);
        SQLBindCol(hstmt, 7, SQL_C_LONG, &Precision, 0, &cbPrecision);
        SQLBindCol(hstmt, 8, SQL_C_LONG, &Length, 0, &cbLength);
        SQLBindCol(hstmt, 9, SQL_C_SHORT, &Scale, 0, &cbScale);
        SQLBindCol(hstmt, 10, SQL_C_SHORT, &Radix, 0, &cbRadix);
        SQLBindCol(hstmt, 11, SQL_C_SHORT, &Nullable, 0, &cbNullable);
        SQLBindCol(hstmt, 12, SQL_C_CHAR, szRemarks, REM_LEN, &cbRemarks);

        retcode = lst_ColumnNames(henv, hdbc, hstmt, 12);

        while (TRUE) {
          count++;
          retcode = SQLFetch(hstmt);
          if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO) {
            /* Process fetched data */
            printf("\tColumn %d : ", count);

            /* *** TABLE_QUALIFIER ------------------- *** */
            if (cbQualifier == SQL_NULL_DATA) strcpy(szQualifier, NULL_VALUE);
            printf("'%s',", szQualifier);
            /* *** TABLE_OWNER ----------------------- *** */
            if (cbOwner == SQL_NULL_DATA) strcpy(szOwner, NULL_VALUE);
            printf("'%s',", szOwner);
            /* *** TABLE_NAME ------------------------ *** */
            printf("'%s',", szTableName);
            /* *** COLUMN_NAME ----------------------- *** */
            printf("'%s',", szColName);
            /* *** DATA_TYPE ------------------------- *** */
            if (cbDataType == SQL_NULL_DATA)
              printf("'%s',", NULL_VALUE);
            else
              printf("'%d',", DataType);
            /* *** TYPE_NAME ------------------------- *** */
            printf("'%s',", szTypeName);
            /* *** PRECISION ------------------------- *** */
            if (cbPrecision == SQL_NULL_DATA)
              printf("'%s',", NULL_VALUE);
            else
              printf("'%d',", Precision);
            /* *** LENGTH ---------------------------- *** */
            if (cbLength == SQL_NULL_DATA)
              printf("'%s',", NULL_VALUE);
            else
              printf("'%d',", Length);
            /* *** SCALE ----------------------------- *** */
            if (cbScale == SQL_NULL_DATA)
              printf("'%s',", NULL_VALUE);
            else
              printf("'%d',", Scale);
            /* *** RADIX ----------------------------- *** */
            if (cbRadix == SQL_NULL_DATA)
              printf("'%s',", NULL_VALUE);
            else
              printf("'%d',", Radix);
            /* *** NULLABLE -------------------------- *** */
            Get_pfNullable(Nullable, buffer);
            printf("'%s',", buffer);
            /* *** REMARKS --------------------------- *** */
            if (cbRemarks == SQL_NULL_DATA) strcpy(szRemarks, NULL_VALUE);
            printf("'%s' \r\n", szRemarks);

          }
          else {
            break;
          }
        }
      }
      if (retcode == SQL_ERROR || retcode == SQL_SUCCESS_WITH_INFO) {
        /* Show Error */
        DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
            "SQLError");
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

      retcode = SQLFreeStmt(hstmt, SQL_CLOSE);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLFreeStmt");
      /* ---------------------------------------------------------------------har- */
      //free sql handles (stmt, dbc, env)
      FREE_SQLHANDLES
}
END_TEST(testSQLColumns)
