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

#define TESTNAME "SQLTables"
#define TABLE "TABLESTST"
#define TABLE1 "TAB1LESTST"
#define TABLE_OWNER "ODBC"
#define TABLE_TYPES "TABLE"

#define MAX_NAME_LEN 50

#define STR_LEN 128+1
#define REM_LEN 254+1

#define NULL_VALUE "<NULL>"
//*-------------------------------------------------------------------------

/* ------------------------------------------------------------------------- */

RETCODE lstTableInfo(SQLHENV henv, SQLHDBC hdbc, SQLHSTMT hstmt)
{
  RETCODE retcode;
  SQLSMALLINT sErr = 0;
  /* ------------------------------------------------------------------------- */
  /* Declare storage locations for result set data */

  CHAR szQualifier[STR_LEN], szOwner[STR_LEN], szTableName[STR_LEN],
      szTableType[STR_LEN], szRemarks[REM_LEN];

  /* Declare storage locations for bytes available to return */

  SQLLEN cbQualifier, cbOwner, cbTableName, cbTableType, cbRemarks;

  SQLSMALLINT count = 0;
  /* ---------------------------------------------------------------------har- */
  /* Bind columns in result set to storage locations */
  SQLBindCol(hstmt, 1, SQL_C_CHAR, szQualifier, STR_LEN, &cbQualifier);
  SQLBindCol(hstmt, 2, SQL_C_CHAR, szOwner, STR_LEN, &cbOwner);
  SQLBindCol(hstmt, 3, SQL_C_CHAR, szTableName, STR_LEN, &cbTableName);
  SQLBindCol(hstmt, 4, SQL_C_CHAR, szTableType, STR_LEN, &cbTableType);
  SQLBindCol(hstmt, 5, SQL_C_CHAR, szRemarks, REM_LEN, &cbRemarks);

  retcode = lst_ColumnNames(henv, hdbc, hstmt, 5);

  while (TRUE) {
    count++;
    retcode = SQLFetch(hstmt);
    /* ERRORCHECK(*phenv,*phdbc,hstmt,lpSrvr,SQL_SUCCESS,rc,"SQLFetch"); */

    if (retcode == SQL_ERROR || retcode == SQL_SUCCESS_WITH_INFO) {
      /* Show Error */
      /* ERRORCHECK(*phenv,*phdbc,hstmt,lpSrvr,SQL_SUCCESS,rc,"SQLError"); */
    }
    if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO) {
      /* Process fetched data */
      if (cbQualifier == SQL_NULL_DATA) strcpy(szQualifier, NULL_VALUE);
      if (cbOwner == SQL_NULL_DATA) strcpy(szOwner, NULL_VALUE);
      if (cbTableName == SQL_NULL_DATA) strcpy(szTableName, NULL_VALUE);
      if (cbTableType == SQL_NULL_DATA) strcpy(szTableType, NULL_VALUE);
      if (cbRemarks == SQL_NULL_DATA) strcpy(szRemarks, NULL_VALUE);

      LOGF("Row %d.: '%s','%s','%s','%s','%s'",
          count, szQualifier, szOwner, szTableName, szTableType, szRemarks);
    }
    else {
      break;
    }
  }
  return TRUE;
}
/* ------------------------------------------------------------------------- */

BEGIN_TEST(testSQLTables1)
{
  /* ------------------------------------------------------------------------- */
  /* Declare storage locations for result set data */

      CHAR szQualifier[STR_LEN], szOwner[STR_LEN], szTableName[STR_LEN],
          szTableType[STR_LEN];

      /* Declare storage locations for bytes available to return */

      SQLSMALLINT cbQualifier, cbOwner, cbTableName, cbTableType;

      CHAR create[MAX_NAME_LEN + MAX_NAME_LEN + 1];
      CHAR drop[MAX_NAME_LEN + 1];
      CHAR tabname[MAX_NAME_LEN];

      SQLSMALLINT count = 0;
      /* ---------------------------------------------------------------------har- */

      //init sql handles (stmt, dbc, env)
      INIT_SQLHANDLES

      /* ------------------------------------------------------------------------- */
      /* ***************************************************************** */
      /* *** I.) Result set contains a list of valid qualifier for the     */
      /* ***     data source                                               */
      /* ***************************************************************** */
      /* All qualifiers, all owners, EMPLOYEE table, all columns */
      strcpy(szQualifier, "%");
      strcpy(szOwner, "");
      strcpy(szTableName, "");
      /* strcpy(szTableType,""); */
      cbQualifier = SQL_NTS;
      cbOwner = cbTableName = cbTableType = 0;

      /* --- Create Table --------------------------------------------- */
      strcpy(tabname, TABLE1);
      strcpy(create, "CREATE TABLE ");
      strcat(create, tabname);
      strcat(create, " (TYP_CHAR CHAR(30), TYP_SMALLINT SMALLINT )");
      LOGF("Create Stmt = '%s'", create);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)create, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      retcode = SQLFreeStmt(hstmt, SQL_CLOSE);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLFreeStmt");
      /* --------------------------------------------------------------- */

      LOGF("I.) SQLTables -> list for all qualifier : Qualifier=%s'",
          szQualifier);

      retcode = SQLTables(hstmt, (SQLCHAR*)szQualifier, cbQualifier, NULL,
          cbOwner, NULL, cbTableName, (SQLCHAR*)NULL, cbTableType);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLTables");

      if (retcode == SQL_SUCCESS) lstTableInfo(henv, hdbc, hstmt);

      retcode = SQLFreeStmt(hstmt, SQL_CLOSE);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLFreeStmt");
      /* ***************************************************************** */
      /* *** II.) Result set contains a list of valid owners for the       */
      /* ***      data source                                              */
      /* ***************************************************************** */
      strcpy(szQualifier, "");
      strcpy(szOwner, "%");
      strcpy(szTableName, "");
      /* strcpy(szTableType,""); */
      cbQualifier = cbTableName = cbTableType = 0;
      cbOwner = SQL_NTS;

      LOGF("II.) SQLTables -> list for all owners : Owner=%s ", szOwner);

      retcode = SQLTables(hstmt, (SQLCHAR*)szQualifier, cbQualifier,
          (SQLCHAR*)szOwner, cbOwner, (SQLCHAR*)szTableName, cbTableName,
          (SQLCHAR*)NULL, cbTableType);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLTables");

      if (retcode == SQL_SUCCESS) lstTableInfo(henv, hdbc, hstmt);
      retcode = SQLFreeStmt(hstmt, SQL_CLOSE);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLFreeStmt");

      /* ***************************************************************** */
      /* *** III.) Result set contains a list of valid types for the       */
      /* ***       data source                                             */
      /* ***************************************************************** */
      strcpy(szQualifier, "");
      strcpy(szOwner, "");
      strcpy(szTableName, "");
      strcpy(szTableType, "%");
      cbQualifier = cbOwner = cbTableName = 0;
      cbTableType = SQL_NTS;

      LOGF("III.) SQLTables -> list for all types : TableTypes=%s ",
          szTableType);

      retcode = SQLTables(hstmt, (SQLCHAR*)szQualifier, cbQualifier,
          (SQLCHAR*)szOwner, cbOwner, (SQLCHAR*)szTableName, cbTableName,
          (SQLCHAR*)szTableType, cbTableType);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLTables");

      if (retcode == SQL_SUCCESS) lstTableInfo(henv, hdbc, hstmt);

      retcode = SQLFreeStmt(hstmt, SQL_CLOSE);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLFreeStmt");

      /* ***************************************************************** */
      /* *** IV.) With other search pattern                                */
      /* ***                                                               */
      /* ***************************************************************** */
      /* --- Create Table --------------------------------------------- */
      strcpy(tabname, TABLE);
      strcpy(create, "CREATE TABLE ");
      strcat(create, tabname);
      strcat(create, " (TYP_CHAR CHAR(30), TYP_SMALLINT SMALLINT )");
      LOGF("Create Stmt = '%s'", create);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)create, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      retcode = SQLFreeStmt(hstmt, SQL_CLOSE);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLFreeStmt");
      /* --------------------------------------------------------------- */

      strcpy(szQualifier, "");
      strcpy(szOwner, TABLE_OWNER);
      strcpy(szTableName, "%");
      strcpy(szTableType, TABLE_TYPES);
      cbQualifier = 0;
      cbOwner = cbTableName = cbTableType = SQL_NTS;

      LOGF(
          "IV.) SQLTables -> list for search pattern : Owner=%s, TableName=%s, TableType=%s'",
          szOwner, szTableName, szTableType);

      retcode = SQLTables(hstmt, (SQLCHAR*)szQualifier, cbQualifier,
          (SQLCHAR*)szOwner, cbOwner, (SQLCHAR*)szTableName, cbTableName,
          (SQLCHAR*)szTableType, cbTableType);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLTables");

      if (retcode == SQL_SUCCESS) lstTableInfo(henv, hdbc, hstmt);

      retcode = SQLFreeStmt(hstmt, SQL_CLOSE);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLFreeStmt");
      /* ----------------------------------------------------------------- */
      strcpy(drop, "DROP TABLE ");
      strcat(drop, tabname);
      LOGF("Drop Stmt= '%s'", drop);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)drop, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");
      /* ---------------------------------------------------------------------har- */
      //free sql handles (stmt, dbc, env)
      FREE_SQLHANDLES
    }
    END_TEST(testSQLTables1)

// simple helper functions
int MySQLSuccess(SQLRETURN rc)
{
  return (rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO);
}

struct DataBinding
{
  SQLSMALLINT TargetType;
  SQLPOINTER TargetValuePtr;
  SQLINTEGER BufferLength;
  SQLLEN StrLen_or_Ind;
};

void printCatalog(const struct DataBinding* catalogResult)
{
  if (catalogResult[0].StrLen_or_Ind != SQL_NULL_DATA){
    printf("\nCatalog name  = %s\t", (char *)catalogResult[0].TargetValuePtr);
    printf("Schema name  = %s\t", (char *)catalogResult[1].TargetValuePtr);
    printf("Table name  = %s\t", (char *)catalogResult[2].TargetValuePtr);
    printf("Table type = %s\t", (char *)catalogResult[3].TargetValuePtr);
    printf("Table description = %s\n", (char *)catalogResult[4].TargetValuePtr);
  }
}

// remember to disconnect and free memory, and free statements and handles
BEGIN_TEST(testSQLTables2)
{
      int bufferSize = 1024, i, numCols = 5;
      struct DataBinding* catalogResult = (struct DataBinding*)malloc(
          numCols * sizeof(struct DataBinding));
      wchar_t* dbName = (wchar_t *)malloc(sizeof(wchar_t) * bufferSize);
      wchar_t* userName = (wchar_t *)malloc(sizeof(wchar_t) * bufferSize);

      //init sql handles (stmt, dbc, env)
      INIT_SQLHANDLES

      bufferSize = 1024;

      // allocate memory for the binding
      // free this memory when done
      for (i = 0; i < numCols; i++) {
        catalogResult[i].TargetType = SQL_C_CHAR;
        catalogResult[i].BufferLength = (bufferSize + 1);
        catalogResult[i].TargetValuePtr = malloc(
            sizeof(unsigned char) * catalogResult[i].BufferLength);
      }

      // setup the binding (can be used even if the statement is closed by closeStatementHandle)
      for (i = 0; i < numCols; i++) {
        retcode = SQLBindCol(hstmt, (SQLUSMALLINT)i + 1,
            catalogResult[i].TargetType, catalogResult[i].TargetValuePtr,
            catalogResult[i].BufferLength, &(catalogResult[i].StrLen_or_Ind));
        DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
            "SQLBindCol");
      }

      // all catalogs query
      LOGF( "A list of names of all catalogs\n");
      retcode = SQLTables(hstmt, (SQLCHAR*)SQL_ALL_CATALOGS, SQL_NTS,
          NULL, 0, NULL, 0, NULL, 0);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLBindCol");

      do {
        retcode = SQLFetch(hstmt);
        if (retcode != SQL_NO_DATA) {
          DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
              "SQLBindCol");
          printCatalog(catalogResult);
        }
      }
      while (retcode != SQL_NO_DATA);



    }
    END_TEST(testSQLTables2)
