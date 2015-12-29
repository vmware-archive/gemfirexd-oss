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

/* ------------------------------------------------------------------------- */
#define TESTNAME "SQLSpecialColumns"
#define TABLE1 "TABSPECCOL1"
#define TABLE2 "TABSPECCOL2"
#define TABLE3 "TABSPECCOL3"
#define SCHEMA "GFXDODBC"

#define STR_LEN 128+1
#define MAX_NAME_LEN 50

#define OUTPUTTXT "Driver does not support this function !"

#define NULL_VALUE "<NULL>"

//*-------------------------------------------------------------------------

/* *********************************************************************** */

RETCODE lstSpecialColInfo(SQLHENV henv, SQLHDBC hdbc, SQLHSTMT hstmt)
{
  RETCODE retcode;
  SQLSMALLINT sErr = 0;
  /* ------------------------------------------------------------------------- */
  /* Declare storage locations for result set data */
  CHAR szColumnName[STR_LEN], szTypeName[STR_LEN];
  CHAR szScope[STR_LEN], szPseudoColumn[STR_LEN], szScale[STR_LEN];
  SQLSMALLINT Scope, DataType, Scale, PseudoColumn;
  SQLINTEGER Precision, Length;

  /* Declare storage locations for bytes available to return */
  SQLLEN cbScope, cbColumnName, cbDataType, cbTypeName, cbPrecision;
  SQLLEN cbLength, cbScale, cbPseudoColumn;

  SQLSMALLINT count = 0;
  /* ---------------------------------------------------------------------har- */
  /* Bind columns in result set to storage locations */
  SQLBindCol(hstmt, 1, SQL_C_SSHORT, &Scope, 0, &cbScope);
  SQLBindCol(hstmt, 2, SQL_C_CHAR, szColumnName, STR_LEN, &cbColumnName);
  SQLBindCol(hstmt, 3, SQL_C_SSHORT, &DataType, 0, &cbDataType);
  SQLBindCol(hstmt, 4, SQL_C_CHAR, szTypeName, STR_LEN, &cbTypeName);
  SQLBindCol(hstmt, 5, SQL_C_SLONG, &Precision, 0, &cbPrecision);
  SQLBindCol(hstmt, 6, SQL_C_SLONG, &Length, 0, &cbLength);
  SQLBindCol(hstmt, 7, SQL_C_SSHORT, &Scale, 0, &cbScale);
  SQLBindCol(hstmt, 8, SQL_C_SSHORT, &PseudoColumn, 0, &cbPseudoColumn);

  retcode = lst_ColumnNames(henv, hdbc, hstmt, 8);

  while (TRUE) {
    count++;

    retcode = SQLFetch(hstmt);
    /* DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,"SQLFetch"); */

    if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO) {
      /* Process fetched data */
      if (cbColumnName == SQL_NULL_DATA) strcpy(szColumnName, NULL_VALUE);
      if (cbTypeName == SQL_NULL_DATA) strcpy(szTypeName, NULL_VALUE);
      if (cbScale == SQL_NULL_DATA)
        strcpy(szScale, NULL_VALUE);
      else
        sprintf(szScale, "%d", Scale);

      /* Scope information */
      switch (Scope) {
        case SQL_SCOPE_CURROW:
          strcpy(szScope, "SQL_SCOPE_CUUROW");
          break;
        case SQL_SCOPE_TRANSACTION:
          strcpy(szScope, "SQL_SCOPE_TRANSACTION");
          break;
        case SQL_SCOPE_SESSION:
          strcpy(szScope, "SQL_SCOPE_SESSION");
          break;
      }
      /* PseudoColumn information */
      switch (PseudoColumn) {
        case SQL_PC_UNKNOWN:
          strcpy(szPseudoColumn, "SQL_PC_UNKNOWN");
          break;
        case SQL_PC_PSEUDO:
          strcpy(szPseudoColumn, "SQL_PC_PSEUDO");
          break;
        case SQL_PC_NOT_PSEUDO:
          strcpy(szPseudoColumn, "SQL_PC_NOT_PSEUDO");
          break;
      }
      printf("\tColumn %d : '%s','%s','%d','%s','%d','%d','%s','%s'\n",
          count, szScope, szColumnName, DataType, szTypeName, Precision,
          Length, szScale, szPseudoColumn);
    }
    else {
      break;
    }
  }
  return TRUE;
}
/* ------------------------------------------------------------------------- */

BEGIN_TEST(testSQLSpecialColumns)
{
  /* ------------------------------------------------------------------------- */
      CHAR create[MAX_NAME_LEN + MAX_NAME_LEN + 1];
      CHAR drop[MAX_NAME_LEN + 1];
      CHAR tabname[MAX_NAME_LEN];

      SQLSMALLINT fColType;
      CHAR szTableQualifier[MAX_NAME_LEN], szTableOwner[MAX_NAME_LEN],
          szTableName[MAX_NAME_LEN];

      SQLSMALLINT fScope, fNullable;

      SQLSMALLINT cbTableQualifier, cbTableOwner, cbTableName;

      /* ---------------------------------------------------------------------har- */

      /* --- Connect ----------------------------------------------------- */
      //init sql handles (stmt, dbc, env)
      INIT_SQLHANDLES

      /* --- Create Table 1. ------------------------------------------- */
      strcpy(tabname, TABLE1);
      strcpy(create, "CREATE TABLE ");
      strcat(create, tabname);
      strcat(create, " (MNR INTEGER, NAME CHAR(30))");
      LOGF("Create Stmt 1.= '%s'", create);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)create, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      /* --- Create Table 2. ------------------------------------------- */
      strcpy(tabname, TABLE2);
      strcpy(create, "CREATE TABLE ");
      strcat(create, tabname);
      strcat(create, " (MNR INTEGER PRIMARY KEY, NAME CHAR(30))");
      LOGF("Create Stmt 2.= '%s'", create);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)create, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      /* --- Create Table 3. ------------------------------------------- */
      strcpy(tabname, TABLE3);
      strcpy(create, "CREATE TABLE ");
      strcat(create, tabname);
      strcat(create, " (MNR INTEGER, NAME CHAR(30), PRIMARY KEY (NAME))");
      LOGF("Create Stmt 3.= '%s'", create);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)create, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      /* --- SQLSpecialColumns 1. ---------------------------------------- */
      LOGF("\1.List SpecialColumns Table -> %s ", TABLE1);
      LOGF(" fColType -> SQL_BEST_ROWID, fScope -> SQL_SCOPE_CURROW ");
      fColType = SQL_BEST_ROWID;
      fScope = SQL_SCOPE_CURROW;
      fNullable = SQL_NULLABLE;

      retcode = SQLSpecialColumns(hstmt, fColType, (SQLCHAR*)NULL,
          0, (SQLCHAR*)SCHEMA, SQL_NTS, (SQLCHAR*)TABLE1,
          SQL_NTS, fScope, fNullable);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLSpecialColumns");

      if (retcode == SQL_SUCCESS) lstSpecialColInfo(henv, hdbc, hstmt);

      retcode = SQLFreeStmt(hstmt, SQL_CLOSE);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLFreeStmt");

      /* --- SQLSpecialColumns 2. ---------------------------------------- */
      LOGF("2.List SpecialColumns Table -> %s ", TABLE2);
      LOGF(" fColType -> SQL_BEST_ROWID, fScope -> SQL_SCOPE_CURROW ");
      fColType = SQL_BEST_ROWID;
      fScope = SQL_SCOPE_CURROW;
      fNullable = SQL_NULLABLE;

      retcode = SQLSpecialColumns(hstmt, fColType, (SQLCHAR*)NULL,
          0, (SQLCHAR*)SCHEMA, SQL_NTS, (SQLCHAR*)TABLE2,
          SQL_NTS, fScope, fNullable);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLSpecialColumns");

      if (retcode == SQL_SUCCESS) lstSpecialColInfo(henv, hdbc, hstmt);

      retcode = SQLFreeStmt(hstmt, SQL_CLOSE);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLFreeStmt");

      /* --- SQLSpecialColumns 3. ---------------------------------------- */
      LOGF("3.List SpecialColumns Table -> %s ", TABLE3);
      printf(
          "\t fColType -> SQL_BEST_ROWID, fScope -> SQL_SCOPE_TRANSACTION ");
      fColType = SQL_BEST_ROWID;
      fScope = SQL_SCOPE_TRANSACTION;
      fNullable = SQL_NULLABLE;

      retcode = SQLSpecialColumns(hstmt, fColType, (SQLCHAR*)NULL,
          0, (SQLCHAR*)SCHEMA, SQL_NTS, (SQLCHAR*)TABLE3,
          SQL_NTS, fScope, fNullable);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLSpecialColumns");

      if (retcode == SQL_SUCCESS) lstSpecialColInfo(henv, hdbc, hstmt);

      retcode = SQLFreeStmt(hstmt, SQL_CLOSE);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLFreeStmt");

      /* --- SQLSpecialColumns 4. ---------------------------------------- */
      LOGF("4.List SpecialColumns Table -> %s ", TABLE3);
      printf(
          "\t fColType -> SQL_BEST_ROWID, fScope -> SQL_SCOPE_SESSION ");
      fColType = SQL_BEST_ROWID;
      fScope = SQL_SCOPE_SESSION;
      fNullable = SQL_NULLABLE;

      retcode = SQLSpecialColumns(hstmt, fColType, (SQLCHAR*)NULL,
          SQL_NTS, (SQLCHAR*)SCHEMA, SQL_NTS, (SQLCHAR*)TABLE3,
          SQL_NTS, fScope, fNullable);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLSpecialColumns");

      if (retcode == SQL_SUCCESS) lstSpecialColInfo(henv, hdbc, hstmt);

      retcode = SQLFreeStmt(hstmt, SQL_CLOSE);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLFreeStmt");

      /* --- SQLSpecialColumns 5. ---------------------------------------- */
      LOGF("5.List SpecialColumns Table -> %s ", TABLE3);
      LOGF(" fColType -> SQL_ROWVER, fScope -> SQL_SCOPE_CURROW ");
      fColType = SQL_ROWVER;
      fScope = SQL_SCOPE_CURROW;
      fNullable = SQL_NULLABLE;

      retcode = SQLSpecialColumns(hstmt, fColType, (SQLCHAR*)NULL,
          0, (SQLCHAR*)SCHEMA, SQL_NTS, (SQLCHAR*)TABLE1,
          SQL_NTS, fScope, fNullable);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLSpecialColumns");

      if (retcode == SQL_SUCCESS) lstSpecialColInfo(henv, hdbc, hstmt);

      retcode = SQLFreeStmt(hstmt, SQL_CLOSE);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLFreeStmt");

      /* --- Drop Table 1. -------------------------------------------- */
      strcpy(drop, "DROP TABLE ");
      strcat(drop, TABLE1);
      LOGF("Drop Stmt= '%s'", drop);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)drop, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      /* --- Drop Table 2. -------------------------------------------- */
      strcpy(drop, "DROP TABLE ");
      strcat(drop, TABLE2);
      LOGF("Drop Stmt= '%s'", drop);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)drop, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      /* --- Drop Table 3. -------------------------------------------- */
      strcpy(drop, "DROP TABLE ");
      strcat(drop, TABLE3);
      LOGF("Drop Stmt= '%s'", drop);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)drop, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");
      /* ----------------------------------------------------------------- */
      //free sql handles (stmt, dbc, env)
      FREE_SQLHANDLES
}
END_TEST(testSQLSpecialColumns)
