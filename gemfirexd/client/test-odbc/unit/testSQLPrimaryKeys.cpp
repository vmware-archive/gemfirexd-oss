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

#define TESTNAME "SQLPRIMARYKEYS"

#define TABLE1 "PR_CUSTOMER"
#define TABLE2 "PR_EMPLOYEE"

#define MAX_NAME_LEN 80

#define STR_LEN 128+1
#define REM_LEN 254+1

#define NULL_VALUE "<NULL>"

//*-------------------------------------------------------------------------

/* ------------------------------------------------------------------------ */
RETCODE lstPrimaryKeysInfo(SQLHENV henv, SQLHDBC hdbc, SQLHSTMT hstmt)
{
  RETCODE retcode;
  SQLSMALLINT sErr = 0;
  /* ------------------------------------------------------------------------- */
  /* Declare storage locations for result set data */
  CHAR szTableQualifier[STR_LEN], szTableOwner[STR_LEN], szTableName[STR_LEN],
      szColumnName[STR_LEN];
  CHAR szPkName[STR_LEN];
  SQLSMALLINT KeySeq;

  /* Declare storage locations for bytes available to return */
  SQLLEN cbTableQualifier, cbTableOwner, cbTableName, cbColumnName;
  SQLLEN cbKeySeq, cbPkName;

  SQLSMALLINT count = 0;
  /* ---------------------------------------------------------------------har- */
  /* Bind columns in result set to storage locations */
  SQLBindCol(hstmt, 1, SQL_C_CHAR, szTableQualifier, STR_LEN,
      &cbTableQualifier);
  SQLBindCol(hstmt, 2, SQL_C_CHAR, szTableOwner, STR_LEN, &cbTableOwner);
  SQLBindCol(hstmt, 3, SQL_C_CHAR, szTableName, STR_LEN, &cbTableName);
  SQLBindCol(hstmt, 4, SQL_C_CHAR, szColumnName, STR_LEN, &cbColumnName);
  SQLBindCol(hstmt, 5, SQL_C_SHORT, &KeySeq, 0, &cbKeySeq);
  SQLBindCol(hstmt, 6, SQL_C_CHAR, szPkName, STR_LEN, &cbPkName);

  retcode = lst_ColumnNames(henv, hdbc, hstmt, 6);

  while (TRUE) {
    count++;

    retcode = SQLFetch(hstmt);
    /* DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,"SQLFetch");*/

    if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO) {
      /* Process fetched data */
      if (cbTableQualifier == SQL_NULL_DATA) strcpy(szTableQualifier,
          NULL_VALUE);
      if (cbTableOwner == SQL_NULL_DATA) strcpy(szTableOwner, NULL_VALUE);
      if (cbTableName == SQL_NULL_DATA) strcpy(szTableName, NULL_VALUE);
      if (cbColumnName == SQL_NULL_DATA) strcpy(szColumnName, NULL_VALUE);
      if (cbPkName == SQL_NULL_DATA) strcpy(szPkName, NULL_VALUE);
      /*
       if (cbKeySeq == SQL_NULL_DATA) KeySeq=NULL;
       */
      printf("\tColumn %d : '%s','%s','%s','%s','%d','%s'\r\n", count,
          szTableQualifier, szTableOwner, szTableName, szColumnName, KeySeq,
          szPkName);
    }
    else {
      break;
    }
  }
  return TRUE;
}
/* ------------------------------------------------------------------------- */

BEGIN_TEST(testSQLPrimaryKeys)
{
  /* ------------------------------------------------------------------------- */
      CHAR tabname[MAX_NAME_LEN + 1];
      CHAR create[(MAX_NAME_LEN + 1) * 2];
      CHAR drop[MAX_NAME_LEN + 1];

      /* Declare storage locations for result set data */
      CHAR szTableName[STR_LEN];

      /* ---------------------------------------------------------------------har- */

      //init sql handles (stmt, dbc, env)
      INIT_SQLHANDLES

      /* ------------------------------------------------------------------------- */
      /* --- Create Table 1. ------------------------------------------ */
      strcpy(tabname, TABLE1);
      strcpy(create, "CREATE TABLE ");
      strcat(create, tabname);
      strcat(create,
          " (CUSTOMER_ID INTEGER, CUST_NAME CHAR(30),ADRESS CHAR(60), PHONE CHAR(15), PRIMARY KEY (CUSTOMER_ID))");
      printf("\tCreate Stmt (Table1: CUSTOMER)= '%s'\r\n", create);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)create, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      /* --- Create Table 2. ------------------------------------------ */
      strcpy(tabname, TABLE2);
      strcpy(create, "CREATE TABLE ");
      strcat(create, tabname);
      strcat(create,
          " (EMPLOYEE_ID INTEGER, NAME CHAR(30),AGE INTEGER, BIRTHDAY DATE, ");
      strcat(create, "PRIMARY KEY (EMPLOYEE_ID, NAME)) ");
      printf("\tCreate Stmt (Table2: EMPLOYEE)= '%s'\r\n", create);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)create, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      /* ------------------------------------------------------------------------- */
      /* ***************************************************************** */
      /* *** I. SQLPrimaryKeys ******************************************* */
      /* ***************************************************************** */
      strcpy(szTableName, TABLE1);

      printf("\tI.) SQLPrimaryKeys -> all primary key for table: %s'\r\n",
          szTableName);

      /* Get all the primary keys. */
      retcode = SQLPrimaryKeys(hstmt, NULL, 0, /* Table qualifier */
      NULL, 0, //lpSrvr->szValidLogin0, SQL_NTS, /* Table owner     */
      (SQLCHAR*)szTableName, SQL_NTS); /* Table name      */
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLPrimaryKeys");

      if (retcode == SQL_SUCCESS) lstPrimaryKeysInfo(henv, hdbc, hstmt);

      retcode = SQLFreeStmt(hstmt, SQL_CLOSE);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLFreeStmt");

      /* ***************************************************************** */
      /* *** II. SQLPrimaryKeys ****************************************** */
      /* ***************************************************************** */
      strcpy(szTableName, TABLE2);

      printf("\tII.) SQLPrimaryKeys -> all primary key for table: %s'\r\n",
          szTableName);

      /* Get all the primary keys. */
      retcode = SQLPrimaryKeys(hstmt, NULL, 0, /* Table qualifier */
      NULL, 0, //lpSrvr->szValidLogin0, SQL_NTS, /* Table owner     */
      (SQLCHAR*)szTableName, SQL_NTS); /* Table name      */
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLPrimaryKeys");

      if (retcode == SQL_SUCCESS) lstPrimaryKeysInfo(henv, hdbc, hstmt);

      retcode = SQLFreeStmt(hstmt, SQL_CLOSE);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLFreeStmt");
      /* --- Drop Table ----------------------------------------------- */
#ifdef DROP_TABLE
      /* --- Drop Table 1. ------------------------------------------ */
      strcpy(tabname, TABLE1);
      strcpy(drop, "DROP TABLE ");
      strcat(drop, tabname);
      printf("\tDrop Stmt (Table1:CUSTOMER)= '%s'\r\n", drop);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)drop, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");
      /* --- Drop Table 2. ------------------------------------------ */
      strcpy(tabname, TABLE2);
      strcpy(drop, "DROP TABLE ");
      strcat(drop, tabname);
      printf("\tDrop Stmt (Table2:EMPLOYEE)= '%s'\r\n", drop);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)drop, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");
#endif
      /* ---------------------------------------------------------------------har- */
      //free sql handles (stmt, dbc, env)
      FREE_SQLHANDLES
    }END_TEST(testSQLPrimaryKeys)
