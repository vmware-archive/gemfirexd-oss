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

#define TESTNAME "SQLFOREIGNKEYS"

#define TABLE1 "FR_CUSTOMER"
#define TABLE2 "FR_EMPLOYEE"
#define TABLE3 "FR_SALES_ORDER"
#define TABLE4 "FR_SALES_LINE"

#define MAX_NAME_LEN 256

#define STR_LEN 128+1
#define REM_LEN 254+1

#define NULL_VALUE "<NULL>"

//*-------------------------------------------------------------------------

bool lstForeignKeysInfo(SQLHENV henv, SQLHDBC hdbc, SQLHSTMT hstmt)
{
  RETCODE rc;
  SQLSMALLINT sErr = 0;
  /* ------------------------------------------------------------------------- */
  /* Declare storage locations for result set data */
  CHAR szPkQualifier[STR_LEN], szPkOwner[STR_LEN], szPkTableName[STR_LEN],
      szPkColumnName[STR_LEN];
  CHAR szFkQualifier[STR_LEN], szFkOwner[STR_LEN], szFkTableName[STR_LEN],
      szFkColumnName[STR_LEN];
  CHAR szFkName[STR_LEN], szPkName[STR_LEN];
  SQLSMALLINT KeySeq, UpdateRule, DeleteRule;
  CHAR szUpdateRule[STR_LEN], szDeleteRule[STR_LEN], szKeySeq[STR_LEN];

  /* Declare storage locations for bytes available to return */
  SQLLEN cbPkQualifier, cbPkOwner, cbPkTableName, cbPkColumnName;
  SQLLEN cbFkQualifier, cbFkOwner, cbFkTableName, cbFkColumnName;
  SQLLEN cbKeySeq, cbUpdateRule, cbDeleteRule, cbFkName, cbPkName;

  SQLSMALLINT count = 0;
  /* ---------------------------------------------------------------------har- */

  /* Bind columns in result set to storage locations */
  SQLBindCol(hstmt, 1, SQL_C_CHAR, szPkQualifier, STR_LEN, &cbPkQualifier);
  SQLBindCol(hstmt, 2, SQL_C_CHAR, szPkOwner, STR_LEN, &cbPkOwner);
  SQLBindCol(hstmt, 3, SQL_C_CHAR, szPkTableName, STR_LEN, &cbPkTableName);
  SQLBindCol(hstmt, 4, SQL_C_CHAR, szPkColumnName, STR_LEN, &cbPkColumnName);
  SQLBindCol(hstmt, 5, SQL_C_CHAR, szFkQualifier, STR_LEN, &cbFkQualifier);
  SQLBindCol(hstmt, 6, SQL_C_CHAR, szFkOwner, STR_LEN, &cbFkOwner);
  SQLBindCol(hstmt, 7, SQL_C_CHAR, szFkTableName, STR_LEN, &cbFkTableName);
  SQLBindCol(hstmt, 8, SQL_C_CHAR, szFkColumnName, STR_LEN, &cbFkColumnName);
  SQLBindCol(hstmt, 9, SQL_C_SHORT, &KeySeq, 0, &cbKeySeq);
  SQLBindCol(hstmt, 10, SQL_C_SHORT, &UpdateRule, 0, &cbUpdateRule);
  SQLBindCol(hstmt, 11, SQL_C_SHORT, &DeleteRule, 0, &cbDeleteRule);
  SQLBindCol(hstmt, 12, SQL_C_CHAR, szFkName, STR_LEN, &cbFkName);
  SQLBindCol(hstmt, 13, SQL_C_CHAR, szPkName, STR_LEN, &cbPkName);

  rc = lst_ColumnNames(henv, hdbc, hstmt, 13);

  while (TRUE) {
    count++;

    rc = SQLFetch(hstmt);
    /* DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,"SQLFetch"); */

    if (rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO) {
      /* Process fetched data */
      if (cbPkQualifier == SQL_NULL_DATA) strcpy(szPkQualifier, NULL_VALUE);
      if (cbPkOwner == SQL_NULL_DATA) strcpy(szPkOwner, NULL_VALUE);
      if (cbPkTableName == SQL_NULL_DATA) strcpy(szPkTableName, NULL_VALUE);
      if (cbPkColumnName == SQL_NULL_DATA) strcpy(szPkColumnName, NULL_VALUE);
      if (cbFkQualifier == SQL_NULL_DATA) strcpy(szFkQualifier, NULL_VALUE);
      if (cbFkOwner == SQL_NULL_DATA) strcpy(szFkOwner, NULL_VALUE);
      if (cbFkTableName == SQL_NULL_DATA) strcpy(szFkTableName, NULL_VALUE);
      if (cbFkColumnName == SQL_NULL_DATA) strcpy(szFkColumnName, NULL_VALUE);
      if (cbUpdateRule == SQL_NULL_DATA) strcpy(szUpdateRule, NULL_VALUE);
      if (cbDeleteRule == SQL_NULL_DATA) strcpy(szDeleteRule, NULL_VALUE);
      if (cbFkName == SQL_NULL_DATA) strcpy(szFkName, NULL_VALUE);
      if (cbPkName == SQL_NULL_DATA) strcpy(szPkName, NULL_VALUE);

      if (cbKeySeq == SQL_NULL_DATA)
        strcpy(szKeySeq, NULL_VALUE);
      else
        sprintf(szKeySeq, "%d", KeySeq);

      /* UpdateRule information */
      switch (UpdateRule) {
        case SQL_CASCADE:
          strcpy(szUpdateRule, "SQL_CASCADE");
          break;
        case SQL_RESTRICT:
          strcpy(szUpdateRule, "SQL_RESTRICT");
          break;
        case SQL_SET_NULL:
          strcpy(szUpdateRule, "SQL_SET_NULL");
          break;
      }
      /* DeleteRule information */
      switch (DeleteRule) {
        case SQL_CASCADE:
          strcpy(szDeleteRule, "SQL_CASCADE");
          break;
        case SQL_RESTRICT:
          strcpy(szDeleteRule, "SQL_RESTRICT");
          break;
        case SQL_SET_NULL:
          strcpy(szDeleteRule, "SQL_SET_NULL");
          break;
      }
      printf(
          "\tColumn %d : '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s' \n",
          count, szPkQualifier, szPkOwner, szPkTableName, szPkColumnName,
          szFkQualifier, szFkOwner, szFkTableName, szFkColumnName, szKeySeq,
          szUpdateRule, szDeleteRule, szFkName, szPkName);
    }
    else {
      break;
    }
  }
  return TRUE;
}
/* ------------------------------------------------------------------------- */

BEGIN_TEST(testSQLForeignKeys)
{
  /* ------------------------------------------------------------------------- */
      CHAR tabname[MAX_NAME_LEN + 1];
      CHAR create[(MAX_NAME_LEN + 1) * 2];
      CHAR drop[MAX_NAME_LEN + 1];
      CHAR buffer[MAX_NAME_LEN + 1];

      /* Declare storage locations for result set data */
      CHAR szPkTableName[STR_LEN];
      CHAR szFkTableName[STR_LEN];
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
      LOGF("Create Stmt (Table1: CUSTOMER)= '%s'", create);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)create, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      /* --- Create Table 2. ------------------------------------------ */
      strcpy(tabname, TABLE2);
      strcpy(create, "CREATE TABLE ");
      strcat(create, tabname);
      strcat(create,
          " (EMPLOYEE_ID INTEGER, NAME CHAR(30),AGE INTEGER, BIRTHDAY DATE, PRIMARY KEY (EMPLOYEE_ID)) ");
      LOGF("Create Stmt (Table2: EMPLOYEE)= '%s'", create);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)create, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      /* --- Create Table 3. ------------------------------------------ */

      strcpy(tabname, TABLE3);
      strcpy(create, "CREATE TABLE ");
      strcat(create, tabname);
      strcat(create, " (SALES_ID INTEGER, CUSTOMER_ID INTEGER,EMPLOYEE_ID INTEGER, TOTAL_PRICE DOUBLE, ");
      strcat(create, "PRIMARY KEY (SALES_ID), ");
      strcat(create, "FOREIGN KEY (CUSTOMER_ID) REFERENCES "TABLE1" (CUSTOMER_ID), ");
      strcat(create, "FOREIGN KEY (EMPLOYEE_ID) REFERENCES "TABLE2" (EMPLOYEE_ID)) ");
      sprintf(buffer, "\tCreate Stmt (Table3: SALES_ORDER)= '%s'", create);
      LOG(buffer);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)create, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      /* --- Create Table 4. ------------------------------------------ */
      strcpy(tabname, TABLE4);
      strcpy(create, "CREATE TABLE ");
      strcat(create, tabname);
      strcat(create,
          " (SALES_ID INTEGER, LINE_NUMBER INTEGER, PART_ID INTEGER, QUANTITY INTEGER, PRICE DOUBLE, ");
      strcat(create,
          "FOREIGN KEY (SALES_ID) REFERENCES "TABLE3" (SALES_ID)) ");
      sprintf(buffer, "\tCreate Stmt (Table4: SALES_LINE)= '%s'", create);
      LOG(buffer);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)create, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      /* ------------------------------------------------------------------------- */
      /* ***************************************************************** */
      /* *** I. SQLForeignKeys (PkTableName) ***************************** */
      /* ***************************************************************** */
      strcpy(szPkTableName, TABLE3);

      LOGF("I.) SQLForeignKeys -> with primary key tablename: %s'",
          szPkTableName);

      /* Get all the foreign keys that refer to TABLE3 primary key.        */
      /* Wenn Ownername -> "lpSrvr->szValidLogin0, SQL_NTS,"*/
      retcode = SQLForeignKeys(hstmt, NULL, 0, /* Primary qualifier */
      NULL, 0, //lpSrvr->szValidLogin0, SQL_NTS, /* Primary owner     */
          (SQLCHAR*)szPkTableName, SQL_NTS, /* Primary table     */
          NULL, 0, /* Foreign qualifier */
          NULL, 0, /* Foreign owner     */
          NULL, 0); /* Foreign table     */
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLForeignKeys");

      if (retcode == SQL_SUCCESS) lstForeignKeysInfo(henv, hdbc, hstmt);

      retcode = SQLFreeStmt(hstmt, SQL_CLOSE);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLFreeStmt");

      /* ***************************************************************** */
      /* *** II. SQLForeignKeys (FkTableName) ***************************** */
      /* ***************************************************************** */
      strcpy(szFkTableName, TABLE3);

      LOGF("II.) SQLForeignKeys -> with foreign key tablename: %s'",
          szFkTableName);

      /* Get all the foreign keys in the TABLE3 table.                     */
      /* Wenn Ownername -> "lpSrvr->szValidLogin0, SQL_NTS,"*/
      retcode = SQLForeignKeys(hstmt, NULL, 0, /* Primary qualifier */
      NULL, 0, /* Primary owner     */
      NULL, 0, /* Primary table     */
      NULL, 0, /* Foreign qualifier */
      NULL, 0, //lpSrvr->szValidLogin0, SQL_NTS, /* Foreign owner     */
          (SQLCHAR*)szFkTableName, SQL_NTS); /* Foreign table     */
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLForeignKeys");

      if (retcode == SQL_SUCCESS) lstForeignKeysInfo(henv, hdbc, hstmt);

      retcode = SQLFreeStmt(hstmt, SQL_CLOSE);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLFreeStmt");

      /* ***************************************************************** */
      /* *** III. SQLForeignKeys (PkTableName and FkTableName) *********** */
      /* ***************************************************************** */
      strcpy(szPkTableName, TABLE3);
      strcpy(szFkTableName, TABLE4);

      LOGF(
          "\tIII.) SQLForeignKeys -> with primary key tablename: '%s' and foreign key tablename: '%s'",
          szPkTableName, szFkTableName);

      /* Get all the foreign keys in the TABLE3 table,refer to primary key.*/
      /* Wenn Ownername -> "lpSrvr->szValidLogin0, SQL_NTS,"*/
      retcode = SQLForeignKeys(hstmt, NULL, 0, /* Primary qualifier */
      NULL, 0, //lpSrvr->szValidLogin0, SQL_NTS, /* Primary owner     */
          (SQLCHAR*)szPkTableName, SQL_NTS, /* Primary table     */
          NULL, 0, /* Foreign qualifier */
          NULL, 0, //lpSrvr->szValidLogin0, SQL_NTS, /* Foreign owner     */
          (SQLCHAR*)szFkTableName, SQL_NTS); /* Foreign table     */
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLForeignKeys");

      if (retcode == SQL_SUCCESS) lstForeignKeysInfo(henv, hdbc, hstmt);

      retcode = SQLFreeStmt(hstmt, SQL_CLOSE);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLFreeStmt");
      /* --- Drop Table ----------------------------------------------- */

      /* --- Drop Table 4. ------------------------------------------ */
      strcpy(tabname, TABLE4);
      strcpy(drop, "DROP TABLE ");
      strcat(drop, tabname);
      LOGF("Drop Stmt (Table1:SALES_LINE)= '%s'", drop);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)drop, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      /* --- Drop Table 2. ------------------------------------------ */
      strcpy(tabname, TABLE3);
      strcpy(drop, "DROP TABLE ");
      strcat(drop, tabname);
      LOGF("Drop Stmt (Table3:SALES_ORDER)= '%s'", drop);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)drop, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      /* --- Drop Table 2. ------------------------------------------ */
      strcpy(tabname, TABLE2);
      strcpy(drop, "DROP TABLE ");
      strcat(drop, tabname);
      LOGF("Drop Stmt (Table2:EMPLOYEE)= '%s'", drop);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)drop, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");
      /* --- Drop Table 4. ------------------------------------------ */
      strcpy(tabname, TABLE1);
      strcpy(drop, "DROP TABLE ");
      strcat(drop, tabname);
      LOGF("Drop Stmt (Table1:CUSTOMER)= '%s'", drop);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)drop, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");
      /* ---------------------------------------------------------------------har- */
      //free sql handles (stmt, dbc, env)
      FREE_SQLHANDLES
}
END_TEST(testSQLForeignKeys)
