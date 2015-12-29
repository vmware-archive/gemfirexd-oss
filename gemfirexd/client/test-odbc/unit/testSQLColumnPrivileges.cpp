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

#define TESTNAME "SQLCOLUMNPRIVILEGES"
#define TABLE "TABSQLCOLPRIV"

#define USER1 "gemfirexduser1"
#define USER2 "gemfirexduser2"

#define PASSWORD1 "gemfirexduser1"
#define PASSWORD2 "gemfirexduser2"

#define MAX_NAME_LEN 512

#define STR_LEN 254+1
#define REM_LEN 254+1

#define NULL_VALUE "<NULL>"

//*-------------------------------------------------------------------------

/* ------------------------------------------------------------------------- */
RETCODE lstColumnPrivilegesInfo(SQLHENV henv, SQLHDBC hdbc, SQLHSTMT hstmt)
{
  RETCODE rc;
  SQLSMALLINT sErr = 0;
  /* ------------------------------------------------------------------------- */
  /* Declare storage locations for result set data */
  CHAR szTableQualifier[STR_LEN], szTableOwner[STR_LEN], szTableName[STR_LEN],
      szColumnName[STR_LEN], szGrantor[STR_LEN], szGrantee[STR_LEN],
      szPrivilege[STR_LEN], szIsGrantable[STR_LEN];

  /* Declare storage locations for bytes available to return */
  SQLLEN cbTableQualifier, cbTableOwner, cbTableName, cbColumnName, cbGrantor,
      cbGrantee, cbPrivilege, cbIsGrantable;

  SQLSMALLINT count = 0;
  /* ---------------------------------------------------------------------har- */
  /* Bind columns in result set to storage locations */
  SQLBindCol(hstmt, 1, SQL_C_CHAR, szTableQualifier, STR_LEN,
      &cbTableQualifier);
  SQLBindCol(hstmt, 2, SQL_C_CHAR, szTableOwner, STR_LEN, &cbTableOwner);
  SQLBindCol(hstmt, 3, SQL_C_CHAR, szTableName, STR_LEN, &cbTableName);
  SQLBindCol(hstmt, 4, SQL_C_CHAR, szColumnName, STR_LEN, &cbColumnName);
  SQLBindCol(hstmt, 5, SQL_C_CHAR, szGrantor, STR_LEN, &cbGrantor);
  SQLBindCol(hstmt, 6, SQL_C_CHAR, szGrantee, STR_LEN, &cbGrantee);
  SQLBindCol(hstmt, 7, SQL_C_CHAR, szPrivilege, STR_LEN, &cbPrivilege);
  SQLBindCol(hstmt, 8, SQL_C_CHAR, szIsGrantable, STR_LEN, &cbIsGrantable);

  rc = lst_ColumnNames(henv, hdbc, hstmt, 8);

  while (TRUE) {
    count++;

    rc = SQLFetch(hstmt);
    /* DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,"SQLFetch");*/

    if (rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO) {
      /* Process fetched data */
      if (cbTableQualifier == SQL_NULL_DATA) strcpy(szTableQualifier,
          NULL_VALUE);
      if (cbTableOwner == SQL_NULL_DATA) strcpy(szTableOwner, NULL_VALUE);
      if (cbTableName == SQL_NULL_DATA) strcpy(szTableName, NULL_VALUE);
      if (cbColumnName == SQL_NULL_DATA) strcpy(szColumnName, NULL_VALUE);
      if (cbGrantor == SQL_NULL_DATA) strcpy(szGrantor, NULL_VALUE);
      if (cbGrantee == SQL_NULL_DATA) strcpy(szGrantee, NULL_VALUE);
      if (cbPrivilege == SQL_NULL_DATA) strcpy(szPrivilege, NULL_VALUE);
      if (cbIsGrantable == SQL_NULL_DATA) strcpy(szIsGrantable, NULL_VALUE);

      LOGF("Column %d : '%s','%s','%s','%s','%s','%s','%s','%s'",
          count, szTableQualifier, szTableOwner, szTableName, szColumnName, szGrantor, szGrantee, szPrivilege, szIsGrantable);
    }
    else {
      /* DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,"SQLFetch"); */
      break;
    }
  }
  return TRUE;
}
/* ------------------------------------------------------------------------- */

BEGIN_TEST(testSQLColumnPrivileges)
{
  /* ------------------------------------------------------------------------- */
      CHAR tabname[MAX_NAME_LEN + 1];
      CHAR create[MAX_NAME_LEN + 1];
      CHAR grant[MAX_NAME_LEN + 1];
      CHAR drop[MAX_NAME_LEN + 1];
      CHAR createuser[MAX_NAME_LEN + 1];

      /* ---------------------------------------------------------------------har- */
      //init sql handles (stmt, dbc, env)
      INIT_SQLHANDLES

      /* ------------------------------------------------------------------------- */

      /* --- Create User USER1 ---------------------------------------- */
      strcpy(createuser, "call sys.create_user('");
      strcat(createuser, USER1);
      strcat(createuser, "', '");
      strcat(createuser, PASSWORD1);
      strcat(createuser, "')");
      LOGF("Create user statement = '%s'", createuser);
      retcode = SQLExecDirect(hstmt, (SQLCHAR*)createuser, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      /* --- Create User USER2 ---------------------------------------- */
      strcpy(createuser, "call sys.create_user('");
      strcat(createuser, USER2);
      strcat(createuser, "', '");
      strcat(createuser, PASSWORD2);
      strcat(createuser, "')");
      LOGF("Create user statement = '%s'", createuser);
      retcode = SQLExecDirect(hstmt, (SQLCHAR*)createuser, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      /* --- Create Table  -------------------------------------------- */
      strcpy(tabname, TABLE);
      strcpy(create, "CREATE TABLE ");
      strcat(create, tabname);
      strcat(create, " (CUST_ID INTEGER, CUST_NAME CHAR(30) )");
      LOGF("Create Stmt (Table: "TABLE")= '%s'", create);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)create, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      /* --- Grant Table 1. (USER1) ------------------------------------- */
      strcpy(tabname, TABLE);
      strcpy(grant, "GRANT SELECT (CUST_NAME) ON ");
      strcat(grant, tabname);
      strcat(grant, " TO "USER1);
      LOGF("Grant Stmt 1.(Table: "TABLE")= '%s'", grant);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)grant, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      /* --- Grant Table 2. (USER2) ------------------------------------- */
      strcpy(tabname, TABLE);
      strcpy(grant, "GRANT SELECT (CUST_NAME), UPDATE (CUST_NAME) ON ");
      strcat(grant, tabname);
      strcat(grant, " TO "USER2);
      LOGF("Grant Stmt 2.(Table: "TABLE")= '%s'", grant);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)grant, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");
      /* ------------------------------------------------------------------------- */
      /* ***************************************************************** */
      /* *** I. SQLColumnPrivileges ************************************** */
      /* ***************************************************************** */
      LOGF("I.) SQLColumnPrivileges -> (TableName: "TABLE" )");

      retcode = SQLColumnPrivileges(hstmt, NULL, 0, /* Table qualifier */
      NULL, SQL_NTS, /* Table owner     */
      (SQLCHAR*)TABLE, SQL_NTS, /* Table name      */
      (SQLCHAR*)"%", SQL_NTS); /* Column name      */
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLColumnPrivileges");

      if (retcode == SQL_SUCCESS) lstColumnPrivilegesInfo(henv, hdbc, hstmt);

      retcode = SQLFreeStmt(hstmt, SQL_CLOSE);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLFreeStmt");

      /* --- Drop Table ----------------------------------------------- */
      strcpy(tabname, TABLE);
      strcpy(drop, "DROP TABLE ");
      strcat(drop, tabname);
      LOGF("Drop Stmt (Table:"TABLE")= '%s'", drop);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)drop, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");
      /* ---------------------------------------------------------------------har- */
      //free sql handles (stmt, dbc, env)
      FREE_SQLHANDLES

}
END_TEST(testSQLColumnPrivileges)
