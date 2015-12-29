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

#define TESTNAME "SQLDescribeCol"
#define TABLE "DESCRIBECOL"
#define MAX_NAME_LEN 50

//*-------------------------------------------------------------------------

BEGIN_TEST(testSQLDescribeCol)
{
      /* ------------------------------------------------------------------------- */
      CHAR tabname[MAX_NAME_LEN + 1];
      CHAR create[MAX_NAME_LEN + 1];
      CHAR insert[MAX_NAME_LEN + 1];
      CHAR select[MAX_NAME_LEN + 1];
      CHAR drop[MAX_NAME_LEN + 1];
      CHAR buffer[MAX_NAME_LEN + 1];

      SQLUSMALLINT icol;
      CHAR szColName[MAX_NAME_LEN];
      SQLSMALLINT cbColNameMax;
      SQLSMALLINT pcbColName;
      SQLSMALLINT pfSqlType;
      SQLULEN pcbColDef;
      SQLSMALLINT pibScale;
      SQLSMALLINT pfNullable;
      /* ---------------------------------------------------------------------har- */
      //init sql handles (stmt, dbc, env)
      INIT_SQLHANDLES

      /* ------------------------------------------------------------------------- */

      /* --- Create Table --------------------------------------------- */
      strcpy(tabname, TABLE);
      strcpy(create, "CREATE TABLE ");
      strcat(create, tabname);
      strcat(create, " (ID INTEGER, NAME VARCHAR(80), AGE SMALLINT)");
      LOGF("Create Stmt = '%s'", create);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)create, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      /* --- Insert Table --------------------------------------------- */
      /* 1. */
      strcpy(tabname, TABLE);
      strcpy(insert, "INSERT INTO ");
      strcat(insert, tabname);
      strcat(insert, " VALUES (10, 'TestName', 40)");
      LOGF("Insert Stmt= '%s'", insert);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)insert, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      /* --- Select Table --------------------------------------------- */
      /* 1. */
      strcpy(tabname, TABLE);
      strcpy(select, "SELECT * FROM ");
      strcat(select, tabname);
      LOGF("Select Stmt= '%s'", select);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)select, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      /* --- Describe Columns------------------------------------------ */

      /* Column 1. */
      icol = 1;
      cbColNameMax = MAX_NAME_LEN;
      retcode = SQLDescribeCol(hstmt, icol, (SQLCHAR*)szColName, cbColNameMax,
          &pcbColName, &pfSqlType, &pcbColDef, &pibScale, &pfNullable);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLDescribeCol");

      LOGF("SQLDescribeCol 1. -> (ouput) ");
      LOGF("icol : '%d'", icol);
      LOGF("szColName  : '%s'", szColName);
      LOGF("pcbColName : '%d'", pcbColName);
      Get_pfSqlType(pfSqlType, buffer);
      LOGF("pfSqlType  : '%d'- %s", pfSqlType, buffer);
      LOGF("pcbColDef  : '%d'", pcbColDef);
      LOGF("pibScale : '%d'", pibScale);
      Get_pfNullable(pfNullable, buffer);
      LOGF("pfNullable : '%d'- %s", pfNullable, buffer);

      /* Column 2. */
      icol = 2;
      cbColNameMax = MAX_NAME_LEN;
      retcode = SQLDescribeCol(hstmt, icol, (SQLCHAR*)szColName, cbColNameMax,
          &pcbColName, &pfSqlType, &pcbColDef, &pibScale, &pfNullable);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLDescribeCol");

      LOGF("SQLDescribeCol 2. -> (ouput) ");
      LOGF("icol       : '%d'", icol);
      LOGF("szColName  : '%s'", szColName);
      LOGF("pcbColName : '%d'", pcbColName);
      Get_pfSqlType(pfSqlType, buffer);
      LOGF("pfSqlType  : '%d'- %s", pfSqlType, buffer);
      LOGF("pcbColDef  : '%d'", pcbColDef);
      LOGF("pibScale   : '%d'", pibScale);
      Get_pfNullable(pfNullable, buffer);
      LOGF("pfNullable : '%d'- %s", pfNullable, buffer);

      /* Column 3. */
      icol = 3;
      cbColNameMax = MAX_NAME_LEN;
      retcode = SQLDescribeCol(hstmt, icol, (SQLCHAR*)szColName, cbColNameMax,
          &pcbColName, &pfSqlType, &pcbColDef, &pibScale, &pfNullable);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLDescribeCol");

      LOGF("SQLDescribeCol 3. -> (ouput) ");
      LOGF("icol       : '%d'", icol);
      LOGF("szColName  : '%s'", szColName);
      LOGF("pcbColName : '%d'", pcbColName);
      Get_pfSqlType(pfSqlType, buffer);
      LOGF("pfSqlType  : '%d'- %s", pfSqlType, buffer);
      LOGF("pcbColDef  : '%d'", pcbColDef);
      LOGF("pibScale   : '%d'", pibScale);
      Get_pfNullable(pfNullable, buffer);
      LOGF("pfNullable : '%d'- %s", pfNullable, buffer);

      retcode = SQLFreeStmt(hstmt, SQL_CLOSE);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLFreeStmt");
      /* --- Drop Table ----------------------------------------------- */
#ifdef DROP_TABLE

      strcpy(tabname, TABLE);
      strcpy(drop, "DROP TABLE ");
      strcat(drop, tabname);
      LOGF("Drop Stmt= '%s'", drop);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)drop, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");
#endif
      /* ---------------------------------------------------------------------har- */
      //free sql handles (stmt, dbc, env)
      FREE_SQLHANDLES

}
END_TEST(testSQLDescribeCol)
