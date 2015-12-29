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

#define TESTNAME "SQLDescribeParam"

#define TABLE "SQLDESCRIBEPARAM"
#define SQLSTMT1 "SELECT * FROM DUAL"
#define MAX_NAME_LEN 1024

BEGIN_TEST(testSQLDescribeParam)
{
      SWORD sErr = 0;
      CHAR buffer[MAX_NAME_LEN + 1];
      CHAR typ_char[MAX_NAME_LEN];
      SDWORD typ_int;
      SWORD typ_smallint;

      SQLLEN cbChar = SQL_NTS;
      SQLLEN cbInt = SQL_NTS;
      SQLLEN cbSmallint = SQL_NTS;

      UWORD ipar;
      SWORD pfSqlType;
      SQLULEN pcbColDef;
      SWORD pibScale;
      SWORD pfNullable;

      /* ------------------------------------------------------------------------- */
      // - Connect -------------------------------------------------------
      INIT_SQLHANDLES
      /* ----------------------------------------------------------------- */
      LOG("1");
      /* --- Create Table --------------------------------------------- */
      retcode = SQLExecDirect(hstmt, (SQLCHAR*)"CREATE TABLE SQLDESCRIBEPARAM (ID INTEGER, NAME CHAR(80), AGE SMALLINT)", SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      /* --- Insert Table --------------------------------------------- */
      /* 1. */
      retcode = SQLPrepare(hstmt, (SQLCHAR*)"INSERT INTO SQLDESCRIBEPARAM VALUES (?, ?, ?)", SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLPrepare");

      retcode = SQLBindParameter(hstmt, 1, SQL_PARAM_INPUT, SQL_C_LONG,
          SQL_INTEGER, 0, 0, &typ_int, sizeof(typ_int), &cbInt);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLBindParameter");

      retcode = SQLBindParameter(hstmt, 2, SQL_PARAM_INPUT, SQL_C_CHAR,
          SQL_CHAR, MAX_NAME_LEN, 0, &typ_char, 0, &cbChar);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLBindParameter");

      retcode = SQLBindParameter(hstmt, 3, SQL_PARAM_INPUT, SQL_C_SHORT,
          SQL_SMALLINT, 0, 0, &typ_smallint, sizeof(typ_smallint),
          &cbSmallint);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLBindParameter");

      typ_int = 9999;
      strcpy(typ_char, "Test DescribeParam");
      typ_smallint = 10;

      retcode = SQLExecute(hstmt);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecute");

      /* --- DescribeParam ------------------------------------------ */
      /* Column 1. */
      ipar = 1;
      retcode = SQLDescribeParam(hstmt, ipar, &pfSqlType, &pcbColDef,
          &pibScale, &pfNullable);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLDescribeParam");

      LOGF("SQLDescribeCol 1. -> (ouput)");
      LOGF("ipar : '%d'\r\n", ipar);
      Get_pfSqlType(pfSqlType, buffer);
      LOGF( "pfSqlType  : '%d'- %s\r\n", pfSqlType, buffer);
      LOGF( "pcbColDef  : '%d'\r\n", pcbColDef);
      LOGF( "pibScale : '%d'\r\n", pibScale);
      Get_pfNullable(pfNullable, buffer);
      LOGF( "pfNullable : '%d'- %s\r\n", pfNullable, buffer);

      /* Column 2. */
      ipar = 2;
      retcode = SQLDescribeParam(hstmt, ipar, &pfSqlType, &pcbColDef,
          &pibScale, &pfNullable);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLDescribeParam");

      LOGF( "SQLDescribeCol 2. -> (ouput)");
      LOGF( "ipar       : '%d'\r\n", ipar);
      Get_pfSqlType(pfSqlType, buffer);
      LOGF( "pfSqlType  : '%d'- %s\r\n", pfSqlType, buffer);
      LOGF( "pcbColDef  : '%d'\r\n", pcbColDef);
      LOGF( "pibScale   : '%d'\r\n", pibScale);
      Get_pfNullable(pfNullable, buffer);
      LOGF( "pfNullable : '%d'- %s\r\n", pfNullable, buffer);

      /* Column 3. */
      ipar = 3;
      retcode = SQLDescribeParam(hstmt, ipar, &pfSqlType, &pcbColDef,
          &pibScale, &pfNullable);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLDescribeParam");

      LOGF( "SQLDescribeCol 3. -> (ouput)");
      LOGF( "ipar       : '%d'\r\n", ipar);
      Get_pfSqlType(pfSqlType, buffer);
      LOGF( "pfSqlType  : '%d'- %s\r\n", pfSqlType, buffer);
      LOGF( "pcbColDef  : '%d'\r\n", pcbColDef);
      LOGF( "pibScale   : '%d'\r\n", pibScale);
      Get_pfNullable(pfNullable, buffer);
      LOGF( "pfNullable : '%d'- %s\r\n", pfNullable, buffer);

      SQLFreeStmt(hstmt, SQL_RESET_PARAMS);

      /* --- Drop Table ----------------------------------------------- */
      retcode = SQLExecDirect(hstmt, (SQLCHAR*)"DROP TABLE SQLDESCRIBEPARAM", SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      // - Disconnect ---------------------------------------------------- */
      FREE_SQLHANDLES

}
END_TEST(testSQLDescribeParam)
