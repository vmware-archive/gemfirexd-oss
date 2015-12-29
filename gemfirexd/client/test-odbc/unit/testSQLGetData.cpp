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
#define TESTNAME "SQLGetdata"
#define TABLE "GETDATA"

#define MAX_NAME_LEN 256

//*-------------------------------------------------------------------------

BEGIN_TEST(testSQLGetData)
{
  /* ------------------------------------------------------------------------- */
      CHAR create[MAX_NAME_LEN + MAX_NAME_LEN + 1];
      CHAR insert[MAX_NAME_LEN + MAX_NAME_LEN + 1];
      CHAR select[MAX_NAME_LEN + MAX_NAME_LEN + 1];
      CHAR drop[MAX_NAME_LEN + 1];
      CHAR tabname[MAX_NAME_LEN];
      CHAR buffer[MAX_NAME_LEN];

      SCHAR rgbValueChar[MAX_NAME_LEN + 1], rgbValueFixed[MAX_NAME_LEN + 1],
          rgbValueDate[MAX_NAME_LEN + 1], rgbValueTime[MAX_NAME_LEN + 1];
      SDOUBLE rgbValueFloat;

      SQLUSMALLINT icol;
      SQLSMALLINT fCType;
      SQLLEN pcbValue;
      /* ---------------------------------------------------------------------har- */
      //init sql handles (stmt, dbc, env)
      INIT_SQLHANDLES

      /* ---------------------------------------------------------------------har- */

      /* --- Create Table --------------------------------------------- */
      strcpy(tabname, TABLE);
      strcpy(create, "CREATE TABLE ");
      strcat(create, tabname);
      strcat(create, " (TYP_CHAR CHAR(30), TYP_FLOAT FLOAT(15),");
      strcat(create,
          " TYP_FIXED DECIMAL(15,2), TYP_DATE DATE, TYP_TIME TIME )");
      LOGF("Create Stmt = '%s'", create);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)create, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      /* --- Insert Table --------------------------------------------- */
      /* --- 1. ---*/
      strcpy(insert, "INSERT INTO ");
      strcat(insert, tabname);
      strcat(insert, " (TYP_CHAR, TYP_FLOAT, TYP_FIXED,");
      strcat(insert, " TYP_DATE,TYP_TIME)");
      strcat(insert, "VALUES ('Dies ein Test.', 123456789,");
      strcat(insert, " 98765321.99, '1994-12-08', '15:45:38')");
      LOGF("Insert Stmt = '%s'", insert);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)insert, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      /* --- Select Table --------------------------------------------- */
      /* --- 1. --- */
      strcpy(select, "SELECT ");
      strcat(select, "TYP_CHAR, TYP_FLOAT, TYP_FIXED, ");
      strcat(select, "TYP_DATE,\"TYP_TIME\"");
      strcat(select, " FROM ");
      strcat(select, tabname);
      LOGF("Select Stmt= '%s'", select);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)select, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      retcode = SQLFetch(hstmt);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLFetch");

      /* Value 1. */
      LOGF(" Get Column 1. (CHAR) --> ");

      icol = 1;
      fCType = SQL_C_CHAR;
      retcode = SQLGetData(hstmt, icol, fCType, rgbValueChar,
          sizeof(rgbValueChar), &pcbValue);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLGetData");

      LOGF("Select Values Col.1: ->'%s'", rgbValueChar);

      /* Value 2. */
      LOGF(" Get Column 2. (FLOAT) --> ");

      icol = 2;
      fCType = SQL_C_DOUBLE;
      retcode = SQLGetData(hstmt, icol, fCType, &rgbValueFloat,
          sizeof(rgbValueFloat), &pcbValue);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLGetData");

      sprintf(buffer, "%.0f", rgbValueFloat);
      LOGF("Select Values Col.2: ->'%s'", buffer);

      /* Value 3. */
      LOGF(" Get Column 3. (FIXED) --> ");

      icol = 3;
      fCType = SQL_C_DEFAULT;
      retcode = SQLGetData(hstmt, icol, fCType, rgbValueFixed,
          sizeof(rgbValueFixed), &pcbValue);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLGetData");

      LOGF("Select Values Col.3: ->'%s'", rgbValueFixed);

      /* Value 4. */
      LOGF(" Get Column 4. (DATE) --> ");

      icol = 4;
      fCType = SQL_C_CHAR;
      retcode = SQLGetData(hstmt, icol, fCType, rgbValueDate,
          sizeof(rgbValueDate), &pcbValue);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLGetData");

      LOGF("Select Values Col.4: ->'%s'", rgbValueDate);

      /* Value 5. */
      LOGF(" Get Column 5. (TIME) --> ");

      icol = 5;
      fCType = SQL_C_CHAR;
      retcode = SQLGetData(hstmt, icol, fCType, rgbValueTime,
          sizeof(rgbValueTime), &pcbValue);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLGetData");

      LOGF("Select Values Col.5: ->'%s'", rgbValueTime);

      retcode = SQLFreeStmt(hstmt, SQL_CLOSE);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLFreeStmt");

      /* --- Drop Table ----------------------------------------------- */
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
END_TEST(testSQLGetData)
