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

#define TESTNAME "SQLColAttribute"

#define SQLSTMT1 "SELECT * FROM DUAL"
#define TABLE "COLATRIBUTE"
#define MAX_NAME_LEN 256
//*-------------------------------------------------------------------------

BEGIN_TEST(testSQLColAttribute)
{
  /* ------------------------------------------------------------------------- */
      CHAR tabname[MAX_NAME_LEN + 1];
      CHAR create[MAX_NAME_LEN + 1];
      CHAR insert[MAX_NAME_LEN + 1];
      CHAR select[MAX_NAME_LEN + 1];
      CHAR drop[MAX_NAME_LEN + 1];
      CHAR buffer[MAX_NAME_LEN + 1];

      SQLSMALLINT icol = 1;
      SQLSMALLINT fDescType;
      PTR rgbDesc;
      CHAR rgbDescChar[MAX_NAME_LEN];
      SQLSMALLINT cbDescMax;
      SQLSMALLINT pcbDesc;
      SQLLEN pfDesc;

      /* ------------------------------------------------------------------------- */
      //initialize the sql handles
      INIT_SQLHANDLES

      /* ----------------------------------------------------------------- */

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

      /* --- SQLColAttributes ---------------------------------------- */

      sprintf(buffer, "%d", icol);
      LOGF("SQLColAttributes Column: %s -> (ouput) ", buffer);

      /* ***** 1. SQL_COLUMN_AUTO_INCREMNT */
      icol = 1;
      fDescType = SQL_COLUMN_AUTO_INCREMENT;
      cbDescMax = MAX_NAME_LEN;
      retcode = SQLColAttribute(hstmt, icol, fDescType, (PTR) & rgbDesc,
          cbDescMax, &pcbDesc, &pfDesc);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLColAttribute 1.");

      Get_BoolValue((SQLSMALLINT)pfDesc, buffer);
      LOGF("1.SQL_COLUMN_AUTO_INCREMT: '%s'", buffer);

      /* ***** 2. SQL_COLUMN_CASE_SENSITIVE */
      icol = 1;
      fDescType = SQL_COLUMN_CASE_SENSITIVE;
      cbDescMax = MAX_NAME_LEN;
      retcode = SQLColAttribute(hstmt, icol, fDescType, (PTR) & rgbDesc,
          cbDescMax, &pcbDesc, &pfDesc);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLColAttribute 2.");

      Get_BoolValue((SQLSMALLINT)pfDesc, buffer);
      LOGF("2.SQL_COLUMN_CASE_SENSITIVE: '%s'", buffer);

      /* ***** 3. SQL_COLUMN_COUNT */
      icol = 1;
      fDescType = SQL_COLUMN_COUNT;
      cbDescMax = MAX_NAME_LEN;
      retcode = SQLColAttribute(hstmt, icol, fDescType, (PTR) & rgbDesc,
          cbDescMax, &pcbDesc, &pfDesc);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLColAttribute 3.");

      LOGF("3.SQL_COLUMN_COUNT: '%d'", pfDesc);

      /* ***** 4. SQL_COLUMN_DISPLAY_SIZE */
      icol = 1;
      fDescType = SQL_COLUMN_DISPLAY_SIZE;
      cbDescMax = MAX_NAME_LEN;
      retcode = SQLColAttribute(hstmt, icol, fDescType, (PTR) & rgbDesc,
          cbDescMax, &pcbDesc, &pfDesc);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLColAttribute 4.");
      LOGF("4.SQL_COLUMN_DISPLAY_SIZE: '%d'", pfDesc);

      /* ***** 5. SQL_COLUMN_LABEL (ODBC 2.0)*/
      icol = 1;
      fDescType = SQL_COLUMN_LABEL;
      cbDescMax = MAX_NAME_LEN;
      strcpy(rgbDescChar, "\0");
#if (ODBCVER >= 0x0200)
      retcode = SQLColAttribute(hstmt, icol, fDescType,(PTR)&rgbDescChar, cbDescMax,
          &pcbDesc, &pfDesc);
#endif
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLColAttribute 5.");
      LOGF("5.SQL_COLUMN_LABEL (ODBC 2.0): '%s'", rgbDescChar);

      /* ***** 6. SQL_COLUMN_LENGTH */
      icol = 1;
      fDescType = SQL_COLUMN_LENGTH;
      cbDescMax = MAX_NAME_LEN;
      retcode = SQLColAttribute(hstmt, icol, fDescType, (PTR) & rgbDesc,
          cbDescMax, &pcbDesc, &pfDesc);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLColAttribute 6.");
      LOGF("6.SQL_COLUMN_LENGTH: '%d'", pfDesc);

      /* ***** 7. SQL_COLUMN_MONEY */
      icol = 1;
      fDescType = SQL_COLUMN_MONEY;
      cbDescMax = MAX_NAME_LEN;
      retcode = SQLColAttribute(hstmt, icol, fDescType, (PTR) & rgbDesc,
          cbDescMax, &pcbDesc, &pfDesc);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLColAttribute 7.");

      Get_BoolValue((SQLSMALLINT)pfDesc, buffer);
      LOGF("7.SQL_COLUMN_MONEY: '%s'", buffer);

      /* ***** 8. SQL_COLUMN_NAME */
      icol = 1;
      fDescType = SQL_COLUMN_NAME;
      cbDescMax = MAX_NAME_LEN;
      strcpy(rgbDescChar, "\0");
      retcode = SQLColAttribute(hstmt, icol, fDescType, (PTR) & rgbDescChar,
          cbDescMax, &pcbDesc, &pfDesc);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLColAttribute 8.");
      LOGF("8.SQL_COLUMN_NAME: '%s'", rgbDescChar);

      /* ***** 9. SQL_COLUMN_NULLABLE */
      icol = 1;
      fDescType = SQL_COLUMN_NULLABLE;
      cbDescMax = MAX_NAME_LEN;
      retcode = SQLColAttribute(hstmt, icol, fDescType, (PTR) & rgbDesc,
          cbDescMax, &pcbDesc, &pfDesc);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLColAttribute 9.");

      Get_pfNullable((SQLSMALLINT)pfDesc, buffer);
      LOGF("9.SQL_COLUMN_NULLABLE: '%s'", buffer);

      /* *****10. SQL_COLUMN_OWNER_NAME (ODBC 2.0) */
      icol = 1;
      fDescType = SQL_COLUMN_OWNER_NAME;
      cbDescMax = MAX_NAME_LEN;
      strcpy(rgbDescChar, "\0");
#if (ODBCVER >= 0x0200)
      retcode = SQLColAttribute(hstmt, icol, fDescType,(PTR)&rgbDescChar, cbDescMax,
          &pcbDesc, &pfDesc);
#endif
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLColAttribute 10.");
      LOGF("10.SQL_COLUMN_OWNER_NAME (ODBC 2.0): '%s'", rgbDescChar);

      /* *****11. SQL_COLUMN_PRECISION */
      icol = 1;
      fDescType = SQL_COLUMN_PRECISION;
      cbDescMax = MAX_NAME_LEN;
      retcode = SQLColAttribute(hstmt, icol, fDescType, (PTR) & rgbDesc,
          cbDescMax, &pcbDesc, &pfDesc);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLColAttribute 11.");
      LOGF("11.SQL_COLUMN_PRECISION: '%d'", pfDesc);

      /* *****12. SQL_COLUMN_QUALIFIER_NAME (ODBC 2.0) */
      icol = 1;
      fDescType = SQL_COLUMN_QUALIFIER_NAME;
      cbDescMax = MAX_NAME_LEN;
      strcpy(rgbDescChar, "\0");
#if (ODBCVER >= 0x0200)
      retcode = SQLColAttribute(hstmt, icol, fDescType,(PTR)&rgbDescChar, cbDescMax,
          &pcbDesc, &pfDesc);
#endif
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLColAttribute 12.");
      LOGF("12.SQL_COLUMN_QUALIFIER_NAME (ODBC 2.0): '%s'",
          rgbDescChar);

      /* *****13. SQL_COLUMN_SCALE */
      icol = 1;
      fDescType = SQL_COLUMN_SCALE;
      cbDescMax = MAX_NAME_LEN;
      retcode = SQLColAttribute(hstmt, icol, fDescType, (PTR) & rgbDesc,
          cbDescMax, &pcbDesc, &pfDesc);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLColAttribute 13.");
      LOGF("13.SQL_COLUMN_SCALE: '%d'", pfDesc);

      /* *****14. SQL_COLUMN_SEARCHABLE */
      icol = 1;
      fDescType = SQL_COLUMN_SEARCHABLE;
      cbDescMax = MAX_NAME_LEN;
      retcode = SQLColAttribute(hstmt, icol, fDescType, (PTR) & rgbDesc,
          cbDescMax, &pcbDesc, &pfDesc);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLColAttribute 14.");

      Get_Searchable((SQLSMALLINT)pfDesc, buffer);
      LOGF("14.SQL_COLUMN_SEARCHABLE: '%s'", buffer);

      /* *****15. SQL_COLUMN_TABLE_NAME (ODBC 2.0) */
      icol = 1;
      fDescType = SQL_COLUMN_TABLE_NAME;
      cbDescMax = MAX_NAME_LEN;
      strcpy(rgbDescChar, "\0");
#if (ODBCVER >= 0x0200)
      retcode = SQLColAttribute(hstmt, icol, fDescType,(PTR)&rgbDescChar, cbDescMax,
          &pcbDesc, &pfDesc);
#endif
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLColAttribute 15.");
      LOGF("15.SQL_COLUMN_TABLE_NAME (ODBC 2.0): '%s'", rgbDescChar);

      /* *****16. SQL_COLUMN_TYPE */
      icol = 1;
      fDescType = SQL_COLUMN_TYPE;
      cbDescMax = MAX_NAME_LEN;
      retcode = SQLColAttribute(hstmt, icol, fDescType, (PTR) & rgbDesc,
          cbDescMax, &pcbDesc, &pfDesc);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLColAttribute 16.");
      LOGF("16.SQL_COLUMN_TYPE: '%d'", pfDesc);

      /* *****17. SQL_COLUMN_TYPE_NAME */
      icol = 1;
      fDescType = SQL_COLUMN_TYPE_NAME;
      cbDescMax = MAX_NAME_LEN;
      strcpy(rgbDescChar, "\0");
      retcode = SQLColAttribute(hstmt, icol, fDescType, (PTR) & rgbDescChar,
          cbDescMax, &pcbDesc, &pfDesc);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLColAttribute 17.");
      LOGF("17.SQL_COLUMN_TYPE_NAME: '%s'", rgbDescChar);

      /* *****18. SQL_COLUMN_UNSIGNED */
      icol = 1;
      fDescType = SQL_COLUMN_UNSIGNED;
      cbDescMax = MAX_NAME_LEN;
      retcode = SQLColAttribute(hstmt, icol, fDescType, (PTR) & rgbDesc,
          cbDescMax, &pcbDesc, &pfDesc);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLColAttribute 18.");

      Get_BoolValue((SQLSMALLINT)pfDesc, buffer);
      LOGF("18.SQL_COLUMN_UNSIGNED: '%s'", buffer);

      /* *****19. SQL_COLUMN_UPDATABLE */
      icol = 1;
      fDescType = SQL_COLUMN_UPDATABLE;
      cbDescMax = MAX_NAME_LEN;
      retcode = SQLColAttribute(hstmt, icol, fDescType, (PTR) & rgbDesc,
          cbDescMax, &pcbDesc, &pfDesc);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLColAttribute 19.");

      Get_Updatable((SQLSMALLINT)pfDesc, buffer);
      LOGF("19.SQL_COLUMN_UPDATABLE: '%s'", buffer);

      retcode = SQLCloseCursor(hstmt);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLCloseCursor");

      /* --- Drop Table ----------------------------------------------- */
      strcpy(tabname, TABLE);
      strcpy(drop, "DROP TABLE ");
      strcat(drop, tabname);
      LOGF("Drop Stmt= '%s'", drop);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)drop, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      /* - Disconnect ---------------------------------------------------- */
      //free sql handles
      FREE_SQLHANDLES
}
END_TEST(testSQLColAttribute)
