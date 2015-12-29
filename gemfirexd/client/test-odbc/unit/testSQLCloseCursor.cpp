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

#define TESTNAME "SQLCloseCursor"

#define TABLE1  "TABCLOSECUR1"
#define TABLE2  "TABCLOSECUR2"

#define SQLSTMT1 "SELECT * FROM DUAL"

#define MAX_ROWS 5      /* Max. Zeilen      */
#define MAX_NAME_LEN 256
//*-------------------------------------------------------------------------

BEGIN_TEST(testSQLCloseCursor)
{
      CHAR create[MAX_NAME_LEN + 1];
      CHAR drop[MAX_NAME_LEN + 1];
      CHAR insert[MAX_NAME_LEN + 1];
      CHAR select[MAX_NAME_LEN + 1];

      CHAR szCharData[MAX_NAME_LEN];
      SQLSMALLINT i, j, count = 0;

      /* ------------------------------------------------------------------------- */
      //initialize the sql handles
      INIT_SQLHANDLES

      /* ----------------------------------------------------------------- */

      /* --- Create Table 1. --------------------------------------------- */
      strcpy(create, "CREATE TABLE ");
      strcat(create, TABLE1);
      strcat(create, " ( NAME CHAR(50), AGE INTEGER, ADRESSE CHAR(80))\0");
      LOGF("Create Stmt 1. = '%s'", create);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)create, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      retcode = SQLCloseCursor(hstmt);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_ERROR, retcode,
          "SQLCloseCursor");
      //TODO: Need to check if we can allow SQLCloseCursor if result set is not open


      /* --- Create Table 2. ------------------------------------------- */
      strcpy(create, "CREATE TABLE ");
      strcat(create, TABLE2);
      strcat(create, " ( KUNDE CHAR(50), ADRESSE CHAR(80), PLZ INTEGER) \0");
      LOGF("Create Stmt 2. = '%s'", create);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)create, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      retcode = SQLCloseCursor(hstmt);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_ERROR, retcode,
          "SQLCloseCursor");
      //TODO: Need to check if we can allow SQLCloseCursor if result set is not open

      /* --- Insert Table 1. ------------------------------------------- */
      strcpy(insert, "INSERT INTO ");
      strcat(insert, TABLE1);
      strcat(insert, " VALUES ('Heinrich', 44, 'Alt-Moabit 96, Berlin')");

      for (j = 0; j < MAX_ROWS; j++) {
        LOGF("Insert Stmt 1. = <%d.> '%s'", j + 1, insert);

        retcode = SQLExecDirect(hstmt, (SQLCHAR*)insert, SQL_NTS);
        DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
            "SQLExecDirect");

        retcode = SQLCloseCursor(hstmt);
        DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_ERROR, retcode,
            "SQLCloseCursor");
        //TODO: Need to check if we can allow SQLCloseCursor if result set is not open
      }
      /* --- Insert Table 2. ------------------------------------------- */
      strcpy(insert, "INSERT INTO ");
      strcat(insert, TABLE2);
      strcat(insert, " VALUES ('Matzke', 'Turmstr. 98, Berlin', 10559)");

      for (j = 0; j < MAX_ROWS; j++) {
        LOGF("Insert Stmt 2. = <%d.> '%s'", j + 1, insert);

        retcode = SQLExecDirect(hstmt, (SQLCHAR*)insert, SQL_NTS);
        DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
            "SQLExecDirect");

        retcode = SQLCloseCursor(hstmt);
        DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_ERROR, retcode,
            "SQLCloseCursor");
        //TODO: Need to check if we can allow SQLCloseCursor if result set is not open
      }

      /* --- Select Table 1. ------------------------------------------- */
      strcpy(select, "SELECT * FROM ");
      strcat(select, TABLE1);
      LOGF("Select Stmt 1. = '%s'", select);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)select, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");
      count = 1;
      i = 1;
      while (retcode != SQL_NO_DATA_FOUND) {
        retcode = SQLFetch(hstmt);
        if (retcode != SQL_NO_DATA_FOUND) {
          DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
                      "SQLFetch");
          retcode = SQLGetData(hstmt, i, SQL_C_CHAR, szCharData,
              sizeof(szCharData), NULL);
          DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
              "SQLGetData");
          LOGF(" Column %d (Pos.%d) = '%s'", i, count, szCharData);
          count++;
        }
      }
      retcode = SQLCloseCursor(hstmt);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLCloseCursor");

      /* --- Select Table 2. ------------------------------------------- */
      strcpy(select, "SELECT * FROM ");
      strcat(select, TABLE2);
      LOGF("Select Stmt 2. = '%s'", select);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)select, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      retcode = SQLCloseCursor(hstmt);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLCloseCursor");

      retcode = SQLFetch(hstmt);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_ERROR, retcode,
          "SQLFetch"); //as the close cursor is called before this

      /* --- Drop Table 1. -------------------------------------------- */
      strcpy(drop, "DROP TABLE ");
      strcat(drop, TABLE1);
      LOGF("Drop Stmt 1.= '%s'", drop);

      retcode = SQLCloseCursor(hstmt);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_ERROR, retcode,
          "SQLCloseCursor");
      //TODO: Need to check if we can allow SQLCloseCursor if result set is not open

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)drop, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      /* --- Drop Table 2. -------------------------------------------- */
      strcpy(drop, "DROP TABLE ");
      strcat(drop, TABLE2);
      LOGF("Drop Stmt 2.= '%s'", drop);

      retcode = SQLCloseCursor(hstmt);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_ERROR, retcode,
          "SQLCloseCursor");
      //TODO: Need to check if we can allow SQLCloseCursor if result set is not open

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)drop, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      /* - Disconnect ---------------------------------------------------- */
      //free sql handles
      FREE_SQLHANDLES

}
END_TEST(testSQLCloseCursor)
