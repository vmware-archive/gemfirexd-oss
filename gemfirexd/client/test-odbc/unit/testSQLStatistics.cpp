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

#define TESTNAME "SQLStatistics"
#define TABLE "TABSTATIST"

#define MAX_NAME_LEN 50

#define STR_LEN 128+1
#define REM_LEN 254+1

#define NULL_VALUE "<NULL>"

//*-------------------------------------------------------------------------

BEGIN_TEST(testSQLStatistics)
{
  /* ------------------------------------------------------------------------- */
      CHAR tabname[MAX_NAME_LEN + 1];
      CHAR colname[MAX_NAME_LEN + 1];
      CHAR create[MAX_NAME_LEN + 1];
      CHAR drop[MAX_NAME_LEN + 1];
      /* Declare storage locations for result set data */

      CHAR szQualifier[STR_LEN], szOwner[STR_LEN], szTableName[STR_LEN];
      CHAR szIndexQualifier[STR_LEN], szIndexName[STR_LEN];
      CHAR szColumnName[STR_LEN], szCollation[STR_LEN];
      CHAR szFilterCondition[STR_LEN];

      CHAR szType[STR_LEN];

      SQLSMALLINT NonUnique, Type, SeqInIndex;

      SQLINTEGER Cardinality, Pages;

      SQLINTEGER fUnique, fAccuracy;

      /* Declare storage locations for bytes available to return */

      SQLLEN cbQualifier, cbOwner, cbTableName, cbNonUnique,
          cbIndexQualifier;
      SQLLEN cbIndexName, cbType, cbSeqInIndex, cbColumnName, cbCollation;
      SQLLEN cbCardinality, cbPages, cbFilterCondition;

      SQLSMALLINT count = 0;
      /* ---------------------------------------------------------------------har- */

      //init sql handles (stmt, dbc, env)
      INIT_SQLHANDLES

      /* ------------------------------------------------------------------------- */
      /* --- Create Table --------------------------------------------- */
      strcpy(tabname, TABLE);
      strcpy(create, "CREATE TABLE ");
      strcat(create, tabname);
      strcat(create,
          " (ID INTEGER, NAME CHAR(80), VNAME CHAR(80), STR CHAR(120), ORT CHAR(60), AGE SMALLINT, GEHALT DECIMAL(8,2))");
      LOGF("Create Stmt = '%s'\r\n", create);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)create, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      /* --- Create Index --------------------------------------------- */
      strcpy(tabname, TABLE);
      strcpy(colname, "ID");
      strcpy(create, "CREATE UNIQUE INDEX ");
      strcat(create, tabname);
      strcat(create, "_");
      strcat(create, colname);
      strcat(create, " ON ");
      strcat(create, tabname);
      strcat(create, "(");
      strcat(create, colname);
      strcat(create, ")");
      LOGF("Create Index Stmt = '%s'\r\n", create);

       retcode = SQLExecDirect(hstmt, (SQLCHAR*)create, SQL_NTS);
       DIAGRECCHECK(SQL_HANDLE_STMT, hstmt,1, SQL_SUCCESS, retcode,"SQLExecDirect");

       /* ------------------------------------------------------------------------- */
      /* All qualifiers, all owners, EMPLOYEE table, all columns */

      fUnique = SQL_INDEX_ALL; /* or. SQL_INDEX_UNIQUE */
      fAccuracy = SQL_QUICK; /* or. SQL_ENSURE       */

      retcode = SQLStatistics(hstmt, NULL, 0, (SQLCHAR*)"GFXDODBC",SQL_NTS,
          (SQLCHAR*)TABLE, SQL_NTS, SQL_INDEX_ALL, SQL_ENSURE);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLStatistics");

      if (retcode == SQL_SUCCESS) {
        /* Bind columns in result set to storage locations */

        SQLBindCol(hstmt, 1, SQL_C_CHAR, szQualifier, STR_LEN, &cbQualifier);
        SQLBindCol(hstmt, 2, SQL_C_CHAR, szOwner, STR_LEN, &cbOwner);
        SQLBindCol(hstmt, 3, SQL_C_CHAR, szTableName, STR_LEN, &cbTableName);
        SQLBindCol(hstmt, 4, SQL_C_SHORT, &NonUnique, 0, &cbNonUnique);
        SQLBindCol(hstmt, 5, SQL_C_CHAR, szIndexQualifier, STR_LEN,
            &cbIndexQualifier);
        SQLBindCol(hstmt, 6, SQL_C_CHAR, szIndexName, STR_LEN, &cbIndexName);
        SQLBindCol(hstmt, 7, SQL_C_SHORT, &Type, 0, &cbType);
        SQLBindCol(hstmt, 8, SQL_C_SHORT, &SeqInIndex, 0, &cbSeqInIndex);
        SQLBindCol(hstmt, 9, SQL_C_CHAR, szColumnName, STR_LEN, &cbColumnName);
        SQLBindCol(hstmt, 10, SQL_C_CHAR, szCollation, STR_LEN, &cbCollation);
        SQLBindCol(hstmt, 11, SQL_C_LONG, &Cardinality, 0, &cbCardinality);
        SQLBindCol(hstmt, 12, SQL_C_LONG, &Pages, 0, &cbPages);
        SQLBindCol(hstmt, 13, SQL_C_CHAR, szFilterCondition, STR_LEN,
            &cbFilterCondition);

        retcode = lst_ColumnNames(henv, hdbc, hstmt, 13);

        while (TRUE) {
          count++;

          retcode = SQLFetch(hstmt);
          //DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
            //  "SQLFetch");

          if (retcode == SQL_ERROR || retcode == SQL_SUCCESS_WITH_INFO) {
            /* Show Error */
            DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
                "SQLError");
          }
          if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO) {
            /* Process fetched data */
            printf("\tColumn %d : ", count);

            /* *** TABLE_QUALIFIER ------------------- *** */
            if (cbQualifier == SQL_NULL_DATA) strcpy(szQualifier, NULL_VALUE);
            printf("'%s',", szQualifier);

            /* *** TABLE_OWNER ------------------- *** */
            if (cbOwner == SQL_NULL_DATA) strcpy(szOwner, NULL_VALUE);
            printf("'%s',", szOwner);

            /* *** TABLE_NAME ------------------- *** */
            printf("'%s',", szTableName);

            /* *** NON_UNIQUE ------------------- *** */
            if (cbNonUnique == SQL_NULL_DATA)
              printf("'%s',", NULL_VALUE);
            else
              printf("'%d',", NonUnique);

            /* *** INDEX_QUALIFIER ------------------- *** */
            if (cbIndexQualifier == SQL_NULL_DATA) strcpy(szIndexQualifier,
                NULL_VALUE);
            printf("'%s',", szIndexQualifier);

            /* *** INDEX_NAME ------------------- *** */
            if (cbIndexName == SQL_NULL_DATA) strcpy(szIndexName, NULL_VALUE);
            printf("'%s',", szIndexName);

            /* *** TYPE ------------------- *** */
            /* Type of information */
            switch (Type) {
              case SQL_TABLE_STAT:
                strcpy(szType, "SQL_TABLE_STAT");
                break;
              case SQL_INDEX_CLUSTERED:
                strcpy(szType, "SQL_INDEX_CLUSTERED");
                break;
              case SQL_INDEX_HASHED:
                strcpy(szType, "SQL_INDEX_HASHED");
                break;
              case SQL_INDEX_OTHER:
                strcpy(szType, "SQL_INDEX_OTHER");
                break;
            }
            printf("'%s',", szType);

            /* *** SEQ_IN_INDEX ------------------- *** */
            if (cbSeqInIndex == SQL_NULL_DATA)
              printf("'%s',", NULL_VALUE);
            else
              printf("'%d',", SeqInIndex);

            /* *** COLUMN_NAME ------------------- *** */
            if (cbColumnName == SQL_NULL_DATA) strcpy(szColumnName,
                NULL_VALUE);
            printf("'%s',", szColumnName);

            /* *** COLLATION ------------------- *** */
            if (cbCollation == SQL_NULL_DATA) strcpy(szCollation, NULL_VALUE);
            printf("'%s',", szCollation);

            /* *** CARDINALITY ------------------- *** */
            if (cbCardinality == SQL_NULL_DATA)
              printf("'%s',", NULL_VALUE);
            else
              printf("'%d',", Cardinality);

            /* *** PAGES ------------------- *** */
            if (cbPages == SQL_NULL_DATA)
              printf("'%s',", NULL_VALUE);
            else
              printf("'%d',", Pages);

            /* *** FILTER_CONDITION ------------------- *** */
            if (cbFilterCondition == SQL_NULL_DATA) strcpy(szFilterCondition,
                NULL_VALUE);
            printf("'%s' \r\n", szFilterCondition);

          }
          else {
            break;
          }
        }
      }
      retcode = SQLFreeStmt(hstmt, SQL_CLOSE);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLFreeStmt");

      /* --- Drop Index ----------------------------------------------- */
      strcpy(colname, "ID");
      strcpy(tabname, TABLE);
      strcpy(drop, "DROP INDEX ");
      strcat(drop, tabname);
      strcat(drop, "_");
      strcat(drop, colname);
      LOGF("Drop Index Stmt= '%s'\r\n", drop);
      // --- mysql ???
       retcode = SQLExecDirect(hstmt, (SQLCHAR*)drop, SQL_NTS);
       DIAGRECCHECK(SQL_HANDLE_STMT, hstmt,1, SQL_SUCCESS, retcode,"SQLExecDirect");

      /* --- Drop Table ----------------------------------------------- */
      strcpy(tabname, TABLE);
      strcpy(drop, "DROP TABLE ");
      strcat(drop, tabname);
      LOGF("Drop Stmt= '%s'\r\n", drop);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)drop, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");
      /* ---------------------------------------------------------------------har- */
      //free sql handles (stmt, dbc, env)
      FREE_SQLHANDLES

}
END_TEST(testSQLStatistics)
