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

#define TESTNAME "SQLGetTypeInfo"
#define TABLE ""

#define MAX_NAME_LEN 50

#define STR_LEN 128+1
#define REM_LEN 254+1

#define NULL_VALUE "<NULL>"

//*-------------------------------------------------------------------------

BEGIN_TEST(testSQLGetTypeInfo)
{
  /* ------------------------------------------------------------------------- */
      CHAR buffer[STR_LEN];

      /* Declare storage locations for result set data */

      CHAR szTypeName[STR_LEN], szLiteralPrefix[STR_LEN];
      CHAR szLiteralSuffix[STR_LEN], szCreateParams[STR_LEN];
      CHAR szLocalTypeName[STR_LEN];

      SQLINTEGER Precision;

      SQLSMALLINT DataType, Nullable, CaseSensitive, Searchable,
          UnsignedAttribute;
      SQLSMALLINT Money, AutoIncrement;
      SQLSMALLINT MinimumScale, MaximumScale;

      /* Declare storage locations for bytes available to return */

      SQLLEN cbTypeName, cbDataType, cbPrecision, cbLiteralPrefix;
      SQLLEN cbLiteralSuffix, cbCreateParams, cbNullable, cbCaseSensitive;
      SQLLEN cbSearchable, cbUnsignedAttribute, cbMoney, cbAutoIncrement;
      SQLLEN cbLocalTypeName, cbMinimumScale, cbMaximumScale;

      SQLSMALLINT count = 0;
      SQLSMALLINT cAbort = 0;
      /* ---------------------------------------------------------------------har- */
      //init sql handles (stmt, dbc, env)
      INIT_SQLHANDLES

      /* ------------------------------------------------------------------------- */
      /* All qualifiers, all owners, EMPLOYEE table, all columns */

      retcode = SQLGetTypeInfo(hstmt, SQL_ALL_TYPES);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLGetTypeInfo");

      if (retcode == SQL_SUCCESS) {
        /* Bind columns in result set to storage locations */

        SQLBindCol(hstmt, 1, SQL_C_CHAR, szTypeName, STR_LEN, &cbTypeName);
        SQLBindCol(hstmt, 2, SQL_C_SHORT, &DataType, STR_LEN, &cbDataType);
        SQLBindCol(hstmt, 3, SQL_C_LONG, &Precision, 0, &cbPrecision);
        SQLBindCol(hstmt, 4, SQL_C_CHAR, szLiteralPrefix, STR_LEN,
            &cbLiteralPrefix);
        SQLBindCol(hstmt, 5, SQL_C_CHAR, szLiteralSuffix, STR_LEN,
            &cbLiteralSuffix);
        SQLBindCol(hstmt, 6, SQL_C_CHAR, szCreateParams, STR_LEN,
            &cbCreateParams);
        SQLBindCol(hstmt, 7, SQL_C_SHORT, &Nullable, 0, &cbNullable);
        SQLBindCol(hstmt, 8, SQL_C_SHORT, &CaseSensitive, 0, &cbCaseSensitive);
        SQLBindCol(hstmt, 9, SQL_C_SHORT, &Searchable, 0, &cbSearchable);
        SQLBindCol(hstmt, 10, SQL_C_SHORT, &UnsignedAttribute, 0,
            &cbUnsignedAttribute);
        SQLBindCol(hstmt, 11, SQL_C_SHORT, &Money, 0, &cbMoney);
        SQLBindCol(hstmt, 12, SQL_C_SHORT, &AutoIncrement, 0,
            &cbAutoIncrement);
        SQLBindCol(hstmt, 13, SQL_C_CHAR, szLocalTypeName, STR_LEN,
            &cbLocalTypeName);
        SQLBindCol(hstmt, 14, SQL_C_SHORT, &MinimumScale, 0, &cbMinimumScale);
        SQLBindCol(hstmt, 15, SQL_C_SHORT, &MaximumScale, 0, &cbMaximumScale);

        printf(
            "\tGetTypeInfo:'TYPE_NAME','DATA_TYPE','PRECISION','LITERAL_PREFIX',\
       'LITERAL_SUFFIX','CREATE_PARAMS','NULLABLE','CASE_SENSITIVE',\
       'SEARCHABLE','UNSIGNED_ATTRIBUTE','MONEY','AUTO_INCREMENT',\
       'LOCAL_TYPE_NAME','MINIMUM_SCALE','MAXIMUM_SCALE'\r\n");

        while (TRUE) {
          count++;

          retcode = SQLFetch(hstmt);
          if(retcode != SQL_NO_DATA)
          DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
              "SQLFetch");

          if (retcode == SQL_ERROR || retcode == SQL_SUCCESS_WITH_INFO) {
            /* Show Error */
            DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
                "SQLError");
          }
          if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO) {
            /* Process fetched data */
            printf("\tColumn %d : ", count);

            /* *** TYPE_NAME ------------------------- *** */
            printf("'%s',", szTypeName);

            /* *** DATA_TYPE ------------------------- *** */
            printf("'%d',", DataType);

            /* *** PRECISION ------------------------- *** */
            if (cbPrecision == SQL_NULL_DATA)
              printf("'%s',", NULL_VALUE);
            else
              printf("'%ld',", Precision);

            /* *** LITERAL_PREFIX -------------------- *** */
            if (cbLiteralPrefix == SQL_NULL_DATA) strcpy(szLiteralPrefix,
                NULL_VALUE);
            printf("'%s',", szLiteralPrefix);

            /* *** LITERAL_SUFFIX -------------------- *** */
            if (cbLiteralSuffix == SQL_NULL_DATA) strcpy(szLiteralSuffix,
                NULL_VALUE);
            printf("'%s',", szLiteralSuffix);

            /* *** CREATE_PARAMS --------------------- *** */
            if (cbCreateParams == SQL_NULL_DATA) strcpy(szCreateParams,
                NULL_VALUE);
            printf("'%s',", szCreateParams);

            /* *** NULLABLE -------------------------- *** */
            Get_pfNullable(Nullable, buffer);
            printf("'%s',", buffer);

            /* *** CASE_SENSITIVE ------------------------- *** */
            Get_BoolValue(CaseSensitive, buffer);
            printf("'%s',", buffer);

            /* *** SEARCHABLE ------------------------ *** */
            Get_Searchable(Searchable, buffer);
            printf("'%s',", buffer);

            /* *** UNSIGNED_ATTRIBUTE ---------------- *** */
            if (cbUnsignedAttribute == SQL_NULL_DATA)
              printf("'%s',", NULL_VALUE);
            else {
              Get_BoolValue(UnsignedAttribute, buffer);
              printf("'%s',", buffer);
            }

            /* *** MONEY ----------------------------- *** */
            Get_BoolValue(Money, buffer);
            printf("'%s',", buffer);

            /* *** AUTO_INCREMENT -------------------- *** */
            if (cbAutoIncrement == SQL_NULL_DATA)
              printf("'%s',", NULL_VALUE);
            else {
              Get_BoolValue(AutoIncrement, buffer);
              printf("'%s',", buffer);
            }

            /* *** LOCAL_TYPE_NAME ------------------- *** */
            if (cbLocalTypeName == SQL_NULL_DATA) strcpy(szLocalTypeName,
                NULL_VALUE);
            printf("'%s',", szLocalTypeName);

            /* *** MINIMUM_SCALE --------------------- *** */
            if (cbMinimumScale == SQL_NULL_DATA)
              printf("'%s',", NULL_VALUE);
            else
              printf("'%d',", MinimumScale);

            /* *** MAXIMUM_SCALE --------------------- *** */
            if (cbMaximumScale == SQL_NULL_DATA)
              printf("'%s'\r\n", NULL_VALUE);
            else
              printf("'%d'\r\n", MaximumScale);
          }
          else {
            break;
          }
        }
      }
      // --- myodbc3: error crashes ---
      //retcode = SQLFreeStmt(hstmt, SQL_CLOSE);
     // DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
       //   "SQLFreeStmt");
      /* ---------------------------------------------------------------------har- */
      //free sql handles (stmt, dbc, env)
      FREE_SQLHANDLES
}
END_TEST(testSQLGetTypeInfo)
