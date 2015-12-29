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
#define TESTNAME "SQLGetStmtAttr"
#define TABLE ""

#define MAX_NAME_LEN 256
#define MAX_RGB_VALUE 256
#define ERROR_TEXT_LEN 511

#define STR_LEN 128+1
#define REM_LEN 254+1

#define PARAM_UNTOUCHED 999999
//*-------------------------------------------------------------------------

#define SQLSTMT1 "SHOW TABLES;"

/* ------------------------------------------------------------------------ */
/* SQLGetStmtAttr, SQLSetStmtAttr Parameters : */
/* ------------------------------------------------------------------------ */
/*
 1. SQL_ASYNC_ENABLE
 2. SQL_BIND_TYPE
 3. SQL_CONCURRENCY (ODBC 2.0)
 4. SQL_CURSOR_TYPE (ODBC 2.0)
 5. SQL_KEYSET_SIZE (ODBC 2.0)
 6. SQL_MAX_LENGTH
 7. SQL_MAX_ROWS
 8. SQL_NOSCAN
 9. SQL_QUERY_TIMEOUT
 10. SQL_RETRIEVE_DATA (ODBC 2.0)
 11. SQL_ROWSET_SIZE (ODBC 2.0)
 12. SQL_SIMULATE_CURSOR (ODBC 2.0)
 13. SQL_USE_BOOKMARKS (ODBC 2.0)

 Only SQLGetStmtOption:
 1. SQL_GET_BOOKMARK (ODBC 2.0)
 2. SQL_ROW_NUMBER (OBDC 2.0)
 */

BEGIN_TEST(testSQLGetStmtAttr)
{
  /* ------------------------------------------------------------------------- */
      CHAR buffer[MAX_NAME_LEN * 20];
      CHAR buf[MAX_NAME_LEN * 2];

      SQLINTEGER fOption;
      SQLPOINTER pvParam;
      SQLINTEGER pPar;
      /* SQLINTEGER                  vParam; */
      /* CHAR                   pvParamChar[MAX_RGB_VALUE];*/
      SQLHANDLE pvParamHandle;
      SQLPOINTER pvParamPtr;

      SQLINTEGER StrLengthPtr = 0;
      /* ---------------------------------------------------------------------har- */
      //init sql handles (stmt, dbc, env)
      INIT_SQLHANDLES
      /* ----------------------------------------------------------------- */

      /*LOGF(" ExecStatement : '%s' ", SQLSTMT1);

       retcode = SQLExecDirect(hstmt, (SQLCHAR*)SQLSTMT1, SQL_NTS);
       DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
       "SQLExecDirect");*/

      /* --- SQLGetStmtAttr ---------------------------------------------------- */

      /* *** 1. SQL_ATTR_APP_PARAM_DESC */
      fOption = SQL_ATTR_APP_PARAM_DESC;
      strcpy(buffer, "\0");
      pvParamHandle = (SQLPOINTER)PARAM_UNTOUCHED;
      retcode = SQLGetStmtAttr(hstmt, fOption, &pvParamHandle, SQL_IS_INTEGER,
          &StrLengthPtr);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLGetStmtAttr");
      sprintf(buffer, "%p", pvParamHandle);
      LOGF("SQLGetStmtAttr -> 1. SQL_ATTR_APP_PARAM_DESC :  Value = '%s'",
          buffer);

      /* *** 2. SQL_ATTR_APP_ROW_DESC */
      fOption = SQL_ATTR_APP_ROW_DESC;
      strcpy(buffer, "\0");
      pvParamHandle = (SQLPOINTER)PARAM_UNTOUCHED;
      retcode = SQLGetStmtAttr(hstmt, fOption, &pvParamHandle, SQL_IS_INTEGER,
          &StrLengthPtr);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLGetStmtAttr");
      sprintf(buffer, "%p", pvParamHandle);
      LOGF("SQLGetStmtAttr -> 2. SQL_ATTR_APP_ROW_DESC :  Value = '%s'", buffer);

      /* *** 3. SQL_ATTR_ASYNC_ENABLE */
      fOption = SQL_ATTR_ASYNC_ENABLE;
      strcpy(buffer, "\0");
      pvParam = (SQLPOINTER)PARAM_UNTOUCHED;
      retcode = SQLGetStmtAttr(hstmt, fOption, &pvParam, SQL_IS_INTEGER,
          &StrLengthPtr);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_ERROR, retcode,
          "SQLGetStmtAttr");
      //Not Supported attribute

      /*pPar = *((int*)pvParam);
       switch (pPar) {
       case SQL_ASYNC_ENABLE_OFF:
       strcpy(buf, "SQL_ASYNC_ENABLE_OFF");
       break;
       case SQL_ASYNC_ENABLE_ON:
       strcpy(buf, "SQL_ASYNC_ENABLE_ON");
       break;
       case PARAM_UNTOUCHED:
       strcpy(buf, "PARAM_UNTOUCHED");
       break;
       default:
       strcpy(buf, "???");
       }
       LOGF("Value = '%s'", buf);*/
      LOGF("SQLGetStmtAttr -> 3. SQL_ATTR_ASYNC_ENABLE :  Not Supported'");

      /* *** 4. SQL_ATTR_CONCURRENCY (ODBC 2.0) */
      fOption = SQL_ATTR_CONCURRENCY;
      strcpy(buffer, "\0");
      //pvParam = (SQLPOINTER)PARAM_UNTOUCHED;
      retcode = SQLGetStmtAttr(hstmt, fOption, &pPar, SQL_IS_INTEGER,
          &StrLengthPtr);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLGetStmtAttr");
      //pPar = ((SQLINTEGER)pvParam);
      switch (pPar) {
        case SQL_CONCUR_READ_ONLY:
          strcpy(buf, "SQL_CONCUR_READ_ONLY");
          break;
        case SQL_CONCUR_LOCK:
          strcpy(buf, "SQL_CONSUR_LOCK");
          break;
        case SQL_CONCUR_ROWVER:
          strcpy(buf, "SQL_CONSUR_ROWCUR");
          break;
        case SQL_CONCUR_VALUES:
          strcpy(buf, "SQL_CONSUR_VALUES");
          break;
        case PARAM_UNTOUCHED:
          strcpy(buf, "PARAM_UNTOUCHED");
          break;
        default:
          strcpy(buf, "???");
      }
      LOGF("SQLGetStmtAttr -> 4. SQL_ATTR_CONCURRENCY :  Value = '%s'", buf);

      /* *** 5. SQL_ATTR_CURSOR_SCROLLABLE (ODBC 3.0) */
      fOption = SQL_ATTR_CURSOR_SCROLLABLE;
      strcpy(buffer, "\0");
      //pvParam = (SQLPOINTER)PARAM_UNTOUCHED;
      retcode = SQLGetStmtAttr(hstmt, fOption, &pPar, SQL_IS_INTEGER,
          &StrLengthPtr);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLGetStmtAttr");
      //pPar = *((int*)pvParam);
      switch (pPar) {
        case SQL_NONSCROLLABLE:
          strcpy(buf, "SQL_NONSCROLLABLE");
          break;
        case SQL_SCROLLABLE:
          strcpy(buf, "SQL_SCROLLABLE");
          break;
        default:
          strcpy(buf, "???");
      }
      LOGF("SQLGetStmtAttr -> 5. SQL_ATTR_CURSOR_SCROLLABLE :  Value = '%s'",
          buf);

      /* *** 6. SQL_ATTR_CURSOR_SENSITIVITY (ODBC 3.0) */
      fOption = SQL_ATTR_CURSOR_SENSITIVITY;
      strcpy(buffer, "\0");
      //pvParam = (SQLPOINTER)PARAM_UNTOUCHED;
      retcode = SQLGetStmtAttr(hstmt, fOption, &pPar, SQL_IS_INTEGER,
          &StrLengthPtr);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLGetStmtAttr");
      //pPar = *((int*)pvParam);
      switch (pPar) {
        case SQL_UNSPECIFIED:
          strcpy(buf, "SQL_UNSPECIFIED");
          break;
        case SQL_INSENSITIVE:
          strcpy(buf, "SQL_INSENSITIVE");
          break;
        case SQL_SENSITIVE:
          strcpy(buf, "SQL_SENSITIVE");
          break;
        default:
          strcpy(buf, "???");
      }
      LOGF("SQLGetStmtAttr -> 6. SQL_ATTR_CURSOR_SENSITIVITY :  Value = '%s'",
          buf);

      /* *** 7. SQL_ATTR_CURSOR_TYPE (ODBC 2.0) */
      fOption = SQL_ATTR_CURSOR_TYPE;
      strcpy(buffer, "\0");
      //pvParam = (SQLPOINTER)PARAM_UNTOUCHED;
      retcode = SQLGetStmtAttr(hstmt, fOption, &pPar, SQL_IS_INTEGER,
          &StrLengthPtr);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLGetStmtAttr");
      //pPar = *((int*)pvParam);
      switch (pPar) {
        case SQL_CURSOR_FORWARD_ONLY:
          strcpy(buf, "SQL_CURSOR_FORWARD_ONLY");
          break;
        case SQL_CURSOR_STATIC:
          strcpy(buf, "SQL_CURSOR_STATIC");
          break;
        case SQL_CURSOR_KEYSET_DRIVEN:
          strcpy(buf, "SQL_CURSOR_KEYSET_DRIVEN");
          break;
        case SQL_CURSOR_DYNAMIC:
          strcpy(buf, "SQL_CURSOR_DYNAMIC");
          break;
        case PARAM_UNTOUCHED:
          strcpy(buf, "PARAM_UNTOUCHED");
          break;
        default:
          strcpy(buf, "???");
      }
      LOGF("SQLGetStmtAttr -> 7. SQL_ATTR_CURSOR_TYPE :  Value = '%s'",
          buf);


      /* *** 8. SQL_ATTR_ENABLE_AUTO_IPD (ODBC 3.0) */
      fOption = SQL_ATTR_ENABLE_AUTO_IPD;
      strcpy(buffer, "\0");
      //pvParam = (SQLPOINTER)PARAM_UNTOUCHED;
      retcode = SQLGetStmtAttr(hstmt, fOption, &pPar, SQL_IS_INTEGER,
          &StrLengthPtr);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLGetStmtAttr");
      //pPar = *((int*)pvParam);
      switch (pPar) {
        case SQL_TRUE:
          strcpy(buf, "SQL_TRUE");
          break;
        case SQL_FALSE:
          strcpy(buf, "SQL_FALSE");
          break;
        default:
          strcpy(buf, "???");
      }
      LOGF("SQLGetStmtAttr -> 8. SQL_ATTR_ENABLE_AUTO_IPD :  Value = '%s'",
          buf);


      /* *** 9. SQL_ATTR_FETCH_BOOKMARK_PTR (ODBC 3.0) */
      fOption = SQL_ATTR_FETCH_BOOKMARK_PTR;
      strcpy(buffer, "\0");
      retcode = SQLGetStmtAttr(hstmt, fOption, &pvParamPtr, SQL_IS_POINTER,
          &StrLengthPtr);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLGetStmtAttr");
      sprintf(buffer, "%p", pvParamPtr);
      LOGF("SQLGetStmtAttr -> 9. SQL_ATTR_FETCH_BOOKMARK_PTR :  Value = '%s'",
          buffer);

      /* *** 10. SQL_ATTR_IMP_PARAM_DESC (ODBC 3.0) */
      fOption = SQL_ATTR_IMP_PARAM_DESC;
      strcpy(buffer, "\0");
      pvParamHandle = (SQLPOINTER)PARAM_UNTOUCHED;
      retcode = SQLGetStmtAttr(hstmt, fOption, &pvParamHandle, SQL_IS_INTEGER,
          &StrLengthPtr);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLGetStmtAttr");
      sprintf(buffer, "%p", pvParamHandle);
      LOGF("SQLGetStmtAttr -> 10. SQL_ATTR_IMP_PARAM_DESC :  Value = '%s'",
          buffer);

      /* *** 11. SQL_ATTR_IMP_ROW_DESC (ODBC 3.0) */
      fOption = SQL_ATTR_IMP_ROW_DESC;
      strcpy(buffer, "\0");
      pvParamHandle = (SQLPOINTER)PARAM_UNTOUCHED;
      retcode = SQLGetStmtAttr(hstmt, fOption, &pvParamHandle, SQL_IS_INTEGER,
          &StrLengthPtr);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLGetStmtAttr");
      sprintf(buffer, "%p", pvParamHandle);
      LOGF("SQLGetStmtAttr -> 11. SQL_ATTR_IMP_ROW_DESC :  Value = '%s'",
          buffer);

      /* *** 12. SQL_ATTR_KEYSET_SIZE (ODBC 2.0) */
      fOption = SQL_ATTR_KEYSET_SIZE;
      retcode = SQLGetStmtAttr(hstmt, fOption, &pPar, SQL_IS_INTEGER,
          &StrLengthPtr);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLGetStmtAttr");
      LOGF("SQLGetStmtAttr -> 12. SQL_ATTR_KEYSET_SIZE :  Value = &d",pPar);

      /* *** 13. SQL_ATTR_MAX_LENGTH */
      fOption = SQL_ATTR_MAX_LENGTH;

      retcode = SQLGetStmtAttr(hstmt, fOption, &pPar, SQL_IS_INTEGER,
          &StrLengthPtr);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLGetStmtAttr");
      LOGF("SQLGetStmtAttr -> 13. SQL_ATTR_MAX_LENGTH : Value = '%d'", pPar);

      /* *** 14. SQL_ATTR_MAX_ROWS */
      fOption = SQL_ATTR_MAX_ROWS;

      retcode = SQLGetStmtAttr(hstmt, fOption, &pPar, SQL_IS_INTEGER,
          &StrLengthPtr);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLGetStmtAttr");
      LOGF("SQLGetStmtAttr -> 14. SQL_ATTR_MAX_ROWS :  Value = '%d'",
          pPar);

      /* *** 15. SQL_ATTR_METADATA_ID (ODBC 3.0) */
      fOption = SQL_ATTR_METADATA_ID;
      retcode = SQLGetStmtAttr(hstmt, fOption, &pPar, SQL_IS_INTEGER,
          &StrLengthPtr);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLGetStmtAttr");
      switch (pPar) {
        case SQL_TRUE:
          strcpy(buf, "SQL_TRUE");
          break;
        case SQL_FALSE:
          strcpy(buf, "SQL_FALSE");
          break;
        default:
          strcpy(buf, "???");
      }
      LOGF("SQLGetStmtAttr -> 15. SQL_ATTR_METADATA_ID :  Value = '%s'", buf)

      /* *** 16. SQL_ATTR_NOSCAN */
      fOption = SQL_ATTR_NOSCAN;
      strcpy(buffer, "\0");
      pvParam = (SQLPOINTER)PARAM_UNTOUCHED;
      retcode = SQLGetStmtAttr(hstmt, fOption, &pvParam, SQL_IS_INTEGER,
          &StrLengthPtr);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLGetStmtAttr");
       switch (pPar) {
       case SQL_NOSCAN_OFF:
       strcpy(buf, "SQL_NOSCAN_OFF");
       break;
       case SQL_NOSCAN_ON:
       strcpy(buf, "SQL_NOSCAN_ON");
       break;
       case PARAM_UNTOUCHED:
       strcpy(buf, "PARAM_UNTOUCHED");
       break;
       default:
       strcpy(buf, "???");
       }
      LOGF("SQLGetStmtAttr -> 16. SQL_ATTR_NOSCAN :  Value = '%s'", buf);

      /* *** 17. SQL_ATTR_PARAM_BIND_OFFSET_PTR (ODBC 3.0) */
      fOption = SQL_ATTR_PARAM_BIND_OFFSET_PTR;
      strcpy(buffer, "\0");
      retcode = SQLGetStmtAttr(hstmt, fOption, &pvParamPtr, SQL_IS_POINTER,
          &StrLengthPtr);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLGetStmtAttr");
      //sprintf(buffer, "%p", pvParamPtr);
      LOGF("SQLGetStmtAttr -> 17. SQL_ATTR_PARAM_BIND_OFFSET_PTR : %d",pvParamPtr);

      /* *** 18. SQL_ATTR_PARAM_BIND_TYPE (ODBC 3.0) */
      fOption = SQL_ATTR_PARAM_BIND_TYPE;
      strcpy(buffer, "\0");
      LOGF("18. SQL_ATTR_PARAM_BIND_TYPE : ");
      //pvParam = (SQLPOINTER)PARAM_UNTOUCHED;
      retcode = SQLGetStmtAttr(hstmt, fOption, &pvParam, SQL_IS_INTEGER,
          &StrLengthPtr);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLGetStmtAttr");
     /* pPar = *((int*)pvParam);
      switch (pPar) {
        case SQL_PARAM_BIND_BY_COLUMN:
          strcpy(buf, "SQL_PARAM_BIND_BY_COLUMN");
          break;
        default:
          strcpy(buf, "???");
      }*/
      LOGF("SQLGetStmtAttr -> 18. SQL_PARAM_BIND_BY_COLUMN : %d",pvParam);

      /* *** 19. SQL_ATTR_PARAM_OPERATION_PTR (ODBC 3.0) */
      fOption = SQL_ATTR_PARAM_OPERATION_PTR;
      strcpy(buffer, "\0");
      retcode = SQLGetStmtAttr(hstmt, fOption, &pvParamPtr, SQL_IS_POINTER,
          &StrLengthPtr);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLGetStmtAttr");
      //sprintf(buffer, "%p", pvParamPtr);
      LOGF("SQLGetStmtAttr -> 19. SQL_ATTR_PARAM_OPERATION_PTR : %d",pvParamPtr);

      /* *** 20. SQL_ATTR_PARAM_STATUS_PTR (ODBC 3.0) */
      fOption = SQL_ATTR_PARAM_STATUS_PTR;
      strcpy(buffer, "\0");
      retcode = SQLGetStmtAttr(hstmt, fOption, &pvParamPtr, SQL_IS_POINTER,
          &StrLengthPtr);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLGetStmtAttr");
      //sprintf(buffer, "%p", pvParamPtr);
      LOGF("SQLGetStmtAttr -> 20. SQL_ATTR_PARAM_STATUS_PTR : %d",pvParamPtr);

      /* *** 21. SQL_ATTR_PARAMS_PROCESSED_PTR (ODBC 3.0) */
      fOption = SQL_ATTR_PARAMS_PROCESSED_PTR;
      strcpy(buffer, "\0");
      retcode = SQLGetStmtAttr(hstmt, fOption,&pvParamPtr, SQL_IS_POINTER,
          &StrLengthPtr);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLGetStmtAttr");
      //sprintf(buffer, "%p", pvParamPtr);
      LOGF("SQLGetStmtAttr -> 17. SQL_ATTR_PARAM_BIND_OFFSET_PTR : %d",pvParamPtr);

      /* *** 22. SQL_ATTR_PARAMSET_SIZE (ODBC 3.0) */
      fOption = SQL_ATTR_PARAMSET_SIZE;
      strcpy(buffer, "\0");
      //pvParam = (SQLPOINTER)PARAM_UNTOUCHED;
      retcode = SQLGetStmtAttr(hstmt, fOption, &pvParam, SQL_IS_INTEGER,
          &StrLengthPtr);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLGetStmtAttr");
      LOGF("SQLGetStmtAttr -> 21. SQL_ATTR_PARAMS_PROCESSED_PTR : %d",pvParamPtr);


      /* *** 23. SQL_ATTR_QUERY_TIMEOUT */
      fOption = SQL_ATTR_QUERY_TIMEOUT;
      //pvParam = (SQLPOINTER)PARAM_UNTOUCHED;
      retcode = SQLGetStmtAttr(hstmt, fOption, &pvParam, SQL_IS_INTEGER,
          &StrLengthPtr);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLGetStmtAttr");
      LOGF("SQLGetStmtAttr -> 23. SQL_ATTR_QUERY_TIMEOUT : %d",pvParamPtr);


      /* *** 24. SQL_ATTR_RETRIEVE_DATA (ODBC 2.0) */
      fOption = SQL_ATTR_RETRIEVE_DATA;
      strcpy(buffer, "\0");
      //pvParam = (SQLPOINTER)PARAM_UNTOUCHED;
      retcode = SQLGetStmtAttr(hstmt, fOption, &pvParam, SQL_IS_INTEGER,
          &StrLengthPtr);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_ERROR, retcode,
          "SQLGetStmtAttr");
     /* pPar = *((int*)pvParam);
      switch (pPar) {
        case SQL_RD_OFF:
          strcpy(buf, "SQL_RD_OFF");
          break;
        case SQL_RD_ON:
          strcpy(buf, "SQL_RD_ON");
          break;
        case PARAM_UNTOUCHED:
          strcpy(buf, "PARAM_UNTOUCHED");
          break;
        default:
          strcpy(buf, "???");
      }*/
      LOGF("SQLGetStmtAttr -> 24. SQL_ATTR_RETRIEVE_DATA : Attribute Not Supported");


      /* *** 25. SQL_ATTR_ROW_ARRAY_SIZE (ODBC 3.0) */
      fOption = SQL_ATTR_ROW_ARRAY_SIZE;
      strcpy(buffer, "\0");
      pvParam = (SQLPOINTER)PARAM_UNTOUCHED;
      retcode = SQLGetStmtAttr(hstmt, fOption, &pvParam, SQL_IS_INTEGER,
          &StrLengthPtr);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLGetStmtAttr");
      LOGF("SQLGetStmtAttr -> 25. SQL_ATTR_ROW_ARRAY_SIZE : Value = '%d'",pvParam);


      /* *** 26. SQL_ATTR_ROW_BIND_OFFSET_PTR (ODBC 3.0) */
      fOption = SQL_ATTR_ROW_BIND_OFFSET_PTR;
      strcpy(buffer, "\0");
      retcode = SQLGetStmtAttr(hstmt, fOption, &pvParamPtr, SQL_IS_POINTER,
          &StrLengthPtr);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLGetStmtAttr");
      sprintf(buffer, "%p", pvParamPtr);
      LOGF("SQLGetStmtAttr -> 26. SQL_ATTR_ROW_BIND_OFFSET_PTR : Value = '%s'", buffer);

      /* *** 27. SQL_ATTR_ROW_BIND_TYPE */
      fOption = SQL_ATTR_ROW_BIND_TYPE;
      retcode = SQLGetStmtAttr(hstmt, fOption, &pPar, SQL_IS_INTEGER,
          &StrLengthPtr);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLGetStmtAttr");
      LOGF("SQLGetStmtAttr param type %d",pPar);

      switch (pPar) {
        case SQL_BIND_BY_COLUMN:
          strcpy(buf, "SQL_BIND_BY_COLUMN");
          break;
        default:
          strcpy(buf, "???");
      }
      LOGF("SQLGetStmtAttr -> 27. SQL_ATTR_ROW_BIND_TYPE : Value = '%s'", buf);

      /* *** 28. SQL_ATTR_ROW_NUMBER (OBDC 2.0) */
      fOption = SQL_ATTR_ROW_NUMBER;
      pvParam = (SQLPOINTER)PARAM_UNTOUCHED;
      retcode = SQLGetStmtAttr(hstmt, fOption, &pvParam, SQL_IS_INTEGER,
          &StrLengthPtr);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_ERROR, retcode,
          "SQLGetStmtAttr");
      LOGF("SQLGetStmtAttr -> 28. SQL_ATTR_ROW_NUMBER : Attribute Not Supported");


      /* *** 29. SQL_ATTR_ROW_OPERATION_PTR (ODBC 3.0) */
      fOption = SQL_ATTR_ROW_OPERATION_PTR;
      strcpy(buffer, "\0");
      retcode = SQLGetStmtAttr(hstmt, fOption, &pvParamPtr, SQL_IS_POINTER,
          &StrLengthPtr);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLGetStmtAttr");
      sprintf(buffer, "%p", pvParamPtr);
      LOGF("SQLGetStmtAttr -> 29. SQL_ATTR_ROW_OPERATION_PTR : %d",pvParamPtr);

      /* *** 30. SQL_ATTR_ROW_STATUS_PTR (ODBC 3.0) */
      fOption = SQL_ATTR_ROW_STATUS_PTR;
      strcpy(buffer, "\0");
      retcode = SQLGetStmtAttr(hstmt, fOption, &pvParamPtr, SQL_IS_POINTER,
          &StrLengthPtr);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLGetStmtAttr");
      sprintf(buffer, "%p", pvParamPtr);
      LOGF("SQLGetStmtAttr ->30. SQL_ATTR_ROW_STATUS_PTR : Value = '%s'", buffer);

      /* *** 31. SQL_ATTR_ROWS_FETCHED_PTR (ODBC 2.0) */
      fOption = SQL_ATTR_ROWS_FETCHED_PTR;
      strcpy(buffer, "\0");
      retcode = SQLGetStmtAttr(hstmt, fOption, &pvParamPtr, SQL_IS_POINTER,
          &StrLengthPtr);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLGetStmtAttr");
      sprintf(buffer, "%p", pvParamPtr);
      LOGF("SQLGetStmtAttr -> 31. SQL_ATTR_ROWS_FETCHED_PTR : Value = '%s'", buffer);

      /* *** 32. SQL_ATTR_SIMULATE_CURSOR (ODBC 2.0) */
      fOption = SQL_ATTR_SIMULATE_CURSOR;
      strcpy(buffer, "\0");
      LOGF("32.  : ");
      pvParam = (SQLPOINTER)PARAM_UNTOUCHED;
      retcode = SQLGetStmtAttr(hstmt, fOption, &pvParam, SQL_IS_INTEGER,
          &StrLengthPtr);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_ERROR, retcode,
          "SQLGetStmtAttr");
      /*pPar = *((int*)pvParam);
      switch (pPar) {
        case SQL_SC_NON_UNIQUE:
          strcpy(buf, "SQL_SC_NON_UNIQUE");
          break;
        case SQL_SC_TRY_UNIQUE:
          strcpy(buf, "SQL_SC_TRY_UNIQUE");
          break;
        case SQL_SC_UNIQUE:
          strcpy(buf, "SQL_SC_UNIQUE");
          break;
        case PARAM_UNTOUCHED:
          strcpy(buf, "PARAM_UNTOUCHED");
          break;
        default:
          strcpy(buf, "???");
      }*/
      LOGF("SQLGetStmtAttr -> 32. SQL_ATTR_SIMULATE_CURSOR : Attribute Not Supported");

      /* *** 33. SQL_ATTR_USE_BOOKMARKS (ODBC 2.0) */
      fOption = SQL_ATTR_USE_BOOKMARKS;
      strcpy(buffer, "\0");
      pvParam = (SQLPOINTER)PARAM_UNTOUCHED;
      retcode = SQLGetStmtAttr(hstmt, fOption, &pvParam, SQL_IS_INTEGER,
          &StrLengthPtr);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_ERROR, retcode,
          "SQLGetStmtAttr");
      /*pPar = *((int*)pvParam);
      switch (pPar) {
        case SQL_UB_OFF:
          strcpy(buf, "SQL_UB_OFF");
          break;
        case SQL_UB_ON:
          strcpy(buf, "SQL_UB_ON");
          break;
        case PARAM_UNTOUCHED:
          strcpy(buf, "PARAM_UNTOUCHED");
          break;
        default:
          strcpy(buf, "???");
      }*/
      LOGF("SQLGetStmtAttr -> 33. SQL_ATTR_USE_BOOKMARKS : Attribute Not Supported");


      /* ***  34. SQL_GET_BOOKMARK (ODBC 2.0) */
      fOption = SQL_GET_BOOKMARK;
      pvParam = (SQLPOINTER)PARAM_UNTOUCHED;
      retcode = SQLGetStmtAttr(hstmt, fOption, &pvParam, SQL_IS_INTEGER,
          &StrLengthPtr);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_ERROR, retcode,
          "SQLGetStmtAttr");
      LOGF("SQLGetStmtAttr -> 34. SQL_GET_BOOKMARK : Attribute Not Supported");


      /* *** 35. SQL_ROWSET_SIZE (ODBC 2.0) */
      fOption = SQL_ROWSET_SIZE;
      pvParam = (SQLPOINTER)PARAM_UNTOUCHED;
      retcode = SQLGetStmtAttr(hstmt, fOption, &pvParam, SQL_IS_INTEGER,
          &StrLengthPtr);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_ERROR, retcode,
          "SQLGetStmtAttr");
      LOGF("SQLGetStmtAttr -> 35. SQL_ROWSET_SIZE : Attribute Not Supported");


      /* --- Disconnect -------------------------------------------------- */
      //free sql handles (stmt, dbc, env)
      FREE_SQLHANDLES

    }
    END_TEST(testSQLGetStmtAttr)
