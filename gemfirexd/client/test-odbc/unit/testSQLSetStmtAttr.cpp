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
#define TESTNAME "SQLSetStmtAttr"
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
/* SQLSetStmtAttr, SQLSetStmtAttr Parameters : */
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

BEGIN_TEST(testSQLSetStmtAttr) {
	/* ------------------------------------------------------------------------- */
			CHAR buffer[MAX_NAME_LEN * 20];
			CHAR buf[MAX_NAME_LEN * 20];

			SQLINTEGER fOption;
			PTR pvParam;
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

			/*printf("\t ExecStatement : '%s' \r\n", SQLSTMT1);

			 retcode = SQLExecDirect(hstmt, (SQLCHAR*)SQLSTMT1, SQL_NTS);
			 DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,"SQLExecDirect");*/

			/* --- SQLGetStmtOption ---------------------------------------------------- */

			/* *** 1. SQL_ATTR_APP_PARAM_DESC */
			LOGF("SQLSetStmtAttr ->1. SQL_ATTR_APP_PARAM_DESC : Value = 'NULL'");
			retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_APP_PARAM_DESC,
					(SQLPOINTER) NULL, StrLengthPtr);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_ERROR, retcode,
					"SQLSetStmtAttr");

			/* *** 2. SQL_ATTR_APP_ROW_DESC */
			LOGF(
					"SQLSetStmtAttr ->2. SQL_ATTR_APP_ROW_DESC : Value = 'SQL_NULL_HANDLE'");
			retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_APP_ROW_DESC,
					(SQLPOINTER) SQL_NULL_HANDLE, StrLengthPtr);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_ERROR, retcode,
					"SQLSetStmtAttr");

			/* *** 3. SQL_ATTR_ASYNC_ENABLE */
			LOGF(
					"SQLSetStmtAttr ->3. SQL_ATTR_ASYNC_ENABLE : Value = 'SQL_ASYNC_ENABLE_ON'");
			retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_ASYNC_ENABLE,
					(SQLPOINTER) SQL_ASYNC_ENABLE_ON, StrLengthPtr);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_ERROR, retcode,
					"SQLSetStmtAttr");

			/* *** 4. SQL_ATTR_CONCURRENCY (ODBC 2.0) */
			LOGF(
					"SQLSetStmtAttr ->4. SQL_ATTR_CONCURRENCY : Value = 'SQL_CONCUR_READ_ONLY'");
			retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_CONCURRENCY,
					(SQLPOINTER) SQL_CONCUR_READ_ONLY, StrLengthPtr);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
					"SQLSetStmtAttr");

			LOGF(
					"SQLSetStmtAttr ->4. SQL_ATTR_CONCURRENCY : Value = 'SQL_CONCUR_LOCK'");
			retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_CONCURRENCY,
					(SQLPOINTER) SQL_CONCUR_LOCK, StrLengthPtr);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
					"SQLSetStmtAttr");

			LOGF(
					"SQLSetStmtAttr ->4. SQL_ATTR_CONCURRENCY : Value = 'SQL_CONCUR_ROWVER'");
			retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_CONCURRENCY,
					(SQLPOINTER) SQL_CONCUR_ROWVER, StrLengthPtr);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
					"SQLSetStmtAttr");

			LOGF(
					"SQLSetStmtAttr ->4. SQL_ATTR_CONCURRENCY : Value = 'SQL_CONCUR_VALUES'");
			retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_CONCURRENCY,
					(SQLPOINTER) SQL_CONCUR_VALUES, StrLengthPtr);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
					"SQLSetStmtAttr");

			LOGF(
					"SQLSetStmtAttr ->4. SQL_ATTR_CONCURRENCY : Value = 'SQL_CONCUR_READ_ONLY'");
			retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_CONCURRENCY,
					(SQLPOINTER) SQL_CONCUR_READ_ONLY, StrLengthPtr);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
					"SQLSetStmtAttr");

			/* *** 5. SQL_ATTR_CURSOR_SCROLLABLE (ODBC 3.0) */
			LOGF(
					"SQLSetStmtAttr ->5. SQL_ATTR_CURSOR_SCROLLABLE : Value = 'SQL_NONSCROLLABLE'");
			retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_CURSOR_SCROLLABLE,
					(SQLPOINTER) SQL_NONSCROLLABLE, StrLengthPtr);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
					"SQLSetStmtAttr");

			LOGF(
					"SQLSetStmtAttr ->5. SQL_ATTR_CURSOR_SCROLLABLE : Value = 'SQL_SCROLLABLE'");
			retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_CURSOR_SCROLLABLE,
					(SQLPOINTER) SQL_SCROLLABLE, StrLengthPtr);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
					"SQLSetStmtAttr");

			/* *** 6. SQL_ATTR_CURSOR_SENSITIVITY (ODBC 3.0) */
			LOGF(
					"SQLSetStmtAttr ->6. SQL_ATTR_CURSOR_SENSITIVITY : Value = 'SQL_INSENSITIVE'");
			retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_CURSOR_SENSITIVITY,
					(SQLPOINTER) SQL_INSENSITIVE, StrLengthPtr);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
					"SQLSetStmtAttr");

			LOGF(
					"SQLSetStmtAttr ->6. SQL_ATTR_CURSOR_SENSITIVITY : Value = 'SQL_SENSITIVE'");
			retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_CURSOR_SENSITIVITY,
					(SQLPOINTER) SQL_SENSITIVE, StrLengthPtr);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
					"SQLSetStmtAttr");

			/* *** 7. SQL_ATTR_CURSOR_TYPE (ODBC 2.0) */
			LOGF(
					"SQLSetStmtAttr ->7. SQL_ATTR_CURSOR_TYPE : Value = 'SQL_CURSOR_FORWARD_ONLY'");
			retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_CURSOR_TYPE,
					(SQLPOINTER) SQL_CURSOR_FORWARD_ONLY, StrLengthPtr);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
					"SQLSetStmtAttr");
			LOGF(
					"SQLSetStmtAttr ->7. SQL_ATTR_CURSOR_TYPE : Value = 'SQL_CURSOR_STATIC'");
			retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_CURSOR_TYPE,
					(SQLPOINTER) SQL_CURSOR_STATIC, StrLengthPtr);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
					"SQLSetStmtAttr");
			LOGF(
					"SQLSetStmtAttr ->7. SQL_ATTR_CURSOR_TYPE : Value = 'SQL_CURSOR_KEYSET_DRIVEN'");
			retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_CURSOR_TYPE,
					(SQLPOINTER) SQL_CURSOR_KEYSET_DRIVEN, StrLengthPtr);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
					"SQLSetStmtAttr");
			LOGF(
					"SQLSetStmtAttr ->7. SQL_ATTR_CURSOR_TYPE : Value = 'SQL_CURSOR_DYNAMIC'");
			retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_CURSOR_TYPE,
					(SQLPOINTER) SQL_CURSOR_DYNAMIC, StrLengthPtr);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
					"SQLSetStmtAttr");

			/* *** 8. SQL_ATTR_ENABLE_AUTO_IPD (ODBC 3.0) */
			LOGF(
					"SQLSetStmtAttr ->8. SQL_ATTR_ENABLE_AUTO_IPD : Value = 'SQL_TRUE'");
			retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_ENABLE_AUTO_IPD,
					(SQLPOINTER) SQL_TRUE, StrLengthPtr);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
					"SQLSetStmtAttr");

			LOGF(
					"SQLSetStmtAttr ->8. SQL_ATTR_ENABLE_AUTO_IPD : Value = 'SQL_FALSE'");
			retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_ENABLE_AUTO_IPD,
					(SQLPOINTER) SQL_FALSE, StrLengthPtr);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
					"SQLSetStmtAttr");

			/* *** 9. SQL_ATTR_FETCH_BOOKMARK_PTR (ODBC 3.0) */
			LOGF(
					"SQLSetStmtAttr ->9. SQL_ATTR_FETCH_BOOKMARK_PTR : Value = 'NULL'",
					buffer);
			retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_FETCH_BOOKMARK_PTR,
					(SQLPOINTER) NULL, StrLengthPtr);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
					"SQLSetStmtAttr");
			sprintf(buffer, "%p", pvParamPtr);

			/* *** 10. SQL_ATTR_IMP_PARAM_DESC (ODBC 3.0) */
			LOGF(
					"SQLSetStmtAttr ->10. SQL_ATTR_IMP_PARAM_DESC : Value = 'NULL'");
			retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_IMP_PARAM_DESC,
					(SQLPOINTER) NULL, StrLengthPtr);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_ERROR, retcode,
					"SQLSetStmtAttr");

			/* *** 11. SQL_ATTR_IMP_ROW_DESC (ODBC 3.0) */
			LOGF("SQLSetStmtAttr ->11. SQL_ATTR_IMP_ROW_DESC : Value = 'NULL'");
			pvParamHandle = (SQLPOINTER) PARAM_UNTOUCHED;
			retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_IMP_ROW_DESC,
					(SQLPOINTER) NULL, StrLengthPtr);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_ERROR, retcode,
					"SQLSetStmtAttr");

			/* *** 12. SQL_ATTR_KEYSET_SIZE (ODBC 2.0) */
			LOGF("SQLSetStmtAttr ->12. SQL_ATTR_KEYSET_SIZE : Value = '%d'",
					100);
			retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_KEYSET_SIZE,
					(SQLPOINTER) 100, StrLengthPtr);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
					"SQLSetStmtAttr");

			/* *** 13. SQL_ATTR_MAX_LENGTH */
			LOGF("SQLSetStmtAttr ->13. SQL_ATTR_MAX_LENGTH : Value = '%d'",
					1024);
			retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_MAX_LENGTH,
					(SQLPOINTER) 1024, StrLengthPtr);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
					"SQLSetStmtAttr");

			/* *** 14. SQL_ATTR_MAX_ROWS */
			LOGF("SQLSetStmtAttr ->14. SQL_ATTR_MAX_ROWS : Value = '%d'", 1000);
			retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_MAX_ROWS,
					(SQLPOINTER) 1000, StrLengthPtr);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
					"SQLSetStmtAttr");

			/* *** 15. SQL_ATTR_METADATA_ID (ODBC 3.0) */
			LOGF(
					"SQLSetStmtAttr ->15. SQL_ATTR_METADATA_ID : Value = 'SQL_TRUE'");
			retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_METADATA_ID,
					(SQLPOINTER) SQL_TRUE, StrLengthPtr);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
					"SQLSetStmtAttr");

			LOGF(
					"SQLSetStmtAttr ->15. SQL_ATTR_METADATA_ID : Value = 'SQL_FALSE'");
			retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_METADATA_ID,
					(SQLPOINTER) SQL_FALSE, StrLengthPtr);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
					"SQLSetStmtAttr");

			/* *** 16. SQL_ATTR_NOSCAN */
			LOGF(
					"SQLSetStmtAttr ->16. SQL_ATTR_NOSCAN : Value = SQL_NOSCAN_OFF");
			retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_NOSCAN,
					(SQLPOINTER) SQL_NOSCAN_OFF, StrLengthPtr);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
					"SQLSetStmtAttr");

			LOGF("SQLSetStmtAttr ->16. SQL_ATTR_NOSCAN : Value = SQL_NOSCAN_ON");
			retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_NOSCAN,
					(SQLPOINTER) SQL_NOSCAN_ON, StrLengthPtr);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
					"SQLSetStmtAttr");

			/* *** 17. SQL_ATTR_PARAM_BIND_OFFSET_PTR (ODBC 3.0) */
			LOGF(
					"SQLSetStmtAttr ->17. SQL_ATTR_PARAM_BIND_OFFSET_PTR : Value = 'NULL'");
			retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_PARAM_BIND_OFFSET_PTR,
					(SQLPOINTER) NULL, StrLengthPtr);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
					"SQLSetStmtAttr");

			/* *** 18. SQL_ATTR_PARAM_BIND_TYPE (ODBC 3.0) */
			LOGF(
					"SQLSetStmtAttr ->18. SQL_ATTR_PARAM_BIND_TYPE : Value = 'SQL_PARAM_BIND_BY_COLUMN'");
			retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_PARAM_BIND_TYPE,
					(SQLPOINTER) SQL_PARAM_BIND_BY_COLUMN, StrLengthPtr);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
					"SQLSetStmtAttr");

			/* *** 19. SQL_ATTR_PARAM_OPERATION_PTR (ODBC 3.0) */
			LOGF(
					"SQLSetStmtAttr ->19. SQL_ATTR_PARAM_OPERATION_PTR : Value = 'NULL'");
			retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_PARAM_OPERATION_PTR,
					(SQLPOINTER) NULL, StrLengthPtr);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
					"SQLSetStmtAttr");

			/* *** 20. SQL_ATTR_PARAM_STATUS_PTR (ODBC 3.0) */
			LOGF(
					"SQLSetStmtAttr ->20. SQL_ATTR_PARAM_STATUS_PTR : Value = 'NULL'",
					buffer);
			retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_PARAM_STATUS_PTR,
					(SQLPOINTER) NULL, StrLengthPtr);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
					"SQLSetStmtAttr");

			/* *** 21. SQL_ATTR_PARAMS_PROCESSED_PTR (ODBC 3.0) */
			LOGF(
					"SQLSetStmtAttr ->21. SQL_ATTR_PARAMS_PROCESSED_PTR : Value = 'NULL'",
					buffer);
			retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_PARAMS_PROCESSED_PTR,
					(SQLPOINTER) NULL, StrLengthPtr);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
					"SQLSetStmtAttr");

			/* *** 22. SQL_ATTR_PARAMSET_SIZE (ODBC 3.0) */
			LOGF("SQLSetStmtAttr ->22. SQL_ATTR_PARAMSET_SIZE : Value = '%d'",
					1000);
			retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_PARAMSET_SIZE,
					(SQLPOINTER) 1000, StrLengthPtr);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
					"SQLSetStmtAttr");

			/* *** 23. SQL_ATTR_QUERY_TIMEOUT */
			LOGF("SQLSetStmtAttr ->23. SQL_ATTR_QUERY_TIMEOUT : Value = '%d'",
					100);
			retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_QUERY_TIMEOUT,
					(SQLPOINTER) 100, StrLengthPtr);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
					"SQLSetStmtAttr");

			/* *** 24. SQL_ATTR_RETRIEVE_DATA (ODBC 2.0) */
			LOGF(
					"SQLSetStmtAttr ->24. SQL_ATTR_RETRIEVE_DATA : Value = 'SQL_RD_OFF'");
			retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_RETRIEVE_DATA,
					(SQLPOINTER) SQL_RD_OFF, StrLengthPtr);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_ERROR, retcode,
					"SQLSetStmtAttr");

			LOGF(
					"SQLSetStmtAttr ->24. SQL_ATTR_RETRIEVE_DATA : Value = 'SQL_RD_ON'");
			retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_RETRIEVE_DATA,
					(SQLPOINTER) SQL_RD_ON, StrLengthPtr);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_ERROR, retcode,
					"SQLSetStmtAttr");

			/* *** 25. SQL_ATTR_ROW_ARRAY_SIZE (ODBC 3.0) */
			LOGF("SQLSetStmtAttr ->25. SQL_ATTR_ROW_ARRAY_SIZE : Value = '%d'",
					1024);
			retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_ROW_ARRAY_SIZE,
					(SQLPOINTER) 1024, StrLengthPtr);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
					"SQLSetStmtAttr");

			/* *** 26. SQL_ATTR_ROW_BIND_OFFSET_PTR (ODBC 3.0) */
			LOGF(
					"SQLSetStmtAttr ->26. SQL_ATTR_ROW_BIND_OFFSET_PTR : Value = 'NULL'",
					buffer);
			retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_ROW_BIND_OFFSET_PTR,
					(SQLPOINTER) NULL, StrLengthPtr);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
					"SQLSetStmtAttr");

			/* *** 27. SQL_ATTR_ROW_BIND_TYPE */
			LOGF(
					"SQLSetStmtAttr ->27. SQL_ATTR_ROW_BIND_TYPE : Value = 'SQL_BIND_BY_COLUMN'");
			retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_ROW_BIND_TYPE,
					(SQLPOINTER) SQL_BIND_BY_COLUMN, StrLengthPtr);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
					"SQLSetStmtAttr");

			/* *** 28. SQL_ATTR_ROW_NUMBER (OBDC 2.0) */
			LOGF("SQLSetStmtAttr ->28. SQL_ATTR_ROW_NUMBER : Value = '%d'", 1);
			retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_ROW_NUMBER, (SQLPOINTER) 1,
					StrLengthPtr);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_ERROR, retcode,
					"SQLSetStmtAttr");

			/* *** 29. SQL_ATTR_ROW_OPERATION_PTR (ODBC 3.0) */
			LOGF(
					"SQLSetStmtAttr ->29. SQL_ATTR_ROW_OPERATION_PTR : Value = 'NULL'",
					buffer);
			retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_ROW_OPERATION_PTR,
					(SQLPOINTER) NULL, StrLengthPtr);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
					"SQLSetStmtAttr");

			/* *** 30. SQL_ATTR_ROW_STATUS_PTR (ODBC 3.0) */
			LOGF(
					"SQLSetStmtAttr ->30. SQL_ATTR_ROW_STATUS_PTR : Value = 'NULL'",
					buffer);
			retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_ROW_STATUS_PTR,
					(SQLPOINTER) NULL, StrLengthPtr);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
					"SQLSetStmtAttr");

			/* *** 31. SQL_ATTR_ROWS_FETCHED_PTR (ODBC 2.0) */

			LOGF(
					"SQLSetStmtAttr ->30. SQL_ATTR_ROWS_FETCHED_PTR : Value = 'NULL'",
					buffer);
			retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_ROWS_FETCHED_PTR,
					(SQLPOINTER) NULL, StrLengthPtr);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
					"SQLSetStmtAttr");

			/* *** 32. SQL_ATTR_SIMULATE_CURSOR (ODBC 2.0) */
			LOGF(
					"SQLSetStmtAttr ->32. SQL_ATTR_SIMULATE_CURSOR : Value = 'SQL_SC_NON_UNIQUE'");
			retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_SIMULATE_CURSOR,
					(SQLPOINTER) SQL_SC_NON_UNIQUE, StrLengthPtr);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_ERROR, retcode,
					"SQLSetStmtAttr");
			LOGF(
					"SQLSetStmtAttr ->32. SQL_ATTR_SIMULATE_CURSOR : Value = 'SQL_SC_TRY_UNIQUE'");
			retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_SIMULATE_CURSOR,
					(SQLPOINTER) SQL_SC_TRY_UNIQUE, StrLengthPtr);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_ERROR, retcode,
					"SQLSetStmtAttr");
			LOGF(
					"SQLSetStmtAttr ->32. SQL_ATTR_SIMULATE_CURSOR : Value = 'SQL_SC_UNIQUE'");
			retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_SIMULATE_CURSOR,
					(SQLPOINTER) SQL_SC_UNIQUE, StrLengthPtr);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_ERROR, retcode,
					"SQLSetStmtAttr");

			/* *** 33. SQL_ATTR_USE_BOOKMARKS (ODBC 2.0) */
			LOGF(
					"SQLSetStmtAttr ->33. SQL_ATTR_USE_BOOKMARKS : Value = 'SQL_UB_OFF'");
			retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_USE_BOOKMARKS,
					(SQLPOINTER) SQL_UB_OFF, StrLengthPtr);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_ERROR, retcode,
					"SQLSetStmtAttr");

			LOGF(
					"SQLSetStmtAttr ->33. SQL_ATTR_USE_BOOKMARKS : Value = 'SQL_UB_ON'");
			retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_USE_BOOKMARKS,
					(SQLPOINTER) SQL_UB_ON, StrLengthPtr);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_ERROR, retcode,
					"SQLSetStmtAttr");

			/* ***  34. SQL_GET_BOOKMARK (ODBC 2.0) */
			LOGF("SQLSetStmtAttr ->34. SQL_GET_BOOKMARK : Value = '%d'", 10);
			retcode = SQLSetStmtAttr(hstmt, SQL_GET_BOOKMARK, (SQLPOINTER) 10,
					StrLengthPtr);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_ERROR, retcode,
					"SQLSetStmtAttr");

			/* *** 35. SQL_ROWSET_SIZE (ODBC 2.0) */
			LOGF("SQLSetStmtAttr ->35. SQL_ROWSET_SIZE : Value = '%d'", 100);
			retcode = SQLSetStmtAttr(hstmt, SQL_ROWSET_SIZE, (SQLPOINTER) 100,
					StrLengthPtr);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_ERROR, retcode,
					"SQLSetStmtAttr");

			/* --- SQLSetStmtOption -------------------------------------------------- */
			//  LOGF("\tSetStmtAttr -> ");
			/* *** 3. SQL_ATTR_ASYNC_ENABLE */
			/*printf("SQLSetStmtAttr ->3. SQL_ATTR_ASYNC_ENABLE : Value = '%s'");
			 retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_ASYNC_ENABLE,
			 (SQLPOINTER) SQL_ASYNC_ENABLE_ON, StrLengthPtr);
			 if (retcode != SQL_SUCCESS) retcode--;
			 DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,"SQLSetStmtAttr");*/

			/* *** 16. SQL_ATTR_NOSCAN */
			/* printf("SQLSetStmtAttr ->16. SQL_ATTR_NOSCAN : (-> SQL_NOSCAN_ON) ");
			 retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_NOSCAN,
			 (SQLPOINTER) SQL_NOSCAN_ON, StrLengthPtr);
			 DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,"SQLSetStmtAttr");*/

			/* --- Disconnect -------------------------------------------------- */
			//free sql handles (stmt, dbc, env)
			FREE_SQLHANDLES
}
END_TEST(testSQLSetStmtAttr)

BEGIN_TEST(testSQLQueryTimeout)
{
			/* ------------------------------------------------------------------------- */
			SQLCHAR sqlstate[10];
			SQLINTEGER esq_sql_code;
			SQLCHAR error_txt[ERROR_TEXT_LEN + 1];
			SQLSMALLINT len_error_txt = ERROR_TEXT_LEN;
			SQLSMALLINT used_error_txt;
			char buffer[1024];

			/* ---------------------------------------------------------------------har- */
			//initialize the sql handles
			INIT_SQLHANDLES

			/* ------------------------------------------------------------------------- */

			/* --- Create Table --------------------------------------------- */
			LOGF(
					"Create Stmt = 'CREATE TABLE SQLQUERYTIMEOUT(ID INTEGER, NAME VARCHAR(80), AGE SMALLINT)'");
			retcode =
					SQLExecDirect(hstmt,
							(SQLCHAR*) "CREATE TABLE SQLQUERYTIMEOUT(ID INTEGER, NAME VARCHAR(80), AGE SMALLINT)",
							SQL_NTS);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
					"SQLExecDirect");

			/* --- Insert values --------------------------------------------- */
			LOGF("Insert Stmt= 'INSERT INTO SQLQUERYTIMEOUT VALUES (?, ?, ?)'");
			retcode = SQLPrepare(hstmt,
					(SQLCHAR*) "INSERT INTO SQLQUERYTIMEOUT VALUES (?, ?, ?)",
					SQL_NTS);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
					"SQLPrepare");

			int id = 0;
			int age = 0;
			char name[100];
			retcode = SQLBindParameter(hstmt, 1, SQL_PARAM_INPUT, SQL_C_LONG,
					SQL_INTEGER, 0, 0, &id, 0, NULL);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
					"SQLBindParameter (SQL_INTEGER)");

			retcode = SQLBindParameter(hstmt, 2, SQL_PARAM_INPUT, SQL_C_CHAR,
					SQL_CHAR, MAX_NAME_LEN, 0, name, 0, NULL);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
					"SQLBindParameter (SQL_CHAR)");

			retcode = SQLBindParameter(hstmt, 3, SQL_PARAM_INPUT, SQL_C_LONG,
					SQL_INTEGER, 0, 0, &age, 0, NULL);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
					"SQLBindParameter (SQL_INTEGER)");
			for (int i = 1; i <= 10; i++) {
				id = i;
				age = i * 5;
				sprintf(name, "User%d", i);
				retcode = SQLExecute(hstmt);
				DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
						"SQLExecute");
			}

			retcode = SQLFreeStmt(hstmt, SQL_RESET_PARAMS);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
					"SQLFreeStmt");

			/* --- create DAPs for adding/removing observer --------------------------------------- */
  	        retcode = SQLExecDirect(hstmt, (SQLCHAR*)"DROP PROCEDURE IF EXISTS PROC_ADD_QUERY_OBSRVR", SQL_NTS);
 		    DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode, "SQLExecDirect");

		    retcode = SQLExecDirect(hstmt, (SQLCHAR*)"DROP PROCEDURE IF EXISTS PROC_REMOVE_QUERY_OBSRVR", SQL_NTS);
		    DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode, "SQLExecDirect");

			LOG("\tCreate stored proedure Stmt= 'CREATE PROCEDURE PROC_ADD_QUERY_OBSRVR(sleepTime int) LANGUAGE JAVA "
							"PARAMETER STYLE JAVA EXTERNAL NAME 'tests.TestProcedures.addQueryObserver''");
			retcode = SQLExecDirect(hstmt,
							(SQLCHAR*) "CREATE PROCEDURE PROC_ADD_QUERY_OBSRVR(sleepTime int) LANGUAGE JAVA "
									"PARAMETER STYLE JAVA EXTERNAL NAME 'tests.TestProcedures.addQueryObserver'",
							SQL_NTS);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
					"SQLExecDirect");

			LOG("\tCreate stored proedure Stmt= 'CREATE PROCEDURE PROC_REMOVE_QUERY_OBSRVR() LANGUAGE JAVA "
							"PARAMETER STYLE JAVA EXTERNAL NAME 'tests.TestProcedures.removeQueryObserver''");
			retcode = SQLExecDirect(hstmt,
							(SQLCHAR*) "CREATE PROCEDURE PROC_REMOVE_QUERY_OBSRVR() LANGUAGE JAVA "
									"PARAMETER STYLE JAVA EXTERNAL NAME 'tests.TestProcedures.removeQueryObserver'",
							SQL_NTS);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
					"SQLExecDirect");

			/* --- execute DAPs for adding/removing observer --------------------------------------- */
			LOG("\tCall DAP Stmt= 'call PROC_ADD_QUERY_OBSRVR(2000)'");
			retcode = SQLExecDirect(hstmt,
					(SQLCHAR*) "call PROC_ADD_QUERY_OBSRVR(2000)", SQL_NTS);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
					"SQLExecDirect");

			/**---------- set the qury timeout -------------------------------*/
  		    retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_QUERY_TIMEOUT, (SQLPOINTER) 1, NULL);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode, "SQLSetStmtAttr");

			/* --- Query table --------------------------------------------- */
			LOG("\tSelect Stmt= 'SELECT * FROM SQLQUERYTIMEOUT where id > ?'");
			retcode = SQLPrepare(hstmt,
					(SQLCHAR*) "SELECT name, age FROM SQLQUERYTIMEOUT where id > ?",
					SQL_NTS);
			retcode = SQLBindParameter(hstmt, 1, SQL_PARAM_INPUT, SQL_C_LONG,
					SQL_INTEGER, 0, 0, &id, 0, NULL);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
					"SQLBindParameter (SQL_INTEGER)");
			id = 1;

			retcode = SQLBindCol(hstmt, 1, SQL_C_CHAR, name, 100, NULL);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
					"SQLBindCol (SQL_CHAR)");

			retcode = SQLBindCol(hstmt, 2, SQL_C_LONG, &age, 0, NULL);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
					"SQLBindCol (SQL_INTEGER)");

			retcode = SQLExecute(hstmt);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_ERROR, retcode,
					"SQLExecute");

			if(retcode != SQL_ERROR)
			{
			  do {
				retcode = SQLFetch(hstmt);
				LOGF("name = %s, age = %d", name, age);
				LOGF("retcode is %d", retcode);
			  } while (retcode != SQL_NO_DATA && retcode != SQL_ERROR);

			  DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode, "SQLFetch");
			}

			retcode = SQLFreeStmt(hstmt, SQL_RESET_PARAMS);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
					"SQLFreeStmt");

			LOG("\tCall DAP Stmt= 'call PROC_REMOVE_QUERY_OBSRVR()'");
			retcode = SQLExecDirect(hstmt,
					(SQLCHAR*) "call PROC_REMOVE_QUERY_OBSRVR()", SQL_NTS);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
					"SQLExecDirect");

			retcode = SQLFreeStmt(hstmt, SQL_CLOSE);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
					"SQLFreeStmt");
			/* ---------------------------------------------------------------------har- */

			/* --- Drop Table ----------------------------------------------- */
			LOGF("Drop Stmt= 'DROP TABLE SQLQUERYTIMEOUT'");
			retcode = SQLExecDirect(hstmt, (SQLCHAR*) "DROP TABLE SQLQUERYTIMEOUT",
					SQL_NTS);
			DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
					"SQLExecDirect");

			/* - Disconnect ---------------------------------------------------- */
			//free sql handles
			FREE_SQLHANDLES
}
END_TEST(testSQLQueryTimeout)
