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
 
// This is a sample test added for debugging purpose
#include "GemFireXDHelper.h"
using namespace std;

BEGIN_TEST(testSQLAllocHandle)
{
      retcode = ::SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE,
          SQL_NULL_HANDLE);
      ASSERT(retcode == SQL_INVALID_HANDLE,
          "SQLAllocHandle should return SQL_INVALID_HANDLE");

      retcode = ::SQLAllocHandle(100, SQL_NULL_HANDLE, SQL_NULL_HANDLE);
      ASSERT(retcode == SQL_ERROR, "SQLAllocHandle should return SQL_ERROR");

      retcode = ::SQLAllocHandle(100, SQL_NULL_HANDLE, &henv);
      ASSERT(retcode == SQL_ERROR, "SQLAllocHandle should return SQL_ERROR");
      ASSERT(henv == SQL_NULL_HANDLE,
          "SQLAllocHandle should return NULL handle");

      retcode = ::SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
      ASSERT(retcode == SQL_SUCCESS, "SQLAllocHandle call failed");
      ASSERT(henv != NULL, "SQLAllocHandle failed to return valid env handle");

      SQLHENV henv1 = SQL_NULL_HANDLE;
      retcode = ::SQLAllocHandle(SQL_HANDLE_ENV, henv, &henv1);
      ASSERT(retcode == SQL_INVALID_HANDLE,
          "SQLAllocHandle should return SQL_INVALID_HANDLE");
      ASSERT(henv1 == SQL_NULL_HANDLE,
          "SQLAllocHandle should set HENV handle to NULL handle");

      retcode = ::SQLAllocHandle(SQL_HANDLE_DBC, SQL_NULL_HANDLE,
          SQL_NULL_HANDLE);
      ASSERT(retcode == SQL_INVALID_HANDLE,
          "SQLAllocHandle should return SQL_INVALID_HANDLE");

      retcode = ::SQLAllocHandle(SQL_HANDLE_DBC, SQL_NULL_HANDLE, &hdbc);
      ASSERT(retcode == SQL_INVALID_HANDLE,
          "SQLAllocHandle should return SQL_INVALID_HANDLE");

      retcode = ::SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);
      ASSERT(retcode != SQL_ERROR, "SQLAllocHandle returned SQL_ERROR");
      ASSERT(hdbc != NULL, "SQLAllocHandle failed to return valid DBC handle");

      retcode = ::SQLAllocHandle(SQL_HANDLE_STMT, SQL_NULL_HANDLE,
          SQL_NULL_HANDLE);
      ASSERT(retcode == SQL_INVALID_HANDLE,
          "SQLAllocHandle should return SQL_INVALID_HANDLE");

      retcode = ::SQLAllocHandle(SQL_HANDLE_STMT, SQL_NULL_HANDLE, &hstmt);
      ASSERT(retcode == SQL_INVALID_HANDLE,
          "SQLAllocHandle should return SQL_INVALID_HANDLE");

      /*retcode = ::SQLAllocHandle(SQL_HANDLE_STMT, henv, &hstmt);
       ASSERT(retcode == SQL_SUCCESS, "SQLAllocHandle returned SQL_ERROR");
       ASSERT(hstmt != NULL, "SQLAllocHandle failed to return valid env handle");*/

      retcode = ::SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
      ASSERT(retcode == SQL_ERROR, "SQLAllocHandle should return SQL_ERROR as connection is not created");
       retcode = ::SQLAllocHandle(SQL_HANDLE_DESC, SQL_NULL_HANDLE, &hdesc);
      ASSERT(retcode == SQL_ERROR, "SQLAllocHandle should return SQL_ERROR");
      ASSERT(hdesc == NULL, "SQLAllocHandle should return NULL handle");

      retcode = ::SQLAllocHandle(SQL_HANDLE_DESC, hstmt, &hdesc);
      ASSERT(retcode == SQL_ERROR, "SQLAllocHandle should return SQL_ERROR");
      ASSERT(hdesc == NULL, "SQLAllocHandle should return NULL handle");
}
END_TEST(testSQLAllocHandle)


//test for testing multiple env handles
BEGIN_TEST(testSQLAllocHandle2)
{
  SQLHDBC hdbc1;
  SQLHENV henv1;
  SQLHSTMT hstmt1;

  retcode = SQLAllocHandle(SQL_HANDLE_ENV, NULL, &henv);
  DIAGRECCHECK(SQL_HANDLE_ENV, henv, 1, SQL_SUCCESS, retcode,"SQLAllocHandle (HENV)");

  retcode = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3,
          0);
  DIAGRECCHECK(SQL_HANDLE_ENV, henv, 1, SQL_SUCCESS, retcode,"SQLSetEnvAttr (HENV)")

  retcode = SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,"SQLAllocHandle (HDBC)");

  retcode = SQLDriverConnect(hdbc, NULL, (SQLCHAR*)GFXDCONNSTRING,
             SQL_NTS, NULL, 0, NULL, SQL_DRIVER_NOPROMPT );
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,"SQLDriverConnect");

  retcode = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
  DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,"SQLAllocHandle (HSTMT)")

  retcode = SQLAllocHandle(SQL_HANDLE_ENV, NULL, &henv1);
  DIAGRECCHECK(SQL_HANDLE_ENV, henv1, 1, SQL_SUCCESS, retcode,"SQLAllocHandle (HENV)");

  retcode = SQLSetEnvAttr(henv1, SQL_ATTR_ODBC_VERSION,
          (void*)SQL_OV_ODBC3, 0);
  DIAGRECCHECK(SQL_HANDLE_ENV, henv1, 1, SQL_SUCCESS, retcode,"SQLSetEnvAttr (HENV)")

  retcode = SQLAllocHandle(SQL_HANDLE_DBC, henv1, &hdbc1);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,"SQLAllocHandle (HDBC)");

  retcode = SQLDriverConnect(hdbc1, NULL, (SQLCHAR*)GFXDCONNSTRING,
               SQL_NTS, NULL, 0, NULL, SQL_DRIVER_NOPROMPT );
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc1, 1, SQL_SUCCESS, retcode,"SQLDriverConnect");

  retcode = SQLAllocHandle(SQL_HANDLE_STMT, hdbc1, &hstmt1);
  DIAGRECCHECK(SQL_HANDLE_STMT, hstmt1, 1, SQL_SUCCESS, retcode,"SQLAllocHandle (HSTMT)");

  retcode = SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
  DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,"SQLFreeHandle (HSTMT)");


  retcode = SQLDisconnect(hdbc);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,"SQLDisconnect");

  retcode = SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,"SQLFreeHandle (HDBC)");

  retcode = SQLFreeHandle(SQL_HANDLE_ENV, henv);
  DIAGRECCHECK(SQL_HANDLE_ENV, henv, 1, SQL_SUCCESS, retcode,"SQLFreeHandle (HENV)");

  retcode = SQLFreeHandle(SQL_HANDLE_STMT, hstmt1);
  DIAGRECCHECK(SQL_HANDLE_STMT, hstmt1, 1, SQL_SUCCESS, retcode,"SQLFreeHandle (HSTMT)");

  retcode = SQLDisconnect(hdbc1);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc1, 1, SQL_SUCCESS, retcode,"SQLDisconnect");

  retcode = SQLFreeHandle(SQL_HANDLE_DBC, hdbc1);
  DIAGRECCHECK(SQL_HANDLE_DBC, hdbc1, 1, SQL_SUCCESS, retcode,"SQLFreeHandle (HDBC)");

  retcode = SQLFreeHandle(SQL_HANDLE_ENV, henv1);
  DIAGRECCHECK(SQL_HANDLE_ENV, henv1, 1, SQL_SUCCESS, retcode,"SQLFreeHandle (HENV)");
}
END_TEST(testSQLAllocHandle2)
