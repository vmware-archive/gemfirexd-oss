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

BEGIN_TEST(testSQLFreeHandle)
{

  retcode = ::SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
  ASSERT(retcode == SQL_SUCCESS, "SQLAllocHandle call failed");
  ASSERT(henv != NULL, "SQLAllocHandle failed to return valid env handle");

  retcode = ::SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);
  ASSERT(retcode != SQL_ERROR, "SQLAllocHandle returned SQL_ERROR");
  ASSERT(hdbc != NULL, "SQLAllocHandle failed to return valid DBC handle");

  retcode = ::SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
  ASSERT(retcode == SQL_ERROR, "SQLAllocHandle should return SQL_ERROR");

  retcode = ::SQLFreeHandle(100, SQL_NULL_HANDLE);
  ASSERT(retcode == SQL_INVALID_HANDLE, "SQLAllocHandle returned SQL_ERROR");

  retcode = ::SQLFreeHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE);
  ASSERT(retcode == SQL_INVALID_HANDLE, "SQLAllocHandle returned SQL_ERROR");

  retcode = ::SQLFreeHandle(100, henv);
  ASSERT(retcode == SQL_ERROR, "SQLFreeHandle should return SQL_ERROR");

  retcode = ::SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
  ASSERT(retcode == SQL_INVALID_HANDLE, "SQLFreeHandle call failed");

  retcode = ::SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
  ASSERT(retcode == SQL_SUCCESS, "SQLFreeHandle call failed");

  retcode = ::SQLFreeHandle(SQL_HANDLE_ENV, henv);
  ASSERT(retcode == SQL_SUCCESS, "SQLFreeHandle call failed");

}
END_TEST(testSQLFreeHandle)
