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



BEGIN_TEST(testSQLConnect)
{
  retcode = ::SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
  ASSERT(retcode == SQL_SUCCESS, "SQLAllocHandle call failed");
  ASSERT(henv != NULL, "SQLAllocHandle failed to return valid env handle");

  retcode = ::SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);
  ASSERT(retcode == SQL_SUCCESS, "SQLAllocHandle call failed");
  ASSERT(hdbc != NULL, "SQLAllocHandle failed to return valid DBC handle");


  retcode = ::SQLConnect(SQL_NULL_HANDLE, NULL, 0, NULL, 0, NULL, 0);
  ASSERT(retcode == SQL_INVALID_HANDLE, "SQLConnect should return invalid handle");

  //retcode = ::SQLConnect(henv, NULL, 0, NULL, 0, NULL, 0);

#if defined(WIN32) || defined (_WIN32)
  retcode = ::SQLConnect(hdbc, NULL, 0, NULL, 0, NULL, 0);
  ASSERT(retcode == SQL_ERROR, "SQLConnect should return SQL_ERROR");
  
  retcode = ::SQLConnect(hdbc, (SQLCHAR*)"gfxddsn", 0, NULL, 0, NULL, 0);
  ASSERT(retcode == SQL_ERROR, "SQLConnect should return SQL_ERROR");

#endif
  retcode = ::SQLConnect(hdbc, (SQLCHAR*)"gfxddsn", SQL_NTS, NULL, 0, NULL, 0);
  ASSERT(retcode == SQL_SUCCESS, "SQLConnect call failed");

  retcode =::SQLDisconnect(hdbc);
  ASSERT(retcode == SQL_SUCCESS, "SQLDisconnect  call failed");

  retcode = ::SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
  ASSERT(retcode == SQL_SUCCESS, "SQLFreeHandle call failed");

  retcode = ::SQLFreeHandle(SQL_HANDLE_ENV, henv);
  ASSERT(retcode == SQL_SUCCESS, "SQLFreeHandle call failed");

}
END_TEST(testSQLConnect)
