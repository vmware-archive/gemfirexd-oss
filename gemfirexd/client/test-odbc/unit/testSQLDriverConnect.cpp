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

BEGIN_TEST(testSQLDriverConnect)
{
    retcode = ::SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
    ASSERT(retcode == SQL_SUCCESS, "SQLAllocHandle call failed");
    ASSERT(henv != NULL, "SQLAllocHandle failed to return valid env handle");

    retcode = ::SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);
    ASSERT(retcode == SQL_SUCCESS, "SQLAllocHandle call failed");
    ASSERT(hdbc != NULL, "SQLAllocHandle failed to return valid DBC handle");

    retcode = ::SQLDriverConnect(NULL, NULL, NULL, 0, NULL, 0, NULL, 0);
    ASSERT(retcode == SQL_INVALID_HANDLE, "SQLDriverConnect should return invalid handle");

    retcode = ::SQLDriverConnect(NULL, NULL, NULL, 0, NULL, 0, NULL, SQL_DRIVER_PROMPT);
    ASSERT(retcode == SQL_ERROR, "SQLDriverConnect should return sql error");

#if defined(WIN32) || defined(_WIN32)
    
    retcode = ::SQLDriverConnect(hdbc, NULL, (SQLCHAR*)"InvalidConnectionString",
        strlen("InvalidConnectionString"), NULL, 0, NULL, SQL_DRIVER_NOPROMPT );
    ASSERT(retcode == SQL_ERROR, "SQLDriverConnect should return sql error");
    retcode = ::SQLDriverConnect(hdbc, NULL, (SQLCHAR*)"InvalidKey=Value",
        strlen("InvalidKey=Value"), NULL, 0, NULL, SQL_DRIVER_NOPROMPT );
    ASSERT(retcode == SQL_ERROR, "SQLDriverConnect should return sql error");
    retcode = ::SQLDriverConnect(hdbc, NULL, (SQLCHAR*)"DSN=Invalid",
        strlen("DSN=Invalid"), NULL, 0, NULL, SQL_DRIVER_NOPROMPT );
    ASSERT(retcode == SQL_ERROR, "SQLDriverConnect should return sql error");

    retcode = ::SQLDriverConnect(hdbc, NULL, (SQLCHAR*)"DSN=",
        strlen("DSN="), NULL, 0, NULL, SQL_DRIVER_NOPROMPT );
    ASSERT(retcode == SQL_ERROR, "SQLDriverConnect should return sql error");

    retcode = ::SQLDriverConnect(hdbc, NULL, (SQLCHAR*)"DSN=DEFAULT",
        strlen("DSN=DEFAULT"), NULL, 0, NULL, SQL_DRIVER_NOPROMPT );
    ASSERT(retcode == SQL_ERROR, "SQLDriverConnect should return sql error");

#endif

    SQLCHAR OutConnStr[255];
    SQLSMALLINT  OutConnStrLen =0;
    retcode = ::SQLDriverConnect(hdbc, NULL, (SQLCHAR*)GFXDCONNSTRING,
        SQL_NTS, OutConnStr, 255, &OutConnStrLen, SQL_DRIVER_NOPROMPT );
    LOGF("\nout conn str is %s and length is %d", OutConnStr, OutConnStrLen);
    ASSERT(strlen((char*)OutConnStr) == OutConnStrLen, "OutConnStrLen and Actual length should match");
    ASSERT(strcmp((char*)OutConnStr, "jdbc:gemfirexd://127.0.0.1:1999/") == 0, "Returned output connection string sould match");

    retcode = ::SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
    ASSERT(retcode == SQL_SUCCESS, "SQLFreeHandle call failed");

    retcode = ::SQLFreeHandle(SQL_HANDLE_ENV, henv);
    ASSERT(retcode == SQL_SUCCESS, "SQLFreeHandle call failed");

}
END_TEST(testSQLDriverConnect)
