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
/*
 * GemFireXDHelper.h
 *
 *  Created on: Mar 29, 2013
 *      Author: shankarh
 */

#ifndef GEMFIREXDHELPER_H_
#define GEMFIREXDHELPER_H_
#include "fw_helper.hpp"

extern "C"
{
#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>
}

#include <ace/OS.h>
#include <ace/Task.h>
#include <ace/Barrier.h>


#ifndef WIN32
#define TRUE true
#define FALSE false
#endif

//*************************  Utility Functions  ****************************
//*  This section contains internal utility functions
//**************************************************************************

/* OUTPUT Parameter */
#define OUTPUT 1
#define NO_OUTPUT -1
#define OUTPUTCH 63                     /* Zeichen : '?' */

/* BREAK Parameter */
#define PRG_BREAK 3
#define DROP_TABLE

// **************************************************************************
#define DIAGRECCHECK(handletype, handle,  recnum, expected, actual, funcName) \
{\
  if(!DiagRec_Check(handletype, (SQLHANDLE) handle, recnum, \
                    expected, actual, (LPSTR) funcName, __LINE__)) \
    if (expected != actual && !(expected == SQL_SUCCESS && actual == SQL_SUCCESS_WITH_INFO)) \
      FAIL("Test failed."); \
}

char* getstringForSQLCODE(SQLRETURN inputCode, char* outputStr)
{
  memset(outputStr, 0, 1024);
  switch (inputCode) {
    case SQL_SUCCESS:
      strcpy(outputStr, "SQL_SUCCESS");
      break;
    case SQL_SUCCESS_WITH_INFO:
      strcpy(outputStr, "SQL_SUCCESS_WITH_INFO");
      break;
    case SQL_ERROR:
      strcpy(outputStr, "SQL_ERROR");
      break;
    case SQL_INVALID_HANDLE:
      strcpy(outputStr, "SQL_INVALID_HANDLE");
      break;
    case SQL_NO_DATA:
      strcpy(outputStr, "SQL_NO_DATA");
      break;
    case SQL_STILL_EXECUTING:
      strcpy(outputStr, "SQL_STILL_EXECUTING");
      break;
    case SQL_NEED_DATA:
      strcpy(outputStr, "SQL_NEED_DATA");
      break;
  }
  return outputStr;
}

/* --------------------------------------------------------------------------
 | DiagRecCheck:
 |     This function will do a simple comparison of return codes and issue
 |     erros on failure.
 |
 | Returns:
 |
 --------------------------------------------------------------------------
 */
bool DiagRec_Check(SQLSMALLINT HandleType, SQLHANDLE Handle,
    SQLSMALLINT RecNumber, SQLRETURN expected, SQLRETURN actual,
    LPSTR szFuncName, int lineNumber)
{
  SQLRETURN api_rc;
  SQLCHAR sqlstate[10];
  SQLINTEGER esq_sql_code;
  SQLCHAR error_txt[1024];
  SQLSMALLINT len_error_txt = 1024; //ERROR_TEXT_LEN
  SQLSMALLINT used_error_txt;
  char outtxt[50]; //MAX_NAME_LEN
  char expectedStr[1024];
  char actualStr[1024];

  int opt = OUTPUT, i;
  bool retVal;
  char buffer[1024];

  i = strlen(szFuncName);
  if (szFuncName[i - 1] == OUTPUTCH) opt = NO_OUTPUT;

  if (opt != NO_OUTPUT) {
    memset(buffer, 0, 1024);
    sprintf(buffer, "%s -> retcode: %d at line %d\n", szFuncName, actual,
        lineNumber);
    LOGSTR(buffer);
  }

  if (opt != NO_OUTPUT) {
    memset(buffer, 0, 1024);
    sprintf(buffer, "%s -> Expected output : '%s' Actual output: '%s'\n",
        szFuncName, getstringForSQLCODE(expected, expectedStr),
        getstringForSQLCODE(actual, actualStr));
    LOGSTR(buffer);
  }

  if (expected != actual) {
    retVal = false;
  }
  else
    retVal = true;

  if (actual != SQL_SUCCESS) {
    api_rc = SQLGetDiagRec(HandleType, Handle, RecNumber, sqlstate,
        &esq_sql_code, error_txt, len_error_txt, &used_error_txt);
    if (opt != NO_OUTPUT) {
      if (api_rc == SQL_NO_DATA_FOUND) {
        LOGSTR("SQLGetDiagRec -> (SQL_NO_DATA_FOUND)\n");
      }
      else {
        if (actual < 0)
          strcpy(outtxt, "Error");
        else
          strcpy(outtxt, "Warning");

        memset(buffer, 0, 1024);
        sprintf(buffer, "SQLGetDiagRec (%s) -> SQLSTATE: %s SQLCODE: %d",
            outtxt, sqlstate, esq_sql_code);
        LOGSTR(buffer);
        memset(buffer, 0, 1024);
        sprintf(buffer, "SQLGetDiagRec (%s) -> Error message: %s\n", outtxt,
            error_txt);
        LOGSTR(buffer);
      }
    }
  }
  return retVal;
}

#define GFXDCONNSTRING "server=127.0.0.1;port=1999;user=gfxdodbc;password=gfxdodbc"

/* ---------------------------------------------------------------------har- */
#define INIT_SQLHANDLES retcode = SQLAllocHandle(SQL_HANDLE_ENV, NULL, &henv); \
    DIAGRECCHECK(SQL_HANDLE_ENV, henv, 1, SQL_SUCCESS, retcode,"SQLAllocHandle (HENV)"); \
    \
    retcode = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0); \
    DIAGRECCHECK(SQL_HANDLE_ENV, henv, 1, SQL_SUCCESS, retcode,"SQLSetEnvAttr (HENV)"); \
    \
    retcode = SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc); \
    DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,"SQLAllocHandle (HDBC)"); \
    \
    /*retcode = SQLConnect(hdbc, (SQLCHAR*)"gfxddsn", SQL_NTS, NULL, SQL_NTS, NULL, SQL_NTS);*/ \
    retcode = SQLDriverConnect(hdbc, NULL, (SQLCHAR*)GFXDCONNSTRING, \
           SQL_NTS, NULL, 0, NULL, SQL_DRIVER_NOPROMPT );\
   DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,"SQLDriverConnect"); \
    \
    retcode = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt); \
    DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,"SQLAllocHandle (HSTMT)");

/* ---------------------------------------------------------------------har- */

/* ---------------------------------------------------------------------har- */
#define FREE_SQLHANDLES  retcode = SQLFreeHandle(SQL_HANDLE_STMT, hstmt); \
    DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,"SQLFreeHandle (HSTMT)");  \
     \
     retcode = SQLDisconnect(hdbc); \
     DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,"SQLDisconnect"); \
     \
     retcode = SQLFreeHandle(SQL_HANDLE_DBC, hdbc); \
     DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,"SQLFreeHandle (HDBC)");\
     \
     retcode = SQLFreeHandle(SQL_HANDLE_ENV, henv); \
     DIAGRECCHECK(SQL_HANDLE_ENV, henv, 1, SQL_SUCCESS, retcode,"SQLFreeHandle (HENV)");

/* ---------------------------------------------------------------------har- */

//*---------------------------------------------------------------------------------
//| Get_pfSqlType:
//|
//|
//| Returns:
//*---------------------------------------------------------------------------------
RETCODE Get_pfSqlType(SQLSMALLINT pfSqlType, CHAR *buffer)
{
  switch (pfSqlType) {
    case SQL_BIGINT:
      strcpy(buffer, "SQL_BIGINT");
      break;
    case SQL_BINARY:
      strcpy(buffer, "SQL_BINARY");
      break;
    case SQL_BIT:
      strcpy(buffer, "SQL_BIT");
      break;
    case SQL_CHAR:
      strcpy(buffer, "SQL_CHAR");
      break;
    case SQL_DATE:
      strcpy(buffer, "SQL_DATE");
      break;
    case SQL_DECIMAL:
      strcpy(buffer, "SQL_DECIMAL");
      break;
    case SQL_DOUBLE:
      strcpy(buffer, "SQL_DOUBLE");
      break;
    case SQL_FLOAT:
      strcpy(buffer, "SQL_FLOAT");
      break;
    case SQL_INTEGER:
      strcpy(buffer, "SQL_INTEGER");
      break;
    case SQL_LONGVARBINARY:
      strcpy(buffer, "SQL_LONGVARBINARY");
      break;
    case SQL_LONGVARCHAR:
      strcpy(buffer, "SQL_LONGVARCHAR");
      break;
    case SQL_NUMERIC:
      strcpy(buffer, "SQL_NUMERIC");
      break;
    case SQL_REAL:
      strcpy(buffer, "SQL_REAL");
      break;
    case SQL_SMALLINT:
      strcpy(buffer, "SQL_SMALLINT");
      break;
    case SQL_TIME:
      strcpy(buffer, "SQL_TIME");
      break;
    case SQL_TIMESTAMP:
      strcpy(buffer, "SQL_TIMESTAMP");
      break;
    case SQL_TINYINT:
      strcpy(buffer, "SQL_TINYINT");
      break;
    case SQL_VARBINARY:
      strcpy(buffer, "SQL_VARBINARY");
      break;
    case SQL_VARCHAR:
      strcpy(buffer, "SQL_VARCHAR");
      break;
  }
  return (0);
}
//*---------------------------------------------------------------------------------
//| Get_pfNullable:
//|
//|
//| Returns:
//*---------------------------------------------------------------------------------
RETCODE Get_pfNullable(SQLSMALLINT pfNullable, CHAR *buffer)
{
  switch (pfNullable) {
    case SQL_NO_NULLS:
      strcpy(buffer, "SQL_NO_NULLS");
      break;
    case SQL_NULLABLE:
      strcpy(buffer, "SQL_NULLABLE");
      break;
    case SQL_NULLABLE_UNKNOWN:
      strcpy(buffer, "SQL_NULL_UNKNOWN");
      break;
  }
  return (0);
}
//*---------------------------------------------------------------------------------
//| Get_BoolValue:
//|
//|
//| Returns:
//*---------------------------------------------------------------------------------
RETCODE Get_BoolValue(SQLSMALLINT pfDesc, CHAR *buffer)
{
  switch (pfDesc) {
    case TRUE:
      strcpy(buffer, "TRUE");
      break;
    case FALSE:
      strcpy(buffer, "FALSE");
      break;
    default:
      strcpy(buffer, "<NULL>");
      break;
  }
  return (0);
}
//*---------------------------------------------------------------------------------
//| Get_Searchable:
//|
//|
//| Returns:
//*---------------------------------------------------------------------------------
RETCODE Get_Searchable(SQLSMALLINT pfDesc, CHAR *buffer)
{
  switch (pfDesc) {
    case SQL_UNSEARCHABLE:
      strcpy(buffer, "SQL_UNSEARCHABLE");
      break;
    case SQL_LIKE_ONLY:
      strcpy(buffer, "SQL_LIKE_ONLY");
      break;
    case SQL_ALL_EXCEPT_LIKE:
      strcpy(buffer, "SQL_ALL_EXCEPT_LIKE");
      break;
    case SQL_SEARCHABLE:
      strcpy(buffer, "SQL_SEARCHABLE");
      break;
    default:
      strcpy(buffer, "<NULL>");
      break;
  }
  return (0);
}
//*---------------------------------------------------------------------------------
//| Get_Updatable:
//|
//|
//| Returns:
//*---------------------------------------------------------------------------------
RETCODE Get_Updatable(SQLSMALLINT pfDesc, CHAR *buffer)
{
  switch (pfDesc) {
    case SQL_ATTR_READONLY:
      strcpy(buffer, "SQL_ATTR_READONLY");
      break;
    case SQL_ATTR_WRITE:
      strcpy(buffer, "SQL_ATTR_WRITE");
      break;
    case SQL_ATTR_READWRITE_UNKNOWN:
      strcpy(buffer, "SQL_ATTR_READWRITE_UNKNOWN");
      break;
    default:
      strcpy(buffer, "<NULL>");
      break;
  }
  return (0);
}

//*---------------------------------------------------------------------------------
//| lst_ColumnNames:
//|
//|
//| Returns:
//*---------------------------------------------------------------------------------
RETCODE lst_ColumnNames(SQLHENV henv, SQLHDBC hdbc, SQLHSTMT hstmt, int outcol)
{
  RETCODE rc;
  SQLSMALLINT sErr = 0;
  /* ------------------------------------------------------------------------- */
  SQLSMALLINT icol;
  SQLCHAR szColName[50];
  SQLSMALLINT cbColNameMax;
  SQLSMALLINT pcbColName;
  SQLSMALLINT pfSqlType;
  SQLUINTEGER pcbColDef;
  SQLSMALLINT pibScale;
  SQLSMALLINT pfNullable;
  /* ------------------------------------------------------------------------- */
  printf("\tColumns->|");

  icol = 1;
  cbColNameMax = 18;
  while (icol <= outcol) {
    rc = SQLDescribeCol(hstmt, icol, szColName, cbColNameMax, &pcbColName,
        &pfSqlType, (SQLULEN*)&pcbColDef, &pibScale, &pfNullable);
    printf("%s|", szColName);
    icol++;
  }
  printf("\r\n");

  return (rc);
}
/* ------------------------------------------------------------------------- */

#endif /* GEMFIREXDHELPER_H_ */
