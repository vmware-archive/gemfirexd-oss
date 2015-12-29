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

/* ------------------------------------------------------------------------- */
#define TESTNAME "SQLSETCONNECTATTR"
#define TABLE "SETCONNECT"

#define MAX_NAME_LEN 50
#define STRING_LEN 10
#define CHAR_LEN 120

/*
 Attribute                                     Set before or after connection?
 ===============================================================================
 SQL_ATTR_ACCESS_MODE                          Either[1]
 SQL_ATTR_AUTO_IPD                             Either
 SQL_ATTR_AUTOCOMMIT                           Either[5]
 SQL_ATTR_CONNECTION_DEAD                      After
 SQL_ATTR_CONNECTION_TIMEOUT                   Either
 SQL_ATTR_CURRENT_CATALOG                      Either[1]
 SQL_ATTR_LOGIN_TIMEOUT                        Before
 SQL_ATTR_METADATA_ID                          Either
 SQL_ATTR_ODBC_CURSORS                         Before
 SQL_ATTR_PACKET_SIZE                          Before
 SQL_ATTR_QUIET_MODE                           Either
 SQL_ATTR_TRACE                                Either
 SQL_ATTR_TRACEFILE                            Either
 SQL_ATTR_TRANSLATE_LIB                        After
 SQL_ATTR_TRANSLATE_OPTION                     After
 SQL_ATTR_TXN_ISOLATION                        Either[3]
 */

/*
 Attribute                     DataType       Possible values
 =======================================================================================
 SQL_ATTR_ACCESS_MODE          SQLUINTEGER    SQL_MODE_READ_ONLY, SQL_MODE_READ_WRITE
 SQL_ATTR_AUTO_IPD             SQLUINTEGER    SQL_TRUE, SQL_FALSE
 SQL_ATTR_AUTOCOMMIT           SQLUINTEGER    SQL_AUTOCOMMIT_ON , SQL_AUTOCOMMIT_OFF
 SQL_ATTR_CONNECTION_DEAD      SQLUINTEGER    SQL_CD_TRUE, SQL_CD_FALSE
 SQL_ATTR_CONNECTION_TIMEOUT   SQLUINTEGER    number of seconds to wait for any request on the connection
 SQL_ATTR_CURRENT_CATALOG      SQLCHAR*       name of the catalog
 SQL_ATTR_LOGIN_TIMEOUT        SQLUINTEGER    number of seconds to wait for a login request
 SQL_ATTR_METADATA_ID          SQLUINTEGER    SQL_TRUE, SQL_FALSE
 SQL_ATTR_ODBC_CURSORS         SQLULEN        SQL_CUR_USE_IF_NEEDED, SQL_CUR_USE_ODBC, SQL_CUR_USE_DRIVER
 SQL_ATTR_PACKET_SIZE          SQLUINTEGER    network packet size in bytes
 SQL_ATTR_QUIET_MODE           HWND handle    window handle
 SQL_ATTR_TRACE                SQLUINTEGER    SQL_OPT_TRACE_OFF, SQL_OPT_TRACE_ON
 SQL_ATTR_TRACEFILE            SQLCHAR*       name of the trace file
 SQL_ATTR_TRANSLATE_LIB        SQLCHAR*       name of the translation library
 SQL_ATTR_TRANSLATE_OPTION     32-bit flag    A 32-bit flag value that is passed to the translation DLL
 SQL_ATTR_TXN_ISOLATION        A 32-bit bitmask that sets the transaction isolation level for the current connection
 */
BEGIN_TEST(testSQLSetConnectAttr)
{
  /* ------------------------------------------------------------------------- */
      CHAR buffer[MAX_NAME_LEN + 1];

      SQLINTEGER pAccessMode, pAutoCommit, pLoginTimeout, pOdbcCursors,
          pOptTrace, pPacketSize, pQuietMode, pTranslateOption, pTxnIsolation,
          pAsyncEnable, pAutoIpd, pConTimeout, pMetadataId, vAutoCommit,
          vConTimeout;

      CHAR pCurrentQualifier[CHAR_LEN], pOptTraceFile[CHAR_LEN],
          pTranslateDLL[CHAR_LEN];

      SQLINTEGER BufferLength;
      SQLINTEGER StringLengthPtr = 0;

      /* ---------------------------------------------------------------------har- */

      /* --- Connect ----------------------------------------------------- */
      //init sql handles (stmt, dbc, env)
      INIT_SQLHANDLES

      LOG("SQL handle initialized");
      /* --- GetConnectAttr ---------------------------------------------- */

      /* *** SQL_ATTR_ACCESS_MODE --------------- *** */
      BufferLength = SQL_IS_UINTEGER;
      retcode = SQLGetConnectAttr(hdbc, SQL_ATTR_ACCESS_MODE, &pAccessMode,
          BufferLength, &StringLengthPtr);
      DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
          "SQLGetConnectAttr (SQL_ATTR_ACCESS_MODE)");

      /* *** SQL_ATTR_ASYNC_ENABLE --------- *** */
      /* *** ODBC 3.0 */
      BufferLength = SQL_IS_UINTEGER;
      retcode = SQLGetConnectAttr(hdbc, SQL_ATTR_ASYNC_ENABLE, &pAsyncEnable,
          BufferLength, &StringLengthPtr);
      //if (retcode != SQL_SUCCESS) retcode--;
      DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
          "SQLGetConnectAttr (SQL_ATTR_ASYNC_ENABLE)");

      /* *** SQL_ATTR_AUTO_IPD ------------ *** */
      /* *** ODBC 3.0 */
      BufferLength = SQL_IS_UINTEGER;
      retcode = SQLGetConnectAttr(hdbc, SQL_ATTR_AUTO_IPD, &pAutoIpd,
          BufferLength, &StringLengthPtr);
      DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
          "SQLGetConnectAttr (SQL_ATTR_AUTO_IPD)");

      /* *** SQL_ATTR_AUTOCOMMIT --------------- *** */
      BufferLength = SQL_IS_UINTEGER;
      retcode = SQLGetConnectAttr(hdbc, SQL_ATTR_AUTOCOMMIT, &pAutoCommit,
          BufferLength, &StringLengthPtr);
      DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
          "SQLGetConnectAttr (SQL_ATTR_AUTOCOMMIT)");

      /* *** SQL_ATTR_CONNECTION_TIMEOUT ------------ *** */
      /* *** ODBC 3.0 */
      BufferLength = SQL_IS_UINTEGER;
      retcode = SQLGetConnectAttr(hdbc, SQL_ATTR_CONNECTION_TIMEOUT,
          &pConTimeout, BufferLength, &StringLengthPtr);
      DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
          "SQLGetConnectAttr (SQL_ATTR_CONNECTION_TIMEOUT)");

      /* *** SQL_ATTR_CURRENT_CATALOG ---------- *** */
      /* *** ODBC 2.0 */
      BufferLength = MAX_NAME_LEN;
      retcode = SQLGetConnectAttr(hdbc, SQL_ATTR_CURRENT_CATALOG,
          pCurrentQualifier, BufferLength, &StringLengthPtr);
      //if (retcode != SQL_SUCCESS) retcode--;
      DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_NO_DATA, retcode,
          "SQLGetConnectAttr (SQL_ATTR_CURRENT_CATALOG)");

      /* *** SQL_ATTR_LOGIN_TIMEOUT --------------- *** */
      BufferLength = SQL_IS_UINTEGER;
      retcode = SQLGetConnectAttr(hdbc, SQL_ATTR_LOGIN_TIMEOUT, &pLoginTimeout,
          BufferLength, &StringLengthPtr);
      DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
          "SQLGetConnectAttr (SQL_ATTR_LOGIN_TIMEOUT)");

      /* *** SQL_ATTR_METADATA_ID ------------ *** */
      /* *** ODBC 3.0 */
      BufferLength = SQL_IS_UINTEGER;
      retcode = SQLGetConnectAttr(hdbc, SQL_ATTR_METADATA_ID, &pMetadataId,
          BufferLength, &StringLengthPtr);
      DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
          "SQLGetConnectAttr (SQL_ATTR_METADATA_ID)");

      /* *** SQL_ATTR_ODBC_CURSORS --------------- *** */
      /* *** ODBC 2.0 */
      BufferLength = SQL_IS_UINTEGER;
      retcode = SQLGetConnectAttr(hdbc, SQL_ATTR_ODBC_CURSORS, &pOdbcCursors,
          BufferLength, &StringLengthPtr);
      DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
          "SQLGetConnectAttr (SQL_ATTR_ODBC_CURSORS)");

      /* *** SQL_ATTR_TRACE --------------- *** */
      BufferLength = SQL_IS_UINTEGER;
      retcode = SQLGetConnectAttr(hdbc, SQL_ATTR_TRACE, &pOptTrace,
          BufferLength, &StringLengthPtr);
      DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
          "SQLGetConnectAttr (SQL_ATTR_TRACE)");

      /* *** SQL_ATTR_TRACEFILE --------------- *** */
      BufferLength = SQL_NTS;
      strcpy(pOptTraceFile, "");
      if (pOptTrace == SQL_OPT_TRACE_ON) {
        retcode = SQLGetConnectAttr(hdbc, SQL_ATTR_TRACEFILE, pOptTraceFile,
            BufferLength, &StringLengthPtr);
        DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_ERROR, retcode,
            "SQLGetConnectAttr (SQL_ATTR_TRACEFILE)");
      }
      /* *** SQL_ATTR_PACKET_SIZE --------------- *** */
      /* *** ODBC 2.0 */
      BufferLength = SQL_IS_UINTEGER;
      retcode = SQLGetConnectAttr(hdbc, SQL_ATTR_PACKET_SIZE, &pPacketSize,
          BufferLength, &StringLengthPtr);
      //if (retcode != SQL_SUCCESS) retcode--;
      DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
          "SQLGetConnectAttr (SQL_ATTR_PACKET_SIZE)");

      /* *** SQL_ATTR_QUIET_MODE --------------- *** */
      /* *** ODBC 2.0 */
      BufferLength = SQL_IS_UINTEGER;
      retcode = SQLGetConnectAttr(hdbc, SQL_ATTR_QUIET_MODE, &pQuietMode,
          BufferLength, &StringLengthPtr);
      DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_NO_DATA, retcode,
          "SQLGetConnectAttr (SQL_ATTR_PACKET_SIZE)");

      /* *** SQL_ATTR_TRANSLATE_LIB --------------- *** */
      BufferLength = MAX_NAME_LEN;
      retcode = SQLGetConnectAttr(hdbc, SQL_ATTR_TRANSLATE_LIB, pTranslateDLL,
          BufferLength, &StringLengthPtr);
      //if (retcode != SQL_SUCCESS) retcode--;
      DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_NO_DATA, retcode,
          "SQLGetConnectAttr (SQL_ATTR_TRANSLATE_LIB)");

      /* *** SQL_ATTR_TRANSLATE_OPTION ------------ *** */
      BufferLength = SQL_NTS;
      retcode = SQLGetConnectAttr(hdbc, SQL_ATTR_TRANSLATE_OPTION,
          &pTranslateOption, BufferLength, &StringLengthPtr);
      DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
          "SQLGetConnectAttr (SQL_ATTR_TRANSLATE_OPTION)");

      /* *** SQL_ATTR_TXN_ISOLATION --------------- *** */
      BufferLength = SQL_IS_UINTEGER;
      retcode = SQLGetConnectAttr(hdbc, SQL_ATTR_TXN_ISOLATION, &pTxnIsolation,
          BufferLength, &StringLengthPtr);
      DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
          "SQLGetConnectAttr (SQL_ATTR_TXN_ISOLATION)");

      /* --- Output ConnectOptons ---------------------------------------- */

      /* *** SQL_ATTR_ACCESS_MODE --------------- *** */
      switch (pAccessMode) {
        case (SQL_MODE_READ_ONLY):
          strcpy(buffer, "SQL_MODE_READ_ONLY");
          break;
        case (SQL_MODE_READ_WRITE):
          strcpy(buffer, "SQL_MODE_READ_WRITE");
          break;
        default:
          strcpy(buffer, "?????");
      }
      LOGF(" SQL_ATTR_ACCESS_MODE : '%s' ", buffer);

      /* *** SQL_ATTR_ASYNC_ENABLE ------------- *** */
      switch (pAsyncEnable) {
        case (SQL_ASYNC_ENABLE_ON):
          strcpy(buffer, "SQL_ASYNC_ENABLE_ON");
          break;
        case (SQL_ASYNC_ENABLE_OFF):
          strcpy(buffer, "SQL_ASYNC_ENABLE_OFF");
          break;
        default:
          strcpy(buffer, "?????");
      }
      LOGF(" SQL_ATTR_ASYNC_ENABLE  : '%s' ", buffer);

      /* *** SQL_ATTR_AUTO_IPD ---------------- *** */
      switch (pAutoIpd) {
        case (SQL_TRUE):
          strcpy(buffer, "SQL_TRUE");
          break;
        case (SQL_FALSE):
          strcpy(buffer, "SQL_FALSE");
          break;
        default:
          strcpy(buffer, "?????");
      }
      LOGF(" SQL_ATTR_AUTO_IPD  : '%s' ", buffer);

      /* *** SQL_ATTR_AUTOCOMMIT --------------- *** */
      switch (pAutoCommit) {
        case (SQL_AUTOCOMMIT_ON):
          strcpy(buffer, "SQL_AUTOCOMMIT_ON");
          break;
        case (SQL_AUTOCOMMIT_OFF):
          strcpy(buffer, "SQL_AUTOCOMMIT_OFF");
          break;
        default:
          strcpy(buffer, "?????");
      }
      LOGF(" SQL_ATTR_AUTOCOMMIT  : '%s' ", buffer);

      /* *** SQL_ATTR_AUTOCOMMIT --------------- *** */
      LOGF(" SQL_ATTR_CONNECTION_TIMEOUT : '%d' ", pConTimeout);

      /* *** SQL_ATTR_CURRENT_CATALOG --------------- *** */
      /* %s - pCurrentQualifier */
      LOGF(" SQL_ATTR_CURRENT_CATALOG : '-' ");

      /* *** SQL_ATTR_LOGIN_TIMEOUT --------- *** */
      LOGF(" SQL_ATTR_LOGIN_TIMEOUT : '%d' ", pLoginTimeout);

      /* *** SQL_ATTR_ODBC_CURSORS ---------- *** */
      /* %d - pOdbcCursors */
      switch (pOdbcCursors) {
        case (SQL_CUR_USE_IF_NEEDED):
          strcpy(buffer, "SQL_CUR_USE_IF_NEEDED");
          break;
        case (SQL_CUR_USE_ODBC):
          strcpy(buffer, "SQL_CUR_USE_ODBC");
          break;
        case (SQL_CUR_USE_DRIVER):
          strcpy(buffer, "SQL_CUR_USE_DRIVER");
          break;
        default:
          strcpy(buffer, "?????");
      }
      LOGF(" SQL_ATTR_ODBC_CURSORS  : '%s' ", buffer);

      /* *** SQL_ATTR_TRACE --------------- *** */
      switch (pOptTrace) {
        case (SQL_OPT_TRACE_ON):
          strcpy(buffer, "SQL_OPT_TRACE_ON");
          break;
        case (SQL_OPT_TRACE_OFF):
          strcpy(buffer, "SQL_OPT_TRACE_OFF");
          break;
        default:
          strcpy(buffer, "?????");
      }
      LOGF(" SQL_ATTR_TRACE : '%s' ", buffer);

      /* *** SQL_ATTR_TRACEFILE --------------- *** */
      LOGF(" SQL_ATTR_TRACEFILE : '%s' ", pOptTraceFile);

      /* *** SQL_ATTR_PACKET_SIZE ------------- *** */
      /* %d - pPacketSize */
      LOGF(" SQL_ATTR_PACKET_SIZE : '-' ");

      /* *** SQL_ATTR_QUIET_MODE -------------- *** */
      /* %d - pQuietMode */
      LOGF(" SQL_ATTR_QUIET_MODE  : '%d' ", pQuietMode);

      /* *** SQL_ATTR_TRANSLATE_LIB ----------- *** */
      /* %s - pTranslateDLL */
      LOGF(" SQL_ATTR_TRANSLATE_LIB : '%s' ", pTranslateDLL);

      /* *** SQL_ATTR_TRANSLATE_OPTION --------------- *** */
      /* %d - pTranslateOption */
      LOGF(" SQL_ATTR_TRANSLATE_OPTION  : '%d' ", pTranslateOption);

      /* *** SQL_ATTR_TXN_ISOLATION --------------- *** */
      /* %d - pTxnIsolation */
      switch (pTxnIsolation) {
        case (SQL_TXN_READ_UNCOMMITTED):
          strcpy(buffer, "SQL_TXN_READ_UNCOMMITTED");
          break;
        case (SQL_TXN_READ_COMMITTED):
          strcpy(buffer, "SQL_TXN_READ_COMMITTED");
          break;
        case (SQL_TXN_REPEATABLE_READ):
          strcpy(buffer, "SQL_TXN_REPEATABLE_READ");
          break;
        case (SQL_TXN_SERIALIZABLE):
          strcpy(buffer, "SQL_TXN_SERIALIZABLE");
          break;
        default:
          strcpy(buffer, "?????");
      }
      LOGF(" SQL_ATTR_TXN_ISOLATION : '%s' ", buffer);

      /* --- SQLSetConnectAttr ----------------------------------------- */

      if (pAutoCommit == SQL_AUTOCOMMIT_ON) {
        LOGF(" Set SQL_ATTR_AUTOCOMMIT to OFF ");
        vAutoCommit = SQL_AUTOCOMMIT_OFF; /* (o. SQL_AUTOCOMMIT_ON) */
        retcode = SQLSetConnectAttr(hdbc, SQL_ATTR_AUTOCOMMIT,
            SQL_AUTOCOMMIT_OFF, SQL_IS_UINTEGER);
        DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
            "SQLSetConnectAttr (SQL_AUTOCOMMIT_OFF)");
      }
      vConTimeout = 60;
      if (vConTimeout > 0) {
        LOGF(" Set SQL_ATTR_CONNECTION_TIMEOUT = '%d' ", vConTimeout);
        retcode = SQLSetConnectAttr(hdbc, SQL_ATTR_CONNECTION_TIMEOUT,
            (SQLPOINTER)vConTimeout, SQL_IS_UINTEGER);
        DIAGRECCHECK(SQL_HANDLE_DBC, hdbc, 1, SQL_SUCCESS, retcode,
            "SQLSetConnectAttr (SQL_ATTR_CONNECTION_TIMEOUT)");
      }

      /* --- Disconnect -------------------------------------------------- */
      //free sql handles (stmt, dbc, env)
      FREE_SQLHANDLES
}
END_TEST(testSQLSetConnectAttr)
