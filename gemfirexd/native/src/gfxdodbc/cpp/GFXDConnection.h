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

/**
 * GFXDConnection.h
 *
 *  Defines wrapper class for the underlying native Connection.
 *
 *      Author: swale
 */

#ifndef GFXDCONNECTION_H_
#define GFXDCONNECTION_H_

#include <Connection.h>

#include "GFXDEnvironment.h"
#include "GFXDDefaults.h"
#include "Library.h"

#define PRODUCT_NAME 1000

namespace com
{
  namespace pivotal
  {
    namespace gemfirexd
    {
      class GFXDEnvironment;
      class GFXDStatement;

      /**
       * Encapsulates a native {@link Connection} and adds ODBC
       * specific methods and state.
       */
      class GFXDConnection : public GFXDHandleBase
      {
      private:
        /** the underlying native connection */
        Connection m_conn;

        /** the current GFXDEnvironment */
        GFXDEnvironment* const m_env;

        static const int TRANSACTION_UNKNOWN = -1;

        /**
         * set of attributes set for this connection using
         * {@link #setAttribute}
         */
        AttributeMap m_attributes;

        /**
         * if true then use case insensitive arguments to meta-data queries
         * else the values are case sensitive
         */
        bool m_argsAsIdentifiers;

        /**
         * Handle of the parent window used to display any dialog boxes.
         * If this is null then no dialogs will be displayed.
         */
        SQLPOINTER m_hwnd;

        /**
         * Translation option argument for the transaction function invocations.
         */
        UDWORD m_translateOption;

        /**
         * The translation library associated with this connection, if any.
         */
        native::Library* m_translationLibrary;

        /**
         * Translation function for charset from DataSource to Driver loaded
         * from the translation library, if any.
         */
        DataSourceToDriver m_dataSourceToDriver;

        /**
         * Translation function for charset from Driver to DataSource loaded
         * from the translation library, if any.
         */
        DriverToDataSource m_driverToDataSource;

        /** GFXDEnvironment needs to access the private destructor */
        friend class GFXDEnvironment;

        /** let GFXDStatement access the private fields */
        friend class GFXDStatement;

        /**
         * Constructor for a GFXDConnection given handle to GFXDEnvironment.
         */
        GFXDConnection(GFXDEnvironment* env);

        ~GFXDConnection();

        /**
         * Establish a new connection to the GemFireXD system. The network server
         * location is specified in the passed "server" and "port" arguments,
         * while the other connection properties (including user/password,
         *   if any) is passed in the "connProps" property bag.
         */
        template<typename CHAR_TYPE>
        SQLRETURN connectT(const std::string& server, const int port,
            const Properties& connProps, CHAR_TYPE* outConnStr,
            const SQLINTEGER outConnStrLen, SQLSMALLINT* connStrLen);

        /**
         * Set an ODBC connection attribute on native connection.
         */
        SQLRETURN setConnectionAttribute(SQLINTEGER attribute,
            const AttributeValue& attrValue);

        /**
         * Fill in a string value from given string.
         */
        template<typename CHAR_TYPE, typename CHAR_TYPE2>
        SQLRETURN getStringValue(const CHAR_TYPE* str, const SQLINTEGER len,
            CHAR_TYPE2* resultValue, SQLINTEGER bufferLength,
            SQLINTEGER* stringLengthPtr, const char* op);

        /**
         * Get the value of an ODBC connection attribute set previously
         * using {@link #setAttribute}.
         */
        template<typename CHAR_TYPE>
        SQLRETURN getAttributeT(SQLINTEGER attribute, SQLPOINTER value,
            SQLINTEGER bufferLength, SQLINTEGER* stringLengthPtr);

        /**
         * Returns more information about the connection as in ODBC SQLGetInfo.
         */
        template<typename CHAR_TYPE>
        SQLRETURN getInfoT(SQLUSMALLINT infoType, SQLPOINTER infoValue,
            SQLSMALLINT bufferLength, SQLSMALLINT* stringLength);

        /**
         * Converts the given SQL statement into the system's native SQL grammar.
         */
        template<typename CHAR_TYPE>
        SQLRETURN nativeSQLT(CHAR_TYPE* inStatementText, SQLINTEGER textLength1,
            CHAR_TYPE* outStatementText, SQLINTEGER bufferLength,
            SQLINTEGER* textLength2Ptr);

      public:

        static SQLRETURN newConnection(GFXDEnvironment *env,
            GFXDConnection*& connRef);

        static SQLRETURN freeConnection(GFXDConnection* conn);

        /**
         * Get the GFXDEnvironment for this connection.
         */
        inline GFXDEnvironment* getEnvironment() {
          return m_env;
        }

        /**
         * Establish a new connection to the GemFireXD system. The network server
         * location is specified in the passed "server" and "port" arguments,
         * while the other connection properties (including user/password,
         *   if any) is passed in the "connProps" property bag.
         */
        SQLRETURN connect(const std::string& server, const int port,
            const Properties& connProps, SQLCHAR* outConnStr,
            const SQLINTEGER outConnStrLen, SQLSMALLINT* connStrLen);

        /**
         * Establish a new connection to the GemFireXD system. The network server
         * location is specified in the passed "server" and "port" arguments,
         * while the other connection properties (including user/password,
         *   if any) is passed in the "connProps" property bag.
         *
         * This is the wide-character string version for returning the
         * output connect string.
         */
        SQLRETURN connect(const std::string& server, const int port,
            const Properties& connProps, SQLWCHAR* outConnStr,
            const SQLINTEGER outConnStrLen, SQLSMALLINT* connStrLen);

        /**
         * Disconnect this connection.
         */
        SQLRETURN disconnect();

        /**
         * Return true if this connection is currently active.
         */
        inline bool isActive() {
          return m_conn.isOpen();
        }

        /**
         * Set an ODBC connection attribute.
         */
        SQLRETURN setAttribute(SQLINTEGER attribute, SQLPOINTER value,
            SQLINTEGER stringLength, bool isAscii);

        /**
         * Get the value of an ODBC connection attribute set previously
         * using {@link #setAttribute}.
         */
        SQLRETURN getAttribute(SQLINTEGER attribute, SQLPOINTER value,
            SQLINTEGER bufferLength, SQLINTEGER* stringLengthPtr);

        /**
         * Get the value of an ODBC connection attribute set previously
         * using {@link #setAttribute}.
         *
         * This is the wide-string version.
         */
        SQLRETURN getAttributeW(SQLINTEGER attribute, SQLPOINTER value,
            SQLINTEGER bufferLength, SQLINTEGER* stringLengthPtr);

        /**
         * Returns more information about the connection as in ODBC SQLGetInfo.
         */
        SQLRETURN getInfo(SQLUSMALLINT infoType, SQLPOINTER infoValue,
            SQLSMALLINT bufferLength, SQLSMALLINT* stringLength);

        /**
         * Returns more information about the connection as in ODBC SQLGetInfo.
         */
        SQLRETURN getInfoW(SQLUSMALLINT infoType, SQLPOINTER infoValue,
            SQLSMALLINT bufferLength, SQLSMALLINT* stringLength);

        /**
         * Converts the given SQL statement into the system's native SQL grammar.
         */
        SQLRETURN nativeSQL(SQLCHAR* inStatementText, SQLINTEGER textLength1,
            SQLCHAR* outStatementText, SQLINTEGER bufferLength,
            SQLINTEGER* textLength2Ptr);

        /**
         * Converts the given SQL statement into the system's native SQL grammar.
         * This is the wide-char version.
         */
        SQLRETURN nativeSQLW(SQLWCHAR* inStatementText, SQLINTEGER textLength1,
            SQLWCHAR* outStatementText, SQLINTEGER bufferLength,
            SQLINTEGER* textLength2Ptr);

        /**
         * Commit the current active transaction.
         */
        SQLRETURN commit();

        /**
         * Rollback the current active transaction.
         */
        SQLRETURN rollback();
      };

    }
  }
}

namespace
{
  SQLRETURN gfxdCommit(GFXDConnection* conn) {
    return conn->commit();
  }

  SQLRETURN gfxdRollback(GFXDConnection* conn) {
    return conn->rollback();
  }
}

#endif /* GFXDCONNECTION_H_ */
