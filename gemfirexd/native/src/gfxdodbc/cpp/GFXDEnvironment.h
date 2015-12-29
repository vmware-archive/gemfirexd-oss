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
 * GFXDEnvironment.h
 *
 * Holds common execution environment and utilities.
 *
 *      Author: swale
 */

#ifndef GFXDENVIRONMENT_H_
#define GFXDENVIRONMENT_H_

#include <Connection.h>

#include "DriverBase.h"
#include "SyncLock.h"

namespace com
{
  namespace pivotal
  {
    namespace gemfirexd
    {

      class GFXDConnection;

      /**
       * Encapsulates the current execution context including the connections,
       * the application ODBC version etc.
       */
      class GFXDEnvironment : public GFXDHandleBase
      {
      private:
        // TODO: implement the shared/non-shared environments
        const bool m_isShared;
        /** the list of all connections registered in this environment */
        std::vector<GFXDConnection*> m_connections;
        /** the lock for {@link #m_connections} list */
        native::SyncLock m_connLock;

        /** the list of all environment handles allocated for this app*/
        static std::vector<GFXDEnvironment*> m_envHandles;

        /**
         * if set to true then the driver will do mappings as required for an
         * ODBC 2.x application; see
         * http://msdn.microsoft.com/en-us/library/windows/desktop/ms714001.aspx
         */
        bool m_appIsVersion2x;

        // couple of template error handling methods used by SQLCHAR/SQLWCHAR
        // variants of ODBC error retrieval methods

        template<typename CHAR_TYPE>
        static SQLRETURN handleError_(const SQLException& err,
            SQLSMALLINT callNumber, SQLSMALLINT recNumber, CHAR_TYPE* sqlState,
            SQLINTEGER* nativeError, CHAR_TYPE* messageText,
            SQLSMALLINT bufferLength, SQLSMALLINT* textLength);

        template<typename CHAR_TYPE>
        static SQLRETURN handleErrorT(SQLSMALLINT handleType, SQLHANDLE handle,
            SQLSMALLINT recNumber, CHAR_TYPE* sqlState, SQLINTEGER* nativeError,
            CHAR_TYPE* messageText, SQLSMALLINT bufferLength,
            SQLSMALLINT* textLength);

        static SQLRETURN handleDiagFieldT(SQLSMALLINT handleType,
            SQLHANDLE handle, SQLSMALLINT recNumber, SQLSMALLINT diagId,
            SQLPOINTER diagInfo, SQLSMALLINT bufferLength,
            SQLSMALLINT* stringLength);

        /** register a new connection in this environment */
        void addNewActiveConnection(GFXDConnection* conn);

        /** gets the active connections for this ENV */
        int getActiveConnectionsCount();

        /**
         * Remove an existing connection from this environment;
         *
         * @return SQL_SUCCESS if connection was found and removed,
         * SQL_NO_DATA if not found and SQL_ERROR in case of some error
         */
        SQLRETURN removeActiveConnection(GFXDConnection* conn);

        friend class GFXDConnection;

      public:

        /**
         * Constructor for GFXDEnvironment to created shared or
         * non-shared environments.
         */
        inline GFXDEnvironment(const bool shared) :
            m_isShared(shared), m_connections(), m_connLock(),
            m_appIsVersion2x(false) {
        }

        /**
         * Returns true if this is a shared environment.
         */
        inline bool isShared() {
          return m_isShared;
        }

        /**
         * Returns true if the application is an ODBC 2.x one.
         *
         * TODO: implement the behaviour differences as noted in the link
         * of the docs in {@link #m_appIsVersion2x}.
         */
        inline bool isApplicationVersion2() {
          return m_appIsVersion2x;
        }

        /**
         * Execute a given function for each active connection in this
         * environment.
         */
        SQLRETURN forEachActiveConnection(
            SQLRETURN (*connOperation)(GFXDConnection*));

        /**
         * Set an ODBC environment attribute.
         */
        SQLRETURN setAttribute(SQLINTEGER attribute, SQLPOINTER value,
            SQLINTEGER stringLength);

        /**
         * Get the value of an ODBC environment attribute set previously
         * using {@link #setAttribute}.
         */
        SQLRETURN getAttribute(SQLINTEGER attribute, SQLPOINTER value,
            SQLINTEGER bufferLength, SQLINTEGER* stringLengthPtr);

        /**
         * Allocate a new GFXDEnvironment.
         */
        static SQLRETURN newEnvironment(GFXDEnvironment*& envRef);

        /**
         * Free the given GFXDEnvironment.
         */
        static SQLRETURN freeEnvironment(GFXDEnvironment* env);

        static SQLRETURN handleError(SQLSMALLINT handleType, SQLHANDLE handle,
            SQLSMALLINT recNumber, SQLCHAR* sqlState, SQLINTEGER* nativeError,
            SQLCHAR* messageText, SQLSMALLINT bufferLength,
            SQLSMALLINT* textLength);

        static SQLRETURN handleError(SQLSMALLINT handleType, SQLHANDLE handle,
            SQLSMALLINT recNumber, SQLWCHAR* sqlState, SQLINTEGER* nativeError,
            SQLWCHAR* messageText, SQLSMALLINT bufferLength,
            SQLSMALLINT* textLength);

        static SQLRETURN handleDiagField(SQLSMALLINT handleType,
            SQLHANDLE handle, SQLSMALLINT recNumber, SQLSMALLINT diagId,
            SQLPOINTER diagInfo, SQLSMALLINT bufferLength,
            SQLSMALLINT* stringLength);

        static SQLRETURN handleDiagFieldW(SQLSMALLINT handleType,
            SQLHANDLE handle, SQLSMALLINT recNumber, SQLSMALLINT diagId,
            SQLPOINTER diagInfo, SQLSMALLINT bufferLength,
            SQLSMALLINT* stringLength);
      };

    } /* namespace gemfirexd */
  } /* namespace pivotal */
} /* namespace com */

#endif /* GFXDENVIRONMENT_H_ */
