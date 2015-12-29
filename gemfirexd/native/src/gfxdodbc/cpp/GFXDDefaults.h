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
 * GFXDDefines.h
 *
 *  Created on: Mar 20, 2013
 *      Author: shankarh
 */

#ifndef GFXDDEFAULTS_H_
#define GFXDDEFAULTS_H_

#include "DriverBase.h"

////////////////////////////////////////////////////////////////////////////////////////////////////////
///     global defines
//////////////////////////////////////////////////////////////////////////////////////////////////////
using namespace com::pivotal::gemfirexd;

#define GFXD_DRIVERNAME "GemFireXD Network Client 1.0"

namespace com
{
  namespace pivotal
  {
    namespace gemfirexd
    {

      class GFXDGlobals
      {
      public:
        static bool g_initialized;
        static bool g_loggingEnabled;
      };

      class GFXDDefaults
      {
      public:
        static const SQLCHAR DEFAULT_DSN[];
        static const SQLWCHAR* DEFAULT_DSNW;
        static const char* ODBC_INI;
        static const char* ODBCINST_INI;

        static const char DEFAULT_SERVER[];
        static const int DEFAULT_SERVER_PORT;

        inline static const char* getDefaultServer() {
          return GFXDDefaults::DEFAULT_SERVER;
        }
      };

      /** typedef for SQLWCHAR strings like std::string */
      typedef std::basic_string<SQLWCHAR> sqlwstring;

      /** typedef for C string property map */
      typedef std::map<std::string, std::string> Properties;

      /**
       * Union for the different types of attributes in SQLSet*Attr methods.
       */
      struct AttributeValue
      {
        bool m_ascii;
        union
        {
          SQLUINTEGER m_intv;
          SQLULEN m_lenv;
          SQLPOINTER m_refv;
        } m_val;

        inline AttributeValue() :
            m_ascii(true) {
        }

        inline AttributeValue(bool isAscii) :
            m_ascii(isAscii) {
        }
      };

      /***
       * Typedef for the map holding currently defined attribute name/values.
       */
      typedef hash_map<SQLINTEGER, AttributeValue>::type AttributeMap;

      /**
       * Function definition for SQLDriverToDataSource.
       */
      typedef BOOL (*DriverToDataSource)(UDWORD fOption, SWORD fSqlType,
          PTR rgbValueIn, SDWORD cbValueIn, PTR rgbValueOut,
          SDWORD cbValueOutMax, SDWORD * pcbValueOut, UCHAR * szErrorMsg,
          SWORD cbErrorMsgMax, SWORD * pcbErrorMsg);

      /**
       * Function definition for SQLDataSourceToDriver.
       */
      typedef BOOL (*DataSourceToDriver)(UDWORD fOption, SWORD fSqlType,
          PTR rgbValueIn, SDWORD cbValueIn, PTR rgbValueOut,
          SDWORD cbValueOutMax, SDWORD * pcbValueOut, UCHAR * szErrorMsg,
          SWORD cbErrorMsgMax, SWORD * pcbErrorMsg);

      /* Check if a paramater is a data-at-exec paramter.*/
      #define IS_DATA_AT_EXEC(X)((X) && \
                                (*(X) == SQL_DATA_AT_EXEC || \
                                *(X) <= SQL_LEN_DATA_AT_EXEC_OFFSET))

      /*MACRO for sync lock*/
      #define SYNC_LOCK(x)  native::SyncLock::Guard lock(x);\
                       lock.acquire();

    }
  }
}

#endif /* GFXDDEFAULTS_H_ */
