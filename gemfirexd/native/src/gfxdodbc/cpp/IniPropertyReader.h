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
 * IniPropertyReader.h
 *
 *  Created on: Mar 20, 2013
 *      Author: shankarh
 */

#ifndef INIPROPERTYREADER_H_
#define INIPROPERTYREADER_H_

#include "PropertyReader.h"
#include "ArrayIterator.h"
#include "GFXDDefaults.h"
#include "StringFunctions.h"

extern "C"
{
#include <odbcinst.h>
}

namespace com
{
  namespace pivotal
  {
    namespace gemfirexd
    {
      namespace impl
      {

        template<typename CHAR_TYPE>
        class IniPropertyReader : public PropertyReader<CHAR_TYPE>
        {
        private:
          SQLCHAR* m_dsn;
          ArrayIterator<std::string>* m_propData;

        public:
          inline IniPropertyReader() :
              m_dsn(NULL), m_propData(NULL) {
          }

          inline ~IniPropertyReader() {
            if (m_dsn != NULL) {
              delete[] m_dsn;
            }
          }

          void init(const CHAR_TYPE* dsn, SQLINTEGER dsnLen, void* propData) {
            if (dsnLen == SQL_NTS) {
              dsnLen = StringFunctions::strlen(dsn);
            }
            const SQLINTEGER mdsnSize = dsnLen * ((sizeof(CHAR_TYPE) << 1) - 1)
                + 1;
            m_dsn = new SQLCHAR[mdsnSize];
            StringFunctions::copyString(dsn, dsnLen, m_dsn, mdsnSize, NULL);
            m_propData = (ArrayIterator<std::string>*)propData;
          }

          SQLRETURN read(std::string& outPropName, std::string& outPropValue,
              GFXDHandleBase* handle) {
            // void* is taken to be the iterator on all property names
            // (or those present in the ini file)
            ArrayIterator<std::string> &iter = *m_propData;
            if (iter.hasCurrent()) {
              char outProp[8192];
              int len;
              for (;;) {
                outPropName.assign(*iter);
                len = ::SQLGetPrivateProfileString((const char*)m_dsn,
                    outPropName.c_str(), "", outProp, 8192,
                    GFXDDefaults::ODBC_INI);
                if (len > 0) {
                  // got a property declared in ini file
                  outPropValue.assign(outProp, len);
                  ++iter;
                  // indicates that more results are available
                  return SQL_SUCCESS_WITH_INFO;
                }
                if (!++iter) {
                  // indicates that no more results are available
                  return SQL_SUCCESS;
                }
              }
            }
            // indicates that no more results are available
            return SQL_SUCCESS;
          }
        };
      }
    }
  }
}

#endif /* INIPROPERTYREADER_H_ */
