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
 * ConnStringPropertyReader.h
 *
 *  Created on: Mar 20, 2013
 *      Author: shankarh
 */

#ifndef CONNSTRINGPROPERTYREADER_H_
#define CONNSTRINGPROPERTYREADER_H_

#include <common/SystemProperties.h>

#include "IniPropertyReader.h"
#include "OdbcIniKeys.h"

namespace com
{
  namespace pivotal
  {
    namespace gemfirexd
    {
      namespace impl
      {
        template<typename CHAR_TYPE>
        class ConnStringPropertyReader : public PropertyReader<CHAR_TYPE>
        {
        private:
          const CHAR_TYPE* m_connStr;
          const CHAR_TYPE* m_connStrp;
          bool m_newConnStr;
          /** for DSN attribute in the connection string */
          IniPropertyReader<CHAR_TYPE> m_iniReader;
          /** property names for {@link #m_iniReader} */
          ArrayIterator<std::string> *m_iniAllPropNames;
          /** set to true when properties using DSN are being read currently */
          bool m_readingDSN;

        public:
          inline ConnStringPropertyReader() :
              m_connStr(NULL), m_connStrp(NULL), m_newConnStr(false),
              m_iniReader(), m_iniAllPropNames(NULL), m_readingDSN(false) {
          }

          virtual ~ConnStringPropertyReader() {
            if (m_newConnStr && m_connStr != NULL) {
              delete[] m_connStr;
            }
          }

        public:
          virtual void init(const CHAR_TYPE* connStr, SQLINTEGER connStrLen,
              void* propData) {
            if (connStrLen == SQL_NTS || connStr[connStrLen] == 0) {
              m_connStr = connStr;
              m_newConnStr = false;
            } else {
              CHAR_TYPE* newConnStr = new CHAR_TYPE[connStrLen + 1];
              ::memcpy(newConnStr, connStr, sizeof(CHAR_TYPE) * connStrLen);
              newConnStr[connStrLen] = 0;
              m_connStr = newConnStr;
              m_newConnStr = true;
            }
            m_connStrp = m_connStr;
            m_iniAllPropNames = (ArrayIterator<std::string>*)propData;
          }

          virtual SQLRETURN read(std::string& outPropName,
              std::string& outPropValue, GFXDHandleBase* handle) {
            // if reading DSN then return values using IniPropertyReader
            if (m_readingDSN) {
              SQLRETURN ret = m_iniReader.read(outPropName, outPropValue,
                  handle);
              // check if all DSN properties have been read
              if (ret != SQL_SUCCESS) {
                if (ret != SQL_SUCCESS_WITH_INFO) {
                  m_readingDSN = false;
                }
                return ret;
              }
              // continue to remaining values in the connection string
              m_readingDSN = false;
            }
            // split the string assuming it to be of the form
            // <key1>=<value1>;<key2>=<value2>...
            if (m_connStrp != NULL
                && StringFunctions::strlen(m_connStrp) != 0) {
              const CHAR_TYPE* semicolonPos = StringFunctions::strchr(
                  m_connStrp, ';');
              const CHAR_TYPE* equalPos = StringFunctions::strchr(m_connStrp,
                  '=');
              SQLINTEGER outPropLen;

              if (equalPos != NULL) {
                if (sizeof(CHAR_TYPE) == 1) {
                  // trim spaces
                  const CHAR_TYPE* connStr = StringFunctions::ltrim(m_connStrp);
                  equalPos = StringFunctions::rtrim(equalPos);
                  outPropName.assign((const char*)connStr, equalPos - connStr);
                } else {
                  // trim spaces
                  const CHAR_TYPE* connStr = StringFunctions::ltrim(m_connStrp);
                  equalPos = StringFunctions::rtrim(equalPos);
                  // convert to UTF-8 for wide-character
                  SQLCHAR cname[256];
                  // not checking for result value since =256 already means
                  // an invalid property name that will be rejected by
                  // higher level in any case
                  StringFunctions::copyString(connStr, equalPos - connStr,
                      cname, 256, NULL);
                  outPropName.assign((const char*)cname);
                }
              } else {
                // should be handled by DriverManager
                std::string connStr;
                StringFunctions::getString(m_connStr, SQL_NTS, connStr);
                handle->setException(GET_SQLEXCEPTION(
                    SQLState::INVALID_CONNECTION_PROPERTY_VALUE,
                    SQLStateMessage::INVALID_CONNECTION_PROPERTY_VALUE_MSG
                        .format(connStr.c_str(), "")));
                return SQL_ERROR;
              }
              equalPos++;
              if (semicolonPos != NULL) {
                outPropLen = semicolonPos - equalPos;
                m_connStrp = semicolonPos + 1;
              } else {
                outPropLen = StringFunctions::strlen(equalPos);
                m_connStrp = NULL;
              }
              // copy the string after '=' (converting into UTF-8 for wchar)
              StringFunctions::getString(equalPos, outPropLen, outPropValue);

              // check for DSN attribute
              if (StringFunctions::equalsIgnoreCase(outPropName,
                  OdbcIniKeys::DSN)) {
                m_iniReader.init(equalPos, outPropLen, m_iniAllPropNames);
                // call self again with m_readingDSN as true
                m_readingDSN = true;
                return read(outPropName, outPropValue, handle);
              }

              // indicates that more results are available
              return SQL_SUCCESS_WITH_INFO;
            }
            // indicates that no more results are available
            return SQL_SUCCESS;
          }
        };

        template<typename CHAR_TYPE>
        static SQLRETURN readProperties(PropertyReader<CHAR_TYPE>* reader,
            const CHAR_TYPE* inputRef, SQLINTEGER inputLen, void* propData,
            const CHAR_TYPE* userName, SQLINTEGER userNameLen,
            const CHAR_TYPE* password, SQLINTEGER passwordLen,
            std::string& outServer, int& outPort, Properties& connProps,
            GFXDHandleBase* handle) {
          std::string propName, connPropName;
          std::string user, passwd;
          std::string propValue;
          int flags;
          SQLRETURN result = SQL_SUCCESS;
          SQLRETURN ret;

          // initialize user name and password attributes from passed ones
          if (userName != NULL) {
            StringFunctions::getString(userName, userNameLen, user);
          }
          if (password != NULL) {
            StringFunctions::getString(password, passwordLen, passwd);
          }
          // initialize the property reader
          reader->init(inputRef, inputLen, propData);
          // read the property names in a loop
          while ((ret = reader->read(propName, propValue, handle))
              == SQL_SUCCESS_WITH_INFO) {
            // lookup the mapping for this property name
            if (!OdbcIniKeys::getConnPropertyName(propName, connPropName,
                flags)) {
              // need to resort to case-insensitive matching against all
              // known names also support property names containing "-"
              StringFunctions::replace(propName, "-", "");
              const OdbcIniKeys::KeyMap& keyMap = OdbcIniKeys::getKeyMap();
              bool foundProp = false;
              for (OdbcIniKeys::KeyMap::const_iterator iter = keyMap.begin();
                  iter != keyMap.end(); ++iter) {
                if (StringFunctions::equalsIgnoreCase(propName, iter->first)) {
                  connPropName.assign(iter->second->getPropertyName());
                  flags = iter->second->getFlags();
                  foundProp = true;
                  break;
                }
              }
              if (!foundProp) {
                handle->setException(GET_SQLEXCEPTION2(
                    SQLStateMessage::INVALID_CONNECTION_PROPERTY_MSG,
                    propName.c_str()));
                result = SQL_SUCCESS_WITH_INFO;
                continue;
              }
            }
            // first check the "Driver" attribute
            if ((flags & ConnectionProperty::F_IS_DRIVER) != 0) {
              if (StringFunctions::equalsIgnoreCase(propValue,
                  OdbcIniKeys::DRIVERNAME) != 0) {
                // should have been handled by DriverManager
                handle->setException(
                    GET_SQLEXCEPTION2(SQLStateMessage::INVALID_DRIVER_NAME_MSG,
                        propValue.c_str(), OdbcIniKeys::DRIVERNAME.c_str()));
                return SQL_ERROR;
              }
            }
            // then check the "Server" attribute
            else if ((flags & ConnectionProperty::F_IS_SERVER) != 0) {
              outServer.assign(propValue);
            }
            // then the "Port" attribute
            else if ((flags & ConnectionProperty::F_IS_PORT) != 0) {
              char* endp = NULL;
              outPort = ::strtol(propValue.c_str(), &endp, 10);
              if (endp == NULL || *endp != 0) {
                handle->setException(GET_SQLEXCEPTION2(
                    SQLStateMessage::INVALID_CONNECTION_PROPERTY_VALUE_MSG,
                    propValue.c_str(), propName.c_str()));
                return SQL_ERROR;
              }
            }
            // then the "User"/"UserName"/"UID" and "Password"/"PWD" attributes if
            // nothing has been passed
            else if ((flags & ConnectionProperty::F_IS_USER) != 0) {
              if (user.size() == 0) {
                if (propValue.size() == 0) {
                  return GFXDHandleBase::errorInvalidBufferLength(0,
                      "UserName length", handle);
                }
                user.assign(propValue);
              }
            } else if ((flags & ConnectionProperty::F_IS_PASSWD) != 0) {
              if (passwd.size() == 0) {
                if (propValue.size() == 0) {
                  return GFXDHandleBase::errorInvalidBufferLength(0,
                      "Password length", handle);
                }
                passwd.assign(propValue);
              }
            }
            // next the remaining connection properties
            else if ((flags & ConnectionProperty::F_IS_SYSTEM_PROP) == 0) {
              connProps[connPropName] = propValue;
            }
            // lastly the system properties shared across all envs etc
            else {
              SystemProperties::setProperty(connPropName, propValue);
            }
          } // end while

          if (ret != SQL_SUCCESS) {
            return ret;
          }
          if (outServer.size() == 0) {
            outServer.assign(GFXDDefaults::DEFAULT_SERVER);
          }
          if (outPort <= 0) {
            outPort = GFXDDefaults::DEFAULT_SERVER_PORT;
          }
          if (user.size() > 0) {
            connProps[OdbcIniKeys::USER] = user;
          }
          if (passwd.size() > 0) {
            connProps[OdbcIniKeys::PASSWORD] = passwd;
          }
          return result;
        }
      }
    }
  }
}

#endif /* CONNSTRINGPROPERTYREADER_H_ */
