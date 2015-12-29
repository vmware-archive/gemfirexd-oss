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
 * OdbcIniKeys.cpp
 *
 *  Created on: Mar 20, 2013
 *      Author: shankarh
 */

#include <ClientAttribute.h>

#include "OdbcIniKeys.h"
#include "StringFunctions.h"

using namespace com::pivotal::gemfirexd;
using namespace com::pivotal::gemfirexd::client;

// initialize the const char* keys in odbc.ini
const std::string OdbcIniKeys::DRIVER = "Driver";
const std::string OdbcIniKeys::DRIVERNAME = GFXD_DRIVERNAME;
const std::string OdbcIniKeys::DSN = "DSN";
const std::string OdbcIniKeys::DBNAME = "GemFireXD";
const std::string OdbcIniKeys::SERVER = "Server";
const std::string OdbcIniKeys::PORT = "Port";
const std::string OdbcIniKeys::USER = "user";
const std::string OdbcIniKeys::USERNAME = "UserName";
const std::string OdbcIniKeys::UID = "UID";
const std::string OdbcIniKeys::PASSWORD = "password";
const std::string OdbcIniKeys::PWD = "PWD";
const std::string OdbcIniKeys::LOAD_BALANCE = "LoadBalance";
const std::string OdbcIniKeys::SECONDARY_LOCATORS = "SecondaryLocators";
const std::string OdbcIniKeys::READ_TIMEOUT = "ReadTimeout";
const std::string OdbcIniKeys::KEEPALIVE_IDLE = "KeepAliveIdle";
const std::string OdbcIniKeys::KEEPALIVE_INTVL = "KeepAliveInterval";
const std::string OdbcIniKeys::KEEPALIVE_CNT = "KeepAliveCount";
const std::string OdbcIniKeys::SINGLE_HOP_ENABLED = "SingleHopEnabled";
const std::string OdbcIniKeys::SINGLE_HOP_MAX_CONNECTIONS =
    "SingleHopMaxConnections";
const std::string OdbcIniKeys::DISABLE_STREAMING = "DisableStreaming";
const std::string OdbcIniKeys::SKIP_LISTENERS = "SkipListeners";
const std::string OdbcIniKeys::SKIP_CONSTRAINT_CHECKS = "SkipConstraintChecks";
const std::string OdbcIniKeys::LOG_FILE = "LogFile";
const std::string OdbcIniKeys::TRACE_FILE = "TraceFile";
const std::string OdbcIniKeys::LOG_DIR = "LogDir";
const std::string OdbcIniKeys::TRACE_LEVEL = "TraceLevel";
const std::string OdbcIniKeys::LOG_APPEND = "LogAppend";
const std::string OdbcIniKeys::SECURITY_MECHANISM = "SecurityMechanism";
const std::string OdbcIniKeys::RETRIEVE_MESSAGE_TEXT = "RetrieveMessageText";
const std::string OdbcIniKeys::SSL_MODE = "SSLMode";
const std::string OdbcIniKeys::SSL_KEYSTORE = "SSLKeyStore";
const std::string OdbcIniKeys::SSL_KEYSTORE_PWD = "SSLKeyStorePassword";
const std::string OdbcIniKeys::SSL_TRUSTSTORE = "SSLTrustStore";
const std::string OdbcIniKeys::SSL_TRUSTSTORE_PWD = "SSLTrustStorePassword";
const std::string OdbcIniKeys::TX_SYNC_COMMITS = "TxSyncCommits";
const std::string OdbcIniKeys::DISABLE_TX_BATCHING = "DisableTXBatching";
const std::string OdbcIniKeys::LOG_FILE_STAMP = "LogFileNS";
const std::string OdbcIniKeys::QUERY_HDFS = "QueryHDFS";
const std::string OdbcIniKeys::DISABLE_THINCLIENT_CANCEL = "disable-cancel";

const std::string* OdbcIniKeys::ALL_PROPERTIES = NULL;
int OdbcIniKeys::NUM_ALL_PROPERTIES = 0;
// population of the map is done in init()
OdbcIniKeys::KeyMap OdbcIniKeys::s_keyMap(46);

static const char* DRIVER_NAME_LIST[] = { GFXD_DRIVERNAME, NULL };

const ConnectionProperty* OdbcIniKeys::getIniKeyMappingAndCheck(
    const std::string& connProperty, std::set<std::string>& allConnProps) {
  const ConnectionProperty* prop = &ConnectionProperty::getProperty(
      connProperty);
  if (allConnProps.size() > 0 && allConnProps.erase(connProperty) == 0) {
    throw GET_SQLEXCEPTION2(SQLStateMessage::INVALID_CONNECTION_PROPERTY_MSG,
        connProperty.c_str());
  }
  return prop;
}

// initialization for OdbcIniKeys members
SQLRETURN OdbcIniKeys::init() {
  std::set<std::string> allConnProps =
      ConnectionProperty::getValidPropertyNames();
  std::string emptyProp;
  std::set<std::string> emptyConnProps;

  // populate the map
  try {
    // first three pointers essentially are a "memory-leak" but does not
    // matter since s_keyMap is a static map
    s_keyMap[DRIVER] = new ConnectionProperty(emptyProp,
        "Name of the GemFireXD Driver: " GFXD_DRIVERNAME, DRIVER_NAME_LIST,
        ConnectionProperty::F_IS_DRIVER);
    s_keyMap[SERVER] = new ConnectionProperty(emptyProp,
        "Hostname or IP address of the network server to connect to", NULL,
        ConnectionProperty::F_IS_SERVER | ConnectionProperty::F_IS_UTF8);
    s_keyMap[PORT] = new ConnectionProperty(emptyProp,
        "Port of the network server to connect to", NULL,
        ConnectionProperty::F_IS_PORT | ConnectionProperty::F_IS_UTF8);

    s_keyMap[USER] = getIniKeyMappingAndCheck(ClientAttribute::USERNAME,
        allConnProps);
    s_keyMap[USERNAME] = getIniKeyMappingAndCheck(ClientAttribute::USERNAME_ALT,
        allConnProps);
    // repeat name, so skip checking in the connProps map
    s_keyMap[UID] = getIniKeyMappingAndCheck(ClientAttribute::USERNAME,
        emptyConnProps);
    s_keyMap[PASSWORD] = getIniKeyMappingAndCheck(ClientAttribute::PASSWORD,
        allConnProps);
    // repeat name, so skip checking in the connProps map
    s_keyMap[PWD] = getIniKeyMappingAndCheck(ClientAttribute::PASSWORD,
        emptyConnProps);
    s_keyMap[LOAD_BALANCE] = getIniKeyMappingAndCheck(
        ClientAttribute::LOAD_BALANCE, allConnProps);
    s_keyMap[SECONDARY_LOCATORS] = getIniKeyMappingAndCheck(
        ClientAttribute::SECONDARY_LOCATORS, allConnProps);
    s_keyMap[READ_TIMEOUT] = getIniKeyMappingAndCheck(
        ClientAttribute::READ_TIMEOUT, allConnProps);
    s_keyMap[KEEPALIVE_IDLE] = getIniKeyMappingAndCheck(
        ClientAttribute::KEEPALIVE_IDLE, allConnProps);
    s_keyMap[KEEPALIVE_INTVL] = getIniKeyMappingAndCheck(
        ClientAttribute::KEEPALIVE_INTVL, allConnProps);
    s_keyMap[KEEPALIVE_CNT] = getIniKeyMappingAndCheck(
        ClientAttribute::KEEPALIVE_CNT, allConnProps);
    s_keyMap[SINGLE_HOP_ENABLED] = getIniKeyMappingAndCheck(
        ClientAttribute::SINGLE_HOP_ENABLED, allConnProps);
    s_keyMap[SINGLE_HOP_MAX_CONNECTIONS] = getIniKeyMappingAndCheck(
        ClientAttribute::SINGLE_HOP_MAX_CONNECTIONS, allConnProps);
    s_keyMap[DISABLE_STREAMING] = getIniKeyMappingAndCheck(
        ClientAttribute::DISABLE_STREAMING, allConnProps);
    s_keyMap[SKIP_LISTENERS] = getIniKeyMappingAndCheck(
        ClientAttribute::SKIP_LISTENERS, allConnProps);
    s_keyMap[SKIP_CONSTRAINT_CHECKS] = getIniKeyMappingAndCheck(
        ClientAttribute::SKIP_CONSTRAINT_CHECKS, allConnProps);
    s_keyMap[LOG_FILE] = getIniKeyMappingAndCheck(ClientAttribute::LOG_FILE,
        allConnProps);
    s_keyMap[TRACE_FILE] = getIniKeyMappingAndCheck(
        ClientAttribute::CLIENT_TRACE_FILE, allConnProps);
    s_keyMap[LOG_DIR] = getIniKeyMappingAndCheck(
        ClientAttribute::CLIENT_TRACE_DIRECTORY, allConnProps);
    s_keyMap[TRACE_LEVEL] = getIniKeyMappingAndCheck(
        ClientAttribute::CLIENT_TRACE_LEVEL, allConnProps);
    s_keyMap[LOG_APPEND] = getIniKeyMappingAndCheck(
        ClientAttribute::CLIENT_TRACE_APPEND, allConnProps);
    s_keyMap[SECURITY_MECHANISM] = getIniKeyMappingAndCheck(
        ClientAttribute::CLIENT_SECURITY_MECHANISM, allConnProps);
    s_keyMap[RETRIEVE_MESSAGE_TEXT] = getIniKeyMappingAndCheck(
        ClientAttribute::CLIENT_RETRIEVE_MESSAGE_TEXT, allConnProps);
    s_keyMap[SSL_MODE] = getIniKeyMappingAndCheck(ClientAttribute::SSL,
        allConnProps);
    s_keyMap[TX_SYNC_COMMITS] = getIniKeyMappingAndCheck(
        ClientAttribute::TX_SYNC_COMMITS, allConnProps);
    s_keyMap[DISABLE_TX_BATCHING] = getIniKeyMappingAndCheck(
        ClientAttribute::DISABLE_TX_BATCHING, allConnProps);
    s_keyMap[LOG_FILE_STAMP] = getIniKeyMappingAndCheck(
        ClientAttribute::LOG_FILE_STAMP, allConnProps);
    s_keyMap[QUERY_HDFS] = getIniKeyMappingAndCheck(ClientAttribute::QUERY_HDFS,
        allConnProps);
    s_keyMap[DISABLE_THINCLIENT_CANCEL] = getIniKeyMappingAndCheck(
        ClientAttribute::DISABLE_THINCLIENT_CANCEL, allConnProps);
  } catch (const SQLException& sqle) {
    GFXDHandleBase::setGlobalException(sqle);
    return SQL_ERROR;
  } catch (const std::exception& ex) {
    GFXDHandleBase::setGlobalException(__FILE__, __LINE__, ex);
    return SQL_ERROR;
  }

  if (allConnProps.size() > 0) {
    std::string reason("Unmapped property name(s) in initialization: ");
    for (std::set<std::string>::const_iterator iter = allConnProps.begin();
        iter != allConnProps.end(); ++iter) {
      reason.append("[").append(*iter).append("] ");
    }
    GFXDHandleBase::setGlobalException(
        GET_SQLEXCEPTION(SQLState::UNKNOWN_EXCEPTION, reason));
    return SQL_ERROR;
  }

  // now initialize the full property array using above map
  NUM_ALL_PROPERTIES = s_keyMap.size();
  std::string* allProps = new std::string[NUM_ALL_PROPERTIES];
  ALL_PROPERTIES = allProps;
  for (KeyMap::iterator it = s_keyMap.begin(); it != s_keyMap.end(); ++it) {
    *allProps++ = it->first;
  }
  return SQL_SUCCESS;
}

bool OdbcIniKeys::getConnPropertyName(const std::string& odbcPropName,
    std::string& returnConnPropName, int& returnFlags) {
  const KeyMap::const_iterator& search = s_keyMap.find(odbcPropName);
  if (search != s_keyMap.end()) {
    const ConnectionProperty* result = search->second;
    returnConnPropName = result->getPropertyName();
    returnFlags = result->getFlags();
    return true;
  } else {
    return false;
  }
}
