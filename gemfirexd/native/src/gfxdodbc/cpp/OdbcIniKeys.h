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
 * OdbcIniKeys.h
 *
 *  Created on: Mar 20, 2013
 *      Author: shankarh
 */

#ifndef ODBCINIKEYS_H_
#define ODBCINIKEYS_H_

#include <Connection.h>

#include "DriverBase.h"
#include "GFXDDefaults.h"

#include <set>

namespace com {
  namespace pivotal {
    namespace gemfirexd {

      /** odbc.ini keys and mappings to corresponding JDBC property names */
      class OdbcIniKeys {
      public:
        typedef hash_map<std::string, const client::ConnectionProperty*>::type
            KeyMap;

      private:
        OdbcIniKeys(); // no instance allowed

        /**
         * Map of ODBC key names to {@link ConnectionProperty}
         * attributes. Most of these names are interpreted by the underlying
         * native Connection API, while few are interpreted at the ODBC
         * layer (like "server").
         */
        static KeyMap s_keyMap;

        static const client::ConnectionProperty* getIniKeyMappingAndCheck(
            const std::string& connProperty,
            std::set<std::string>& allConnProps);

      public:
        /** the driver property name */
        static const std::string DRIVER;
        /** the name identifying this driver */
        static const std::string DRIVERNAME;
        /** the DSN attribute; not in INI file */
        static const std::string DSN;
        /** the database name which is constant for GemFireXD */
        static const std::string DBNAME;
        /** the server host name/address attribute */
        static const std::string SERVER;
        /** the server port attribute */
        static const std::string PORT;
        /** the user attribute */
        static const std::string USER;
        /** the username alias for user */
        static const std::string USERNAME;
        /** the UID alias for user */
        static const std::string UID;
        /** the password attribute */
        static const std::string PASSWORD;
        /** the pwd alias for password */
        static const std::string PWD;
        /** the load-balance attribute */
        static const std::string LOAD_BALANCE;
        /** the secondary-locators attribute */
        static const std::string SECONDARY_LOCATORS;
        /** the read-timeout attribute */
        static const std::string READ_TIMEOUT;
        /** the keepalive-idle attribute */
        static const std::string KEEPALIVE_IDLE;
        /** the keepalive-interval attribute */
        static const std::string KEEPALIVE_INTVL;
        /** the keepalive-count attribute */
        static const std::string KEEPALIVE_CNT;
        /** the sh-enabled attribute */
        static const std::string SINGLE_HOP_ENABLED;
        /** the sh-conn-pool-size attribute */
        static const std::string SINGLE_HOP_MAX_CONNECTIONS;
        /** the streaming disabled attribute */
        static const std::string DISABLE_STREAMING;
        /** the skip-listeners attribute */
        static const std::string SKIP_LISTENERS;
        /** the skip-constraint-checks attribute */
        static const std::string SKIP_CONSTRAINT_CHECKS;
        // TODO: combine LOG_FILE and TRACE_FILE into a single property
        /** attribute to specify the log file */
        static const std::string LOG_FILE;
        /** attribute to specify the trace file */
        static const std::string TRACE_FILE;
        /** attribute to specify the directory for log/trace file */
        static const std::string LOG_DIR;
        /** attribute to specify the logging trace level */
        static const std::string TRACE_LEVEL;
        /** attribute to specify if logging should append to existing file */
        static const std::string LOG_APPEND;
        /** attribute to specify the security mechanism to be used */
        static const std::string SECURITY_MECHANISM;
        /**
         * attribute to specify whether exception messages should be
         * retrived from the server separately
         */
        static const std::string RETRIEVE_MESSAGE_TEXT;
        /** attribute to enable SSL connections */
        static const std::string SSL_MODE;
        /** attribute to specify the SSL keystore path (currently only gkr) */
        static const std::string SSL_KEYSTORE;
        /** attribute to specify the SSL keystore password */
        static const std::string SSL_KEYSTORE_PWD;
        /** attribute to specify the SSL truststore path (currently only gkr) */
        static const std::string SSL_TRUSTSTORE;
        /** attribute to specify the SSL truststore password */
        static const std::string SSL_TRUSTSTORE_PWD;
        /**
         * attribute to specify for 2nd phase commit to complete on all nodes
         * instead of returning as soon as current server's 2nd phase commit
         * is done
         */
        static const std::string TX_SYNC_COMMITS;
        /** the disable-tx-batching attribute */
        static const std::string DISABLE_TX_BATCHING;
        /** log file path to which the initialization nano time is appended */
        static const std::string LOG_FILE_STAMP;

        /**
         * A connection level property. If set to true, data in HDFS can
         * be queried.Otherwise, only in-memory data is queried.
         */
        static const std::string QUERY_HDFS;

        /**
         * A connection level property. If true, then Statement.cancel()
         * through thin driver will not be supported.
         */
        static const std::string DISABLE_THINCLIENT_CANCEL;

        /** contains the list of all the properties above */
        static const std::string* ALL_PROPERTIES;
        static int NUM_ALL_PROPERTIES;

        // the JDBC property names are in
        // com.pivotal.gemfirexd.client.ClientAttribute

        /** initialize this class before use */
        static SQLRETURN init();

        /**
         * Return the native connection property name for given ODBC key name.
         * Also returns whether the flags for the provided property. The return
         * boolean is true when the name was found and false otherwise.
         */
        static bool getConnPropertyName(const std::string& odbcPropName,
            std::string& returnConnPropName, int& returnFlags);

        inline static const KeyMap& getKeyMap() {
          return s_keyMap;
        }
      };

    }
  }
}

#endif /* ODBCINIKEYS_H_ */
