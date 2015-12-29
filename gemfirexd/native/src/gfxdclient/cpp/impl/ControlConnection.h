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
 *  Created on: 31 May 2014
 *      Author: swale
 */

#ifndef CONTROLCONNECTION_H_
#define CONTROLCONNECTION_H_

#include "ClientService.h"

#include <boost/thread/mutex.hpp>

#include "../thrift/LocatorService.h"

namespace com
{
  namespace pivotal
  {
    namespace gemfirexd
    {
      namespace client
      {
        namespace impl
        {

          /**
           * Holds locator, server information to use for failover. Also provides
           * convenience methods to actually search for an appropriate host for
           * failover.
           * <p>
           * One distributed system is supposed to have one ControlConnection.
           *
           * @author swale
           * @since gfxd 1.1
           */
          class ControlConnection
          {
          private:
            const thrift::ServerType m_gfxdServerType;
            //const SSLSocketParameters& m_sslParams;
            const std::set<thrift::ServerType> m_gfxdServerTypeSet;
            const std::vector<thrift::HostAddress>& m_locators;
            thrift::HostAddress m_controlHost;
            AutoPtr<thrift::LocatorServiceClient> m_controlLocator;
            const std::vector<thrift::HostAddress> m_controlHosts;
            const hash_set<thrift::HostAddress>::type m_controlHostSet;
            const std::set<std::string>& m_serverGroups;

            const boost::mutex m_lock;

            /**
             * Since one DS is supposed to have one ControlConnection, so we expect the
             * total size of this static global list to be small.
             */
            static const std::vector<AutoPtr<ControlConnection> > s_allConnections;
            /** Global lock for {@link allConnections} */
            static const boost::mutex s_allConnsLock;

            void failoverToAvailableHost(
                std::set<thrift::HostAddress>& failedServers,
                const std::exception* failure);

            void refreshAllHosts(
                const std::vector<thrift::HostAddress>& allHosts);

            const thrift::GFXDException& unexpectedError(
                const std::exception& e, const thrift::HostAddress& host);

            void failoverExhausted(const std::exception& cause,
                thrift::GFXDException& result);

          public:
            ControlConnection(const ClientService& service);

            static const AutoPtr<ControlConnection>& getOrCreateControlConnection(
                const thrift::HostAddress& hostAddr,
                const ClientService& service);

            const thrift::HostAddress& getPreferredServer(
                std::set<thrift::HostAddress>& failedServers, bool forFailover);
          };

        } /* namespace impl */
      } /* namespace client */
    } /* namespace gemfirexd */
  } /* namespace pivotal */
} /* namespace com */

#endif /* CONTROLCONNECTION_H_ */
