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
 * ClientService.h
 *
 *      Author: swale
 */

#ifndef CLIENTSERVICE_H_
#define CLIENTSERVICE_H_

#include "ClientBaseImpl.h"

#include <boost/shared_ptr.hpp>
#include <boost/thread/mutex.hpp>

#include "../thrift/GFXDService.h"

using namespace apache::thrift;

namespace apache {
  namespace thrift {
    namespace transport {
      class TSocket;
    }
    namespace protocol {
      class TProtocol;
    }
  }
}

namespace com {
  namespace pivotal {
    namespace gemfirexd {
      namespace client {
        namespace impl {

          class BufferedSocketTransport;
          class ControlConnection;
          class ClientServiceHolder;

          class GFXDClient : public thrift::GFXDServiceClient {
          public:
            GFXDClient(protocol::TProtocol* prot) :
                thrift::GFXDServiceClient(
                    boost::shared_ptr < protocol::TProtocol > (prot)) {
            }

          private:
            void resetProtocols(
                const boost::shared_ptr<protocol::TProtocol>& iprot,
                const boost::shared_ptr<protocol::TProtocol>& oprot) {
              piprot_ = iprot;
              poprot_ = oprot;
              iprot_ = iprot.get();
              oprot_ = oprot.get();
            }

            friend class ClientService;
          };

          class ClientService {
          private:
            const thrift::OpenConnectionArgs m_connArgs;
            bool m_loadBalance;
            thrift::ServerType::type m_reqdServerType;
            //const SSLSocketParameters m_sslParams;
            std::set<std::string> m_serverGroups;

            boost::shared_ptr<BufferedSocketTransport> m_transport;
            GFXDClient m_client;
            uint32_t m_serviceId;

            thrift::HostAddress m_currentHostAddr;
            std::vector<thrift::HostAddress> m_connHosts;
            int32_t m_connId;
            std::string m_token;
            bool m_isOpen;

            std::map<TransactionAttribute::type, bool> m_pendingTXAttrs;
            bool m_hasPendingTXAttrs;
            IsolationLevel::type m_isolationLevel;
            std::map<TransactionAttribute::type, bool> m_currentTXAttrs;

            // no copy constructor or assignment operator due to obvious issues
            // with usage of same connection by multiple threads concurrently
            ClientService(const ClientService&);
            ClientService& operator=(const ClientService&);

            void clearPendingTransactionAttrs();

            static thrift::OpenConnectionArgs& initConnectionArgs(
                thrift::OpenConnectionArgs& connArgs);

            static protocol::TProtocol* createDummyProtocol();

            static protocol::TProtocol* createProtocol(
                thrift::HostAddress& hostAddr,
                const thrift::ServerType::type serverType,
                //const SSLSocketParameters& sslParams,
                boost::shared_ptr<BufferedSocketTransport>& returnTransport);

            friend class ClientServiceHolder;

          protected:
            virtual void handleGFXDException(const thrift::GFXDException& sqle);

            virtual void handleStdException(const char* op,
                const std::exception& stde);

            virtual void handleTTransportException(const char* op,
                const transport::TTransportException& tte);

            virtual void handleTProtocolException(const char* op,
                const protocol::TProtocolException& tpe);

            virtual void handleTException(const char* op, const TException& te);

            virtual void handleUnknownException(const char* op);

            void throwSQLExceptionForNodeFailure(const char* op,
                const std::exception& se);

            void openConnection(thrift::HostAddress& hostAddr,
                std::set<thrift::HostAddress>& failedServers);

            void flushPendingTransactionAttrs();

            void setPendingTransactionAttrs(thrift::StatementAttrs& stmtAttrs);

            void destroyTransport() throw ();

          private:
            // the static hostName and hostId used by all connections
            static std::string s_hostName;
            static std::string s_hostId;
            static boost::mutex s_globalLock;

          public:
            ClientService(const std::string& host, const int port,
                thrift::OpenConnectionArgs& arguments, void** outServiceId);

            virtual ~ClientService() throw ();

            static void staticInitialize(
                std::map<std::string, std::string>& props);

            static thrift::ServerType::type getServerType(bool isServer,
                bool useBinaryProtocol, bool useSSL);

            inline uint32_t getServiceId() const throw () {
              return m_serviceId;
            }

            inline bool isOpen() const throw () {
              return m_isOpen;
            }

            inline boost::shared_ptr<BufferedSocketTransport>& getTransport() {
              return m_transport;
            }

            const char* getTokenStr() const throw () {
              return m_token.empty() ? NULL : m_token.c_str();
            }

            IsolationLevel::type getCurrentIsolationLevel() const throw () {
              return m_isolationLevel;
            }

            void execute(thrift::StatementResult& _return,
                const std::string& sql,
                const std::map<int32_t, thrift::OutputParameter>& outputParams,
                const thrift::StatementAttrs& attrs);

            void executeUpdate(thrift::UpdateResult& _return,
                const std::vector<std::string>& sqls,
                const thrift::StatementAttrs& attrs);

            void executeQuery(thrift::RowSet& _return, const std::string& sql,
                const thrift::StatementAttrs& attrs);

            void prepareStatement(thrift::PrepareResult& _return,
                const std::string& sql,
                const std::map<int32_t, thrift::OutputParameter>& outputParams,
                const thrift::StatementAttrs& attrs);

            void executePrepared(thrift::StatementResult& _return,
                thrift::PrepareResult& prepResult, const thrift::Row& params,
                const std::map<int32_t, thrift::OutputParameter>& outputParams);

            void executePreparedUpdate(thrift::UpdateResult& _return,
                thrift::PrepareResult& prepResult, const thrift::Row& params);

            void executePreparedQuery(thrift::RowSet& _return,
                thrift::PrepareResult& prepResult, const thrift::Row& params);

            void executePreparedBatch(thrift::UpdateResult& _return,
                thrift::PrepareResult& prepResult,
                const std::vector<thrift::Row>& paramsBatch);

            void prepareAndExecute(thrift::StatementResult& _return,
                const std::string& sql,
                const std::vector<thrift::Row>& paramsBatch,
                const std::map<int32_t, thrift::OutputParameter>& outputParams,
                const thrift::StatementAttrs& attrs);

            void getNextResultSet(thrift::RowSet& _return,
                const int32_t cursorId, const int8_t otherResultSetBehaviour);

            void getBlobChunk(thrift::BlobChunk& _return, const int32_t lobId,
                const int64_t offset, const int32_t size,
                const bool freeLobAtEnd);

            void getClobChunk(thrift::ClobChunk& _return, const int32_t lobId,
                const int64_t offset, const int32_t size,
                const bool freeLobAtEnd);

            int32_t sendBlobChunk(thrift::BlobChunk& chunk);

            int32_t sendClobChunk(thrift::ClobChunk& chunk);

            void freeLob(const int32_t lobId);

            void scrollCursor(thrift::RowSet& _return, const int32_t cursorId,
                const int32_t offset, const bool offsetIsAbsolute,
                const bool fetchReverse, const int32_t fetchSize);

            void executeCursorUpdate(const int32_t cursorId,
                const int8_t operation, const thrift::Row& changedRow,
                const std::vector<int32_t>& changedColumns,
                const int32_t changedRowIndex);

            void executeBatchCursorUpdate(const int32_t cursorId,
                const std::vector<int8_t>& operations,
                const std::vector<thrift::Row>& changedRows,
                const std::vector<std::vector<int32_t> >& changedColumnsList,
                const std::vector<int32_t>& changedRowIndexes);

            void beginTransaction(const IsolationLevel::type isolationLevel);

            void setTransactionAttribute(const TransactionAttribute::type flag,
                bool isTrue);

            bool getTransactionAttribute(const TransactionAttribute::type flag);

            void getTransactionAttributes(
                std::map<TransactionAttribute::type, bool>& _return);

            void commitTransaction(const bool startNewTransaction);

            void rollbackTransaction(const bool startNewTransaction);

            bool prepareCommitTransaction();

            void fetchActiveConnections(
                std::vector<thrift::ConnectionProperties>& _return);

            void fetchActiveStatements(std::map<int32_t, std::string>& _return);

            void getServiceMetaData(thrift::ServiceMetaData& _return);

            void getSchemaMetaData(thrift::RowSet& _return,
                const thrift::ServiceMetaDataCall::type schemaCall,
                thrift::ServiceMetaDataArgs& metadataArgs);

            void getIndexInfo(thrift::RowSet& _return,
                thrift::ServiceMetaDataArgs& metadataArgs, const bool unique,
                const bool approximate);

            void getUDTs(thrift::RowSet& _return,
                thrift::ServiceMetaDataArgs& metadataArgs,
                const std::vector<thrift::GFXDType::type>& types);

            void getBestRowIdentifier(thrift::RowSet& _return,
                thrift::ServiceMetaDataArgs& metadataArgs, const int32_t scope,
                const bool nullable);

            void closeResultSet(const int32_t cursorId);

            void cancelStatement(const int32_t stmtId);

            void closeStatement(const int32_t stmtId);

            void close();
          };

          /**
           * This is a global registry for ClientService. The primary reason
           * for having this is to avoid having to refer to boost::shared_ptr
           * in Connection and other public classes. Instead the reference
           * count is maintained here and removed from list once it goes
           * down to zero.
           */
          class ClientServiceHolder {
          private:
            typedef std::vector<boost::shared_ptr<ClientService> > service_list;
            typedef hash_map<long, service_list>::type services_map;

            services_map m_services;
            boost::mutex m_servicesLock;
            uint32_t m_numServices;

            ClientServiceHolder();

            /**
             * This is a small memory "leak" of single global object but we
             * will live with it to ensure that detached/non-joined threads
             * keep using valid service object even when main thread exists
             * and calls global destructors.
             */
            static ClientServiceHolder* s_instance;

          public:

            inline static ClientServiceHolder& instance() throw () {
              return *s_instance;
            }

            void* registerInstance(ClientService& service);

            void incrementReferenceCount(void* serviceId);

            void decrementReferenceCount(void* serviceId);

            boost::shared_ptr<ClientService> getService(const void* serviceId);
          };

        } /* namespace impl */
      } /* namespace client */
    } /* namespace gemfirexd */
  } /* namespace pivotal */
} /* namespace com */

#endif /* CLIENTSERVICE_H_ */
