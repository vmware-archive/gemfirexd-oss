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
 *  Created on: 7 Jun 2014
 *      Author: swale
 */

#ifndef INTERNALLOGGER_H_
#define INTERNALLOGGER_H_

#include <boost/thread/thread.hpp>

#include "ThreadSafeMap.h"
#include "LogWriter.h"

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

          class InternalLogger
          {
          private:
            InternalLogger(); // no instance
            ~InternalLogger(); // no instance
            InternalLogger(const InternalLogger&); // no instance
            InternalLogger& operator=(const InternalLogger&); // no instance

            /**
             * the common map from thread ID to its name used by all LogWriters
             * to dump names of any new thread IDs in compact logging
             */
            static ThreadSafeMap<boost::thread::id, std::string> s_threadNames;

            static std::ostream& printCompact_(std::ostream& out,
                const LogLevel::type logLevel, const char* flag,
                const boost::thread::id tid);

            static void compactLogThreadName(std::ostream& out,
                const boost::thread::id tid);

            static void compactHeader(std::ostream& out,
                const boost::thread::id tid, const char* opId,
                const char* opSql, const int32_t sqlId, const bool isStart,
                const int64_t nanos, const int64_t milliTime,
                const int32_t connId, const std::string& token);

            friend class com::pivotal::gemfirexd::client::LogWriter;

          public:
            static void TRACE_COMPACT(const boost::thread::id tid,
                const char* opId, const char* opSql, const int32_t sqlId,
                const bool isStart, const int64_t nanos, const int32_t connId,
                const std::string& token, const std::exception* se = NULL,
                const int64_t milliTime = 0);

            static void TRACE_COMPACT(const boost::thread::id tid,
                const char* opId, const char* opSql, const int32_t sqlId,
                const bool isStart, const int64_t nanos, const int32_t connId,
                const std::string& token, const SQLException* sqle,
                const int64_t milliTime = 0);
          };

        } /* namespace impl */
      } /* namespace client */
    } /* namespace gemfirexd */
  } /* namespace pivotal */
} /* namespace com */

#endif /* INTERNALLOGGER_H_ */
