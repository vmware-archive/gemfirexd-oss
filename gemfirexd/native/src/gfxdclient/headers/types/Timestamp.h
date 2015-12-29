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
 * Timestamp.h
 *
 *      Author: swale
 */

#ifndef TIMESTAMP_H_
#define TIMESTAMP_H_

#include "SQLException.h"
#include "DateTime.h"

namespace com
{
  namespace pivotal
  {
    namespace gemfirexd
    {
      namespace client
      {
        namespace types
        {

          class Timestamp: public DateTime
          {
          private:
            int32_t m_nanos;

            Timestamp(); // no default constructor

          public:
            static const int32_t NANOS_MAX = 1000000000;

            /**
             * Seconds since Epoch 1970-01-01 00:00:00 +0000 (UTC).
             */
            Timestamp(const int64_t secsSinceEpoch) :
                DateTime(secsSinceEpoch), m_nanos(0) {
            }

            /**
             * Seconds since Epoch 1970-01-01 00:00:00 +0000 (UTC)
             * and nano seconds component of this timestamp.
             */
            Timestamp(const int64_t secsSinceEpoch, const int32_t nanos) :
                DateTime(secsSinceEpoch) {
              setNanos(nanos);
            }

            /**
             * Local time as year, month, day, hour, minutes, seconds.
             *
             * @param year  number of years e.g. 1998
             * @param month number of months since January, in the range 1 to 12
             * @param day   day of the month, in the range 1 to 31
             * @param hour  number of hours past midnight, in the range 0 to 23
             * @param min   number of minutes after the hour,
             *              in the range 0 to 59
             * @param sec   number of seconds after the minute,
             *              normally in the range 0 to 59, but can be up to 60
             *              to allow for leap seconds
             * @param nanos the nano seconds component of this timestamp
             */
            Timestamp(const uint16_t year, const uint16_t month,
                const uint16_t day, const uint16_t hour, const uint16_t min,
                const uint16_t sec, int32_t nanos, const bool utc = true) :
                DateTime(year, month, day, hour, min, sec, utc) {
              setNanos(nanos);
            }

            /** Get the nano seconds component of this timestamp */
            inline int32_t getNanos() const throw() {
              return m_nanos;
            }

            /** Set the new nano seconds component of this timestamp */
            inline void setNanos(int32_t nanos) {
              if (nanos >= 0 && nanos < NANOS_MAX) {
                m_nanos = nanos;
              }
              else {
                throw GET_SQLEXCEPTION2(
                    SQLStateMessage::LANG_DATE_RANGE_EXCEPTION_MSG3, nanos,
                    NANOS_MAX - 1);
              }
            }

            /**
             * Create a Timestamp instance parsing the timestamp
             * in string form hh:mm:ss hh:mm:ss[.NNNNNNNNN]
             */
            static Timestamp parseString(const std::string& str,
                const bool utc = true, const uint32_t columnIndex = 0);

            /**
             * Get the UTC date time representation
             * yyyy-mm-dd hh:mm:ss[.NNNNNNNNN]. The last portion is the
             * nano seconds component that is printed only if present
             * and trailing zeros may be removed.
             */
            std::string& toString(std::string& str,
                const bool utc = true) const;

            /** convert to thrift Timestamp for sending over the wire */
            inline thrift::Timestamp toTTimestamp() const {
              return thrift::Timestamp(getEpochTime(), m_nanos);
            }
          };

          struct time_fmt_manip
          {
          private:
            const bool m_utc;

          public:

            time_fmt_manip(const bool utc) :
                m_utc(utc) {
            }

            void operator()(std::ostream& out) const;
          };

          inline time_fmt_manip time_fmt(const bool utc) {
            return time_fmt_manip(utc);
          }

          std::ostream& operator<<(std::ostream& out, const time_fmt_manip op);

        } /* namespace types */
      } /* namespace client */
    } /* namespace gemfirexd */
  } /* namespace pivotal */
} /* namespace com */

std::ostream& operator <<(std::ostream& stream,
    com::pivotal::gemfirexd::client::types::Timestamp ts);

#endif /* TIMESTAMP_H_ */
