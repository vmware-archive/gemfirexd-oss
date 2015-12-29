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
 * Decimal.h
 *
 *      Author: swale
 */

#ifndef DECIMAL_H_
#define DECIMAL_H_

#include "SQLException.h"

#include <gmp.h>

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

          class Decimal
          {
          private:
            // no copy constructor for now
            Decimal operator=(const Decimal& other);

            void initializeBigInteger(const int8_t signum,
                const int8_t* magnitude, const uint32_t maglen,
                const bool bigEndian);

            /** discard fractional part */
            const mpz_t* getBigInteger(mpz_t* copy) const throw ();

            static uint32_t TEN_POWERS_TABLE[];

            mpz_t m_bigInt;
            uint32_t m_scale;
            mutable uint32_t m_precision;

            void parseString(const std::string& str, const uint32_t columnIndex);

          public:
            Decimal(const thrift::Decimal& dec);

            Decimal(const int8_t signum, const uint32_t scale,
                const int8_t* magnitude, const uint32_t maglen,
                const bool bigEndian);

            Decimal(const int32_t v);

            Decimal(const uint32_t v);

            Decimal(const int64_t v);

            Decimal(const uint64_t v);

            Decimal(const float v, const uint32_t precision =
                DEFAULT_REAL_PRECISION);

            Decimal(const double v, const uint32_t precision =
                DEFAULT_REAL_PRECISION);

            Decimal(const std::string& str, const uint32_t columnIndex = -1);

            Decimal(const Decimal& other);

            static Decimal ZERO;
            static Decimal ONE;

            bool operator==(const Decimal& other) const throw ();

            bool operator!=(const Decimal& other) const throw ();

            uint32_t precision() const throw ();

            uint32_t scale() const throw () {
              return m_scale;
            }

            int32_t signum() const throw () {
              return mpz_sgn(m_bigInt);
            }

            bool toULong(uint64_t& result,
                const bool allowOverflow = false) const throw ();

            bool toLong(int64_t& result,
                const bool allowOverflow = false) const throw ();

            bool toDouble(double& result) const throw ();

            uint32_t toByteArray(std::string& str) const throw ();

            bool wholeDigits(uint8_t* bytes, const uint32_t maxLen,
                uint32_t& actualLen) const throw ();

            void copyTo(thrift::Decimal& target) const throw () {
              toByteArray(target.magnitude);
              target.signum = signum();
              target.scale = m_scale;
            }

            uint32_t toString(std::string& str) const throw ();

            ~Decimal();
          };

        } /* namespace types */
      } /* namespace client */
    } /* namespace gemfirexd */
  } /* namespace pivotal */
} /* namespace com */

#endif /* DECIMAL_H_ */
