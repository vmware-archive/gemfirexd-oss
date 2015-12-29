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
 * StringFunctions.cpp
 *
 * Various utility functions for string manipulation.
 *
 *      Author: swale
 */

#include "StringFunctions.h"

namespace com {
  namespace pivotal {
    namespace gemfirexd {

      void StringFunctions::getString(const SQLWCHAR* chars, const SQLLEN len,
          std::string& result) {
        const SQLWCHAR* endChars;
        if (len != SQL_NTS) {
          endChars = (chars + len);
          // reserve some length to avoid reallocations (using len*2 as average)
          result.reserve(result.size() + (len * 2));
        } else {
          endChars = NULL;
        }

        int ch;
        while (chars != endChars && (ch = (*chars++ & 0xFF)) != 0) {
          // get next byte unsigned
          const int k = ch >> 5;
          // classify based on the high order 3 bits
          switch (k) {
            case 6: {
              // two byte encoding
              // 110yyyyy 10xxxxxx
              // use low order 6 bits
              const int y = ch & 0x1F;
              // use low order 6 bits of the next byte
              // It should have high order bits 10, which we don't check.
              const int x = *chars++ & 0x3F;
              // 00000yyy yyxxxxxx
              result.push_back((char)(y << 6 | x));
              break;
            }
            case 7: {
              // three byte encoding
              // 1110zzzz 10yyyyyy 10xxxxxx
              //assert ( b & 0x10 )
              //     == 0 : "UTF8Decoder does not handle 32-bit characters";
              // use low order 4 bits
              const int z = ch & 0x0F;
              // use low order 6 bits of the next byte
              // It should have high order bits 10, which we don't check.
              const int y = *chars++ & 0x3F;
              // use low order 6 bits of the next byte
              // It should have high order bits 10, which we don't check.
              const int x = *chars++ & 0x3F;
              // zzzzyyyy yyxxxxxx
              result.push_back((char)(z << 12 | y << 6 | x));
              break;
            }
            default:
              // one byte encoding
              // 0xxxxxxx
              // use just low order 7 bits
              // 00000000 0xxxxxxx
              result.push_back((char)(ch & 0x7F));
              break;
          }
        }
      }

    } /* namespace gemfirexd */
  } /* namespace pivotal */
} /* namespace com */
