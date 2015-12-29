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
 * StringFunctions.h
 *
 * Various utility functions for string manipulation.
 *
 *      Author: swale
 */

#ifndef STRINGFUNCTIONS_H_
#define STRINGFUNCTIONS_H_

#include "OdbcBase.h"

extern "C"
{
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <wctype.h>

#include <assert.h>
}

#include <string>

namespace com { namespace pivotal { namespace gemfirexd {

/**
 * Static class containing various string manipulation functions.
 */
class StringFunctions
{
private:
  StringFunctions(); // no instance
  /**
   * convert to UTF8 encoded string from given wide-characted string upto
   * given max size of the output buffer; return true if the output string
   * was truncated; also returns the total length of encoded string if the
   * provided "totalLen" param is non-null
   */
  template<typename CHAR_TYPE>
  static bool getUTFString(const CHAR_TYPE* wchars, const SQLLEN charsLen,
      SQLCHAR* outStr, const SQLLEN outMaxLen, SQLLEN* totalLen) {
    const CHAR_TYPE* endChars =
        (charsLen != SQL_NTS) ? (wchars + charsLen) : NULL;
    SQLCHAR* outPtr = outStr;
    const SQLCHAR* end = outStr + outMaxLen - 1;
    bool truncated = false;
    CHAR_TYPE wch;
    while ((endChars == NULL || wchars < endChars) && (wch = *wchars++) != 0) {
      if (wch > 0 && wch <= 0x7F) {
        if (outPtr <= end) {
          *outPtr++ = (SQLCHAR)wch;
        } else {
          truncated = true;
          break;
        }
      } else if (wch <= 0x7FF) {
        if ((outPtr + 1) <= end) {
          *outPtr++ = (SQLCHAR)(0xC0 + ((wch >> 6) & 0x1F));
          *outPtr++ = (SQLCHAR)(0x80 + (wch & 0x3F));
        } else {
          truncated = true;
          break;
        }
      } else {
        if ((outPtr + 2) <= end) {
          *outPtr++ = (SQLCHAR)(0xE0 + ((wch >> 12) & 0xF));
          *outPtr++ = (SQLCHAR)(0x80 + ((wch >> 6) & 0x3F));
          *outPtr++ = (SQLCHAR)(0x80 + (wch & 0x3F));
        } else {
          truncated = true;
          break;
        }
      }
    }
    const int len = outPtr - outStr;
    // if we need to know the length then iterate over the remaining chars
    if (totalLen != NULL) {
      int tlen = len;
      while ((endChars == NULL || wchars < endChars) && (wch = *wchars++) != 0) {
        if (wch > 0 && wch <= 0x7F) {
          tlen++;
        } else if (wch <= 0x7FF) {
          tlen += 2;
        } else {
          tlen += 3;
        }
      }
      *totalLen = tlen;
    }
    *outPtr = 0;
    return truncated;
  }

public:
  /**
   * copy given C or wide string to C or wide string without any
   * conversion; "outStr" buffer must be pre-allocated with given max
   * size; return true if the output string was truncated
   */
  template<typename CHAR_TYPE, typename CHAR_TYPE2>
  static bool getASCIIString(const CHAR_TYPE* chars, const SQLLEN len,
      CHAR_TYPE2* outStr, const SQLLEN outMaxLen, SQLLEN* totalLen) {
    // we don't want to be here for wide to C string copy
    assert(sizeof(CHAR_TYPE) == 1 || sizeof(CHAR_TYPE2) == 2);

    if (len == SQL_NTS) {
      const CHAR_TYPE* start = chars;
      const CHAR_TYPE* end = chars + outMaxLen - 1;
      while (chars < end) {
        if ((*outStr = *chars) == 0) {
          if (totalLen != NULL) {
            *totalLen = (chars - start);
          }
          return false;
        }
        outStr++, chars++;
      }
      *outStr = 0;
      // iterate over the remaining characters if we need to return
      // the actual length of the string
      if (totalLen != NULL) {
        while (*chars) {
          chars++;
        }
        *totalLen = (chars - start);
      }
      return true;
    } else {
      const SQLLEN outLen =
          (outMaxLen <= 0 || len < outMaxLen) ? len : (outMaxLen - 1);
      if (sizeof(CHAR_TYPE) == sizeof(CHAR_TYPE2)) {
        ::memcpy(outStr, chars, outLen);
        outStr += outLen;
      } else {
        const CHAR_TYPE* end = chars + outLen;
        while (chars < end) {
          *outStr++ = *chars++;
        }
      }
      *outStr = 0;
      if (totalLen != NULL) {
        *totalLen = len;
      }
      return (outLen < len);
    }
  }

  /**
   * Copy and possibly convert given C or wide string to output
   * C or wide string. For the case of wide string copied to C output
   * string, UTF8 conversion is done for the output via an overload.
   */
  inline static bool copyString(const SQLCHAR* chars, const SQLLEN len,
      SQLCHAR* outStr, const SQLLEN outMaxLen, SQLLEN* totalLen) {
    return getASCIIString<SQLCHAR, SQLCHAR>(chars, len, outStr, outMaxLen,
        totalLen);
  }
  inline static bool copyString(const char* chars, const SQLLEN len,
      SQLCHAR* outStr, const SQLLEN outMaxLen, SQLLEN* totalLen) {
    return getASCIIString<char, SQLCHAR>(chars, len, outStr, outMaxLen,
        totalLen);
  }
  inline static bool copyString(const SQLCHAR* chars, const SQLLEN len,
      SQLWCHAR* outStr, const SQLLEN outMaxLen, SQLLEN* totalLen) {
    return getASCIIString<SQLCHAR, SQLWCHAR>(chars, len, outStr, outMaxLen,
        totalLen);
  }
  inline static bool copyString(const SQLWCHAR* chars, const SQLLEN len,
      SQLWCHAR* outStr, const SQLLEN outMaxLen, SQLLEN* totalLen) {
    return getASCIIString<SQLWCHAR, SQLWCHAR>(chars, len, outStr, outMaxLen,
        totalLen);
  }
  inline static bool copyString(const SQLWCHAR* chars, const SQLLEN len,
      SQLCHAR* outStr, const SQLLEN outMaxLen, SQLLEN* totalLen) {
    return getUTFString(chars, len, outStr, outMaxLen, totalLen);
  }

  inline static void getString(const SQLCHAR* chars, const SQLLEN len,
      std::string& result) {
    if (len == SQL_NTS) {
      result.append((const char*)chars);
    } else {
      result.append((const char*)chars, len);
    }
  }

  static void getString(const SQLWCHAR* chars, const SQLLEN len,
      std::string& result);

  /**
   * convert a UTF8 encoded string to given wide-characted string upto
   * given max size of the output buffer; return the size of output string
   * excluding the terminating null
   */
  template<typename CHAR_TYPE>
  static int getWideString(const char* utf8Chars, const SQLLEN charsLen,
      CHAR_TYPE* outStr, const SQLLEN outMaxLen) {
    const char* endChars =
        (charsLen != SQL_NTS) ? (utf8Chars + charsLen) : NULL;
    bool checkEndOfString = true;
    CHAR_TYPE* outPtr = outStr;
    const CHAR_TYPE* end = outStr + outMaxLen - 1;
    int ch;
    if (outStr != NULL) {
      while (outPtr < end) {
        // get next byte unsigned
        if ((endChars != NULL && utf8Chars >= endChars)
            || (ch = (*utf8Chars++ & 0xFF)) == 0) {
          checkEndOfString = false; // end of string reached
          break;
        }
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
            const int x = *utf8Chars++ & 0x3F;
            // 00000yyy yyxxxxxx
            *outPtr++ = (y << 6 | x);
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
            const int y = *utf8Chars++ & 0x3F;
            // use low order 6 bits of the next byte
            // It should have high order bits 10, which we don't check.
            const int x = *utf8Chars++ & 0x3F;
            // zzzzyyyy yyxxxxxx
            *outPtr++ = (z << 12 | y << 6 | x);
            break;
          }
          default:
            // one byte encoding
            // 0xxxxxxx
            // use just low order 7 bits
            // 00000000 0xxxxxxx
            *outPtr++ = (ch & 0x7F);
            break;
        }
      }
      *outPtr = 0;
    }
    // iterate over the remaining chars to know the full length
    if (checkEndOfString) {
      while ((endChars == NULL || utf8Chars < endChars)
          && (ch = (*utf8Chars++ & 0xFF)) != 0) {
        const int k = ch >> 5;
        // classify based on the high order 3 bits
        switch (k) {
          case 6: {
            // two byte encoding
            // 110yyyyy 10xxxxxx
            utf8Chars++;
            outPtr++;
            break;
          }
          case 7: {
            // three byte encoding
            // 1110zzzz 10yyyyyy 10xxxxxx
            utf8Chars += 2;
            outPtr++;
            break;
          }
          default:
            // one byte encoding
            // 0xxxxxxx
            outPtr++;
            break;
        }
      }
    }
    return (outPtr - outStr);
  }

  template<typename CHAR_TYPE>
  static int strlen(const CHAR_TYPE* str) {
    const CHAR_TYPE* strp = str;
    while (*strp) {
      strp++;
    }
    return (strp - str);
  }

  inline static int strlen(const SQLCHAR* str) {
    return ::strlen((const char*)str);
  }

  inline static int strlen(const char* str) {
    return ::strlen(str);
  }

  template<typename CHAR_TYPE, typename CHAR_TYPE2>
  static int strlen(const CHAR_TYPE* str, const CHAR_TYPE2* dummy) {
    return strlen(str);
  }

  inline static int strlen(const SQLWCHAR* wchars, const SQLCHAR* dummy) {
    SQLWCHAR wch;
    int len = 0;
    while ((wch = *wchars++) != 0) {
      if (wch > 0 && wch <= 0x7F) {
        len++;
      } else if (wch <= 0x7FF) {
        len += 2;
      } else {
        len += 3;
      }
    }
    return len;
  }

  template<typename CHAR_TYPE, typename CHAR_TYPE2>
  static int strcmp(const CHAR_TYPE* str1, int str1Len, const CHAR_TYPE2* str2,
      int str2Len) {
    CHAR_TYPE ch1;
    CHAR_TYPE2 ch2;
    while (str1Len-- > 0) {
      if (str2Len-- <= 0) {
        return 1;
      } else if ((ch1 = *str1) != (ch2 = *str2)) {
        return (ch1 - ch2);
      }
      str1++, str2++;
    }
    return (str2Len == 0) ? 0 : -1;
  }

  template<typename CHAR_TYPE, typename CHAR_TYPE2>
  static int strncmp(const CHAR_TYPE* str1, const CHAR_TYPE2* str2,
      int strLen) {
    CHAR_TYPE ch1;
    CHAR_TYPE2 ch2;
    while (strLen-- > 0) {
      if ((ch2 = *str2) == 0) {
        return 1;
      } else if ((ch1 = *str1) != ch2) {
        return (ch1 - ch2);
      }
      str1++, str2++;
    }
    return (strLen == 0) ? 0 : -1;
  }

  inline static int toUpper(const SQLCHAR ch) {
    return ::toupper(ch);
  }

  inline static int toUpper(const char ch) {
    return ::toupper(ch);
  }

  inline static int toUpper(const SQLWCHAR ch) {
    return ::towupper(ch);
  }

//#ifndef _WINDOWS
//  inline static int toUpper(const wchar_t ch)
//  {
//    return ::towupper(ch);
//  }
//#endif

  template<typename STR1_TYPE, typename STR2_TYPE>
  static bool equalsIgnoreCase(const STR1_TYPE& str1, const STR2_TYPE& str2) {
    const size_t str1Len = str1.size();
    if (str1Len == str2.size()) {
      for (size_t index = 0; index < str1Len; index++) {
        if (toUpper(str1[index]) != toUpper(str2[index])) {
          return false;
        }
      }
      return true;
    } else {
      return false;
    }
  }

  template<typename CHAR_TYPE>
  static const CHAR_TYPE* strchr(const CHAR_TYPE* s, const char c) {
    CHAR_TYPE ch;
    while ((ch = *s)) {
      if (ch == c) {
        return s;
      }
      s++;
    }
    // check for 0 itself being searched
    if (ch == c) {
      return s;
    }
    return NULL;
  }

  template<typename CHAR_TYPE>
  static const CHAR_TYPE* ltrim(const CHAR_TYPE* startp) {
    CHAR_TYPE ch;
    if (sizeof(CHAR_TYPE) == 1) {
      while ((ch = *startp) == ' ' || ch == '\t') {
        startp++;
      }
    } else {
      while ((ch = *startp) == L' ' || ch == L'\t') {
        startp++;
      }
    }
    return startp;
  }

  template<typename CHAR_TYPE>
  static const CHAR_TYPE* rtrim(const CHAR_TYPE* endp) {
    CHAR_TYPE ch;
    if (sizeof(CHAR_TYPE) == 1) {
      while ((ch = *endp) == ' ' || ch == '\t') {
        endp--;
      }
    } else {
      while ((ch = *endp) == L' ' || ch == L'\t') {
        endp--;
      }
    }
    return endp;
  }

  template<typename STR_TYPE, typename CHAR_TYPE>
  static void replace(STR_TYPE& s, const CHAR_TYPE* from, const CHAR_TYPE* to) {
    size_t pos;
    if ((pos = s.find(from, 0)) != s.npos) {
      STR_TYPE res;
      const int sLen = s.size();
      const int fromLen = strlen(from);
      int startPos = pos + fromLen;
      res.append(s.substr(0, pos));
      res.append(to);
      while (startPos < sLen && (pos = s.find(from, startPos)) != s.npos) {
        res.append(s.substr(startPos, pos - startPos));
        res.append(to);
        startPos = pos + fromLen;
      }
      s = res;
    }
  }
};

}}}

#endif /* STRINGFUNCTIONS_H_ */
