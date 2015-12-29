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
package com.gemstone.gemfire.management.internal.web.util;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;

import com.gemstone.gemfire.internal.lang.StringUtils;

/**
 * The UriUtils is a utility class for processing URIs and URLs.
 * <p/>
 * @author John Blum
 * @see java.net.URI
 * @see java.net.URL
 * @see java.net.URLDecoder
 * @see java.net.URLEncoder
 * @since 7.5
 */
@SuppressWarnings("unused")
public abstract class UriUtils {

  public static final String DEFAULT_ENCODING = StringUtils.UTF_8;

  /**
   * Decodes the encoded String value using the default encoding, UTF-8.  It is assumed the String value was encoded
   * with the URLEncoder using the UTF-8 encoding.  This method handles UnsupportedEncodingException by just returning
   * the encodedValue.
   * <p/>
   * @param encodedValue the encoded String value encoded to decode.
   * @return the decoded value of the String.  If UTF-8 is unsupported, then the encodedValue is returned.
   * @see #decode(String, String)
   * @see java.net.URLDecoder
   */
  public static String decode(final String encodedValue) {
    return decode(encodedValue, DEFAULT_ENCODING);
  }

  /**
   * Decodes the encoded String value using the specified encoding (such as UTF-8).  It is assumed the String value
   * was encoded with the URLEncoder using the specified encoding.  This method handles UnsupportedEncodingException
   * by just returning the encodedValue.
   * <p/>
   * @param encodedValue the encoded String value to decode.
   * @param encoding a String value specifying the encoding.
   * @return the decoded value of the String.  If the encoding is unsupported, then the encodedValue is returned.
   * @see #decode(String)
   * @see java.net.URLDecoder
   */
  public static String decode(final String encodedValue, String encoding) {
    try {
      return URLDecoder.decode(encodedValue, encoding);
    }
    catch (UnsupportedEncodingException ignore) {
      return encodedValue;
    }
  }

  /**
   * Encode the specified String value using the default encoding, UTF-8.
   * <p/>
   * @param value the String value to encode.
   * @return an encoded value of the String using the default encoding, UTF-8.  If UTF-8 is unsupported,
   * then value is returned.
   * @see #encode(String, String)
   * @see java.net.URLEncoder
   */
  public static String encode(final String value) {
    return encode(value, DEFAULT_ENCODING);
  }

  /**
   * Encode the String value using the specified encoding.
   * <p/>
   * @param value the String value to encode.
   * @param encoding a String value indicating the encoding.
   * @return an encoded value of the String using the specified encoding.  If the encoding is unsupported,
   * then value is returned.
   * @see #encode(String)
   * @see java.net.URLEncoder
   */
  public static String encode(final String value, final String encoding) {
    try {
      return URLEncoder.encode(value, encoding);
    }
    catch (UnsupportedEncodingException ignore) {
      return value;
    }
  }

}
