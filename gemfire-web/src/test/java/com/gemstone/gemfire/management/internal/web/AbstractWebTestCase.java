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
package com.gemstone.gemfire.management.internal.web;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;

import com.gemstone.gemfire.internal.lang.StringUtils;
import com.gemstone.gemfire.management.internal.web.domain.Link;

/**
 * The AbstractWebDomainTests class is abstract base class containing functionality common to a test suite classes
 * in the com.gemstone.gemfire.management.internal.web.domain package.
 * <p/>
 * @author John Blum
 * @see java.net.URI
 * @see java.net.URLDecoder
 * @see java.net.URLEncoder
 * @see com.gemstone.gemfire.management.internal.web.domain.Link
 * @since 7.5
 */
public abstract class AbstractWebTestCase {

  protected String decode(final String encodedValue) throws UnsupportedEncodingException {
    return URLDecoder.decode(encodedValue, StringUtils.UTF_8);
  }

  protected String encode(final String value) throws UnsupportedEncodingException {
    return URLEncoder.encode(value, StringUtils.UTF_8);
  }

  protected String toString(final Link... links) throws UnsupportedEncodingException {
    final StringBuilder buffer = new StringBuilder("[");
    int count = 0;

    for (final Link link : links) {
      buffer.append(count++ > 0 ? ", " : StringUtils.EMPTY_STRING).append(toString(link));

    }

    buffer.append("]");

    return buffer.toString();
  }

  protected String toString(final Link link) throws UnsupportedEncodingException {
    return link.toHttpRequestLine();
  }

  protected String toString(final URI uri) throws UnsupportedEncodingException {
    return decode(uri.toString());
  }

  protected URI toUri(final String uriString) throws UnsupportedEncodingException, URISyntaxException {
    return new URI(encode(uriString));
  }

}
