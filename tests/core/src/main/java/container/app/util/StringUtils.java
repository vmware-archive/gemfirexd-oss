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
package container.app.util;

import java.text.CharacterIterator;
import java.text.StringCharacterIterator;

public final class StringUtils {

  public static final String NOT_IMPLEMENTED = "Not Implemented!";
  public static final String OPERATION_NOT_SUPPORTED = "Operation Not Supported!";

  public static String defaultIfNoValue(final String value, final String defaultValue) {
    return (hasValue(value) ? value : defaultValue);
  }

  public static boolean hasValue(final String value) {
    return !isWorthless(value);
  }

  public static boolean isBlank(final String value) {
    return (length(value) > 0 && "".equals(trim(value)));
  }

  public static boolean isEmpty(final String value) {
    return "".equals(value);
  }
  
  public static boolean isWorthless(final String value) {
    return (value == null || isEmpty(value) || isBlank(value));
  }

  public static int length(final String value) {
    return (value == null ? 0 : value.length());
  }

  public static String reverse(final String value) {
    if (value != null) {
      final CharacterIterator it = new StringCharacterIterator(value);
      final StringBuilder buffer = new StringBuilder();

      for (char c = it.last(); c != CharacterIterator.DONE; c = it.previous()) {
        buffer.append(c);
      }

      return buffer.toString();
    }

    return value;
  }

  public static String[] split(final String value) {
    return split(value, "\\s");
  }

  public static String[] split(final String value, final String regex) {
    return (value != null ? value.split(regex) : new String[0]);
  }

  public static String toLowerCase(final String value) {
    return (value == null ? null : value.toLowerCase());
  }

  public static String toUpperCase(final String value) {
    return (value == null ? null : value.toUpperCase());
  }

  public static String trim(final String value) {
    return (value == null ? null : value.trim());
  }

}
