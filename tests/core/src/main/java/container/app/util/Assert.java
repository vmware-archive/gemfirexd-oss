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

import java.text.MessageFormat;

public final class Assert {

  private static String formatMessage(final String message, final Object... args) {
    return (ArrayUtils.isEmpty(args) ? message : MessageFormat.format(message, args));
  }

  public static void hasValue(final String value, final String message, final Object... args) {
    hasValue(value, new IllegalArgumentException(formatMessage(message, args)));
  }

  public static void hasValue(final String value, final RuntimeException e) {
    if (StringUtils.isWorthless(value)) {
      throw e;
    }
  }
 
  public static void instanceOf(final Object obj, final Class<?> clazz, final String message, final Object... args) {
    instanceOf(obj, clazz, new IllegalArgumentException(formatMessage(message, args)));
  }

  public static void instanceOf(final Object obj, final Class<?> clazz, final RuntimeException e) {
    if (!clazz.isAssignableFrom(obj.getClass())) {
      throw e;
    }
  }

  public static void notNull(final Object obj, final String message, final Object... args) {
    notNull(obj, new NullPointerException(formatMessage(message, args)));
  }

  public static void notNull(final Object obj, final RuntimeException e) {
    if (obj == null) {
      throw e;
    }
  }

  public static void state(final Boolean condition, final String message, final Object... args) {
    state(condition, new IllegalStateException(formatMessage(message, args)));
  }

  public static void state(final Boolean condition, final RuntimeException e) {
    if (!Boolean.TRUE.equals(condition)) {
      throw e;
    }
  }

}
