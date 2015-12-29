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

public final class SystemUtils {

  private static String formatMessage(final String message, final Object... args) {
    return (ArrayUtils.isEmpty(args) ? message : MessageFormat.format(message, args));
  }

  public static void printToStandardError(final String message, final Object... args) {
    System.err.println(formatMessage(message, args));
    System.err.flush();
  }

  public static void printToStandardError(final Throwable t) {
    t.printStackTrace(System.err);
    System.err.flush();
  }

  public static void printToStandardOut(final String message, final Object... args) {
    System.out.println(formatMessage(message, args));
    System.out.flush();
  }

}
