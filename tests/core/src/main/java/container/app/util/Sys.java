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

public final class Sys {

  public static void err(final String message, final Object... args) {
    SystemUtils.printToStandardError(message, args);
  }

  public static void err(final Throwable t) {
    SystemUtils.printToStandardError(t);
  }

  public static void out(final String message, final Object... args) {
    SystemUtils.printToStandardOut(message, args);
  }

}
