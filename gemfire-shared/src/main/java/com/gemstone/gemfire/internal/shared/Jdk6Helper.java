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

package com.gemstone.gemfire.internal.shared;

import java.io.Console;
import java.io.InputStream;
import java.net.NetworkInterface;
import java.net.SocketException;


/**
 * {@link JdkHelper} implementation for JDK6.
 * 
 * @author swale
 * @since 7.5
 */
public final class Jdk6Helper extends Jdk5Helper {

  /** dummy method to force inclusion by classlister */
  public static void init() {
  }

  /**
   * @see JdkHelper#readChars(InputStream, boolean)
   */
  @Override
  public final String readChars(final InputStream in, final boolean noecho) {
    final Console console;
    if (noecho && (console = System.console()) != null) {
      return new String(console.readPassword());
    }
    else {
      return super.readChars(in, noecho);
    }
  }

  /**
   * @see JdkHelper#isInterfaceUp(NetworkInterface)
   */
  @Override
  public boolean isInterfaceUp(NetworkInterface iface) throws SocketException {
    return iface.isUp();
  }
}
