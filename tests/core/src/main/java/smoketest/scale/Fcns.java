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

package smoketest.scale;

/**
 * Functions for use in test configuration files.  All functions must return
 * Strings suitable for further parsing by hydra.
 */
public class Fcns {

  /**
   * Describes non-default socket arguments.
   */
  public static String describeSockets(boolean conserveSockets) {
    StringBuffer buf = new StringBuffer();
    buf.append("Conserve sockets is " + conserveSockets + ".");
    String s = "\"" + buf.toString() + "\"";
    return s;
  }

  /**
   * Describes non-default connection arguments.
   */
  public static String describeConnections(int maxThreads, int maxConnections,
                                           boolean threadLocalConnections) {
    StringBuffer buf = new StringBuffer();
    if (maxThreads > 0) {
      if (buf.length() != 0) buf.append(" ");
      buf.append("Uses selector.");
    }
    if (threadLocalConnections) {
      if (buf.length() != 0) buf.append(" ");
      buf.append("Uses thread local connections.");
    }
    if (maxConnections > 0) {
      if (buf.length() != 0) buf.append(" ");
      buf.append("Limits pool size.");
    }
    if (buf.length() == 0) {
      buf.append("Uses default connection settings.");
    }
    String s = "\"" + buf.toString() + "\"";
    return s;
  }
}
