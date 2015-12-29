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

package hydra.gemfirexd;

import hydra.BasePrms;
import hydra.HydraConfigException;

/**
 * A class used to store keys for network server configuration settings.
 * The settings are used to create instances of {@link
 * NetworkServerDescription}.
 * <p>
 * The number of description instances is gated by {@link #names}.  For other
 * parameters, if fewer values than names are given, the remaining instances
 * will use the last value in the list.  See $JTESTS/hydra/hydra.txt for more
 * details.
 * <p>
 * Unused parameters default to null, except where noted.  This uses the
 * product default, except where noted.
 * <p>
 * Values and fields of a parameter can be set to {@link #DEFAULT},
 * except where noted.  This uses the product default, except where noted.
 * <p>
 * Values, fields, and subfields can be set to {@link #NONE} where noted, with
 * the documented effect.
 * <p>
 * Values and fields of a parameter can use oneof, range, or robing
 * except where noted, but each description created will use a fixed value
 * chosen at test configuration time.  Use as a task attribute is illegal.
 */
public class NetworkServerPrms extends BasePrms {

  static {
    setValues(NetworkServerPrms.class);
  }

  /**
   * (String(s))
   * Logical names of the network server descriptions.  Each name must be
   * unique.  Defaults to null.  Not for use with oneof, range, or robing.
   */
  public static Long names;

  /**
   * (String(s))
   * The connection listener class name.  Can be specified as {@link #NONE}
   * (default).
   */
  public static Long connectionListener;

  /**
   * (boolean(s))
   * Whether to log connections.  Defaults to true.
   */
  public static Long logConnections;

  /**
   * (int(s))
   * Maximum time, in milliseconds, for a server thread to wait for a new
   * client session before terminating due to lack of activity.
   */
  public static Long maxIdleTime;

  /**
   * (int(s))
   * Time to wait for the network server to start, in seconds.  Defaults to 30.
   */
  public static Long maxStartupWaitSec;

  /**
   * (int(s))
   * Maximum number of threads that can be used for connections.  Defaults to 0
   * (unlimited).  Be sure to set {@link #timeSlice} when limiting the number
   * of threads.
   */
  public static Long maxThreads;

  /**
   * (int(s))
   * The number of network servers to start.  Defaults to 1.
   */
  public static Long numServers;

  /**
   * (boolean(s))
   * Whether to reuse ports on restart.  Defaults to true.
   */
  public static Long reusePorts;

  /**
   * (int(s))
   * Time each session can use a connection thread before yielding to a waiting
   * session, in milliseconds.  Defaults to 0 (no yield).
   */
  public static Long timeSlice;

  /**
   * (boolean(s))
   * Whether to trace connections.  Defaults to false.
   */
  public static Long trace;
}
