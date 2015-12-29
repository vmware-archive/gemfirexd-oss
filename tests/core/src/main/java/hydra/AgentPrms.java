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

package hydra;

/**
 * A class used to store keys for agent configuration settings.
 * The settings are used to create instances of {@link AgentDescription}.
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
 * Values and fields of a parameter can use oneof, range, or robing
 * except where noted, but each description created will use a fixed value
 * chosen at test configuration time.  Use as a task attribute is illegal.
 */
public class AgentPrms extends BasePrms {

  static {
    setValues(AgentPrms.class);
  }

  /**
   * (String(s))
   * Logical names of the agent descriptions.  Each name must be unique.
   * Defaults to null.  Not for use with oneof, range, or robing.
   */
  public static Long names;

  /**
   * (String(s))
   * Name of logical admin configuration, as found in {@link AdminPrms#names}.
   * This is a required parameter with no default value.
   */
  public static Long adminName;

  /**
   * (boolean(s))
   * HTTP enabled.  Defaults to false.
   */
  public static Long httpEnabled;

  /**
   * (boolean(s))
   * RMI enabled.
   */
  public static Long rmiEnabled;

  /**
   * (boolean(s))
   * RMI registry enabled.
   */
  public static Long rmiRegistryEnabled;

  /**
   * (String(s))
   * Name of logical SSL configuration, as found in {@link SSLPrms#names}.
   * Can be specified as {@link #NONE} (default).
   * <p>
   * The default values for the logical SSL configuration are taken from
   * {@link com.gemstone.gemfire.distributed.internal.DistributionConfig}
   * rather than {@link com.gemstone.gemfire.agent.AgentConfig}, but they
   * are the same.
   * <p>
   * Used to configure security between the agent and the outside world.
   * For security between the agent and the distributed system, see {@link
   * AdminPrms#sslName}.
   */
  public static Long sslName;
}
