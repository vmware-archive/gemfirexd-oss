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
 * A class used to store keys for configuration settings related to SSL.  The
 * settings are used to create instances of {@link SSLDescription}, and can be
 * referenced from a gemfire configuration via {@link GemFirePrms#sslName}.
 * <p>
 * The number of description instances is gated by {@link #names}.  For other
 * parameters, if fewer values than names are given, the remaining instances
 * will use the last value in the list.  See $JTESTS/hydra/hydra.txt for more
 * details.
 * <p>
 * Unused parameters default to null, except where noted.  This uses the
 * product default, except where noted.
 * <p>
 * Values and fields of a parameter can be set to {@link #DEFAULT}, except
 * where noted.  This uses the product default, except where noted.
 * <p>
 * Values and fields of a parameter can use oneof or robing except
 * where noted, but each description created will use a fixed value
 * chosen at test configuration time.  Use as a task attribute is illegal.
 */
public class SSLPrms extends BasePrms {

  /**
   * (String(s))
   * Logical names of the SSL descriptions.  Each name must be unique.
   * Defaults to null.  Not for use with oneof, range, or robing.
   */
  public static Long names;

  /**
   * (Strings(s))
   * Key store path for each configuration.  Can be relative to $JTESTS.
   * This is a required field.
   */
  public static Long keyStore;

  /**
   * (Strings(s))
   * Key store password for each configuration.  This is a required field.
   */
  public static Long keyStorePassword;
 
  /**
   * (String(s))
   * SSL ciphers for each configuration.  Defaults to use the provider
   * defaults.
   */
  public static Long sslCiphers;

  /**
   * (boolean(s))
   * SSL enabled for each configuration.
   */
  public static Long sslEnabled;

  /**
   * (String(s))
   * SSL protocols for each configuration.  Defaults to use the provider
   * defaults.
   */
  public static Long sslProtocols;

  /**
   * (boolean(s))
   * SSL require authentication for each configuration.
   */
  public static Long sslRequireAuthentication;

  /**
   * (Strings(s))
   * Trust store path for each configuration.  Can be relative to $JTESTS.
   * Defaults to the value of {@link #keyStore}.
   */
  public static Long trustStore;

  /**
   * (Strings(s))
   * Trust store password for each configuration.  Defaults to the value of
   * {@link #keyStorePassword}.
   */
  public static Long trustStorePassword;
 
  static {
    setValues(SSLPrms.class);
  }
}
