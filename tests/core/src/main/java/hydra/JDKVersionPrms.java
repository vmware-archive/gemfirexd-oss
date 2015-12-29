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
 * A class used to store keys for JDK version configuration settings. The
 * settings are used to create instances of {@link JDKVersionDescription}.
 * These can be referenced by instances of {@link ClientDescription} as
 * configured using {@link ClientPrms-jdkVersionNames}.
 * <p>
 * The number of description instances is gated by {@link #names}.  For other
 * parameters, if fewer values than names are given, the remaining instances
 * will use the last value in the list.  See $JTESTS/hydra/hydra.txt for more
 * details.
 * <p>
 * Unused parameters default to null, except where noted.  This uses the
 * default, except where noted.
 * <p>
 * Values and fields of a parameter can be set to {@link #DEFAULT},
 * except where noted.  This uses the default, except where noted.
 * <p>
 * Values and fields of a parameter can use oneof, range, or robing
 * except where noted, but each description created will use a fixed value
 * chosen at test configuration time.  Use as a task attribute is illegal.
 */
public class JDKVersionPrms extends BasePrms {

  /**
   * (String(s))
   * Logical names of the JDK version descriptions. Each name must be unique.
   * Defaults to null. Not for use with oneof, range, or robing.
   */
  public static Long names;

  /**
   * (Comma-separated lists of String(s))
   * Java home directories used by hydra client JVMs configured to use this
   * description. If supplied, values must be absolute paths to the Java home
   * directory. Defaults to null, in which case the hydra client uses the
   * Java home configured in its {@link HostPrms#javaHomes}. Otherwise, hydra
   * starts a hydra client with the first Java home given. Each time the JVM
   * is restarted, it uses the next Java home in its list. Once the last Java
   * home is reached, the JVM stays with it for all further restarts.
   * <p>
   * This feature was put in place for tests that do rolling JDK upgrades. It
   * is up to the test developer to use Java versions that are compatible with
   * the corresponding product versions. For example, a product built with
   * JDK 1.7 cannot run in a 1.6 JVM.
   */
  public static Long javaHomes;

//------------------------------------------------------------------------------
// parameter setup
//------------------------------------------------------------------------------

  static {
    setValues(JDKVersionPrms.class);
  }
}
