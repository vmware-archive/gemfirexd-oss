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

import java.util.Iterator;

/**
 * A class used to store keys for WindowTester/GFMon configuration settings.
 */
public class GFMonPrms extends BasePrms {

  static {
    setValues(GFMonPrms.class);
  }

  public static final String DEFAULT_EXTRA_VM_ARGS = "-Xms64m -Xmx256m";
  public static final String LOG_LEVEL_PROPERTY_NAME = "logLevel";

  /**
   * (String)
   * The name of the WindowTester test class to run.  Defaults to null.
   * When set, the hydra master controller will spawn a VM on the local host
   * that runs this test in WindowTester with in-process access to the GFMon
   * tool.  Defaults to false.
   * <p>
   * The test class must be on the classpath specified by the system property
   * EXTRA_JTESTS.  The GFMON system property must also be specified and point
   * to a user-owned modifiable copy of a suitable GFMon build.
   */
  public static Long testClassName;

  /**
   * Returns the name of the WindowTester class found in {@link #testClassName}.
   */
  public static String getTestClassName() {
    Long key = testClassName;
    String val = tab().stringAt(key, null);
    /*
    if (val != null) {
      try
      {
        // verify class exists
        Class.forName(val);
      }
      catch (NoClassDefFoundError e) {
        String s = BasePrms.nameForKey(key) + " class not found: " + val;
        throw new HydraConfigException(s, e);
      }
      catch (ClassNotFoundException e) {
        String s = BasePrms.nameForKey(key) + " class not found: " + val;
        throw new HydraConfigException(s, e);
      }
    }
    */
    return val;
  }

  /**
   * (String(s))
   * Extra VM arguments passed to the GFMon/WindowTester VM.  Ignored unless
   * {@link #testClassName} is set.  Defaults to {@link #DEFAULT_EXTRA_VM_ARGS}.
   */
  public static Long extraVMArgs;

  /**
   * Returns the extra VM args found in {@link #extraVMArgs}.
   */
  protected static String getExtraVMArgs() {
    Long key = extraVMArgs;
    String args = DEFAULT_EXTRA_VM_ARGS;
    HydraVector val = tab().vecAt(extraVMArgs, null);
    if (val != null) {
      for (Iterator it = val.iterator(); it.hasNext();) {
        try {
          String arg = (String)it.next();
          if (arg.equals("=")) {
            String s = "Malformed value in " + nameForKey(key)
                     + ", use quotes to include '='";
            throw new HydraConfigException(s);
          } else {
            args += " " + arg;
          }
        } catch (ClassCastException e) {
          String s = "Malformed value in " + nameForKey(key)
                   + ", use single string or list of values";
          throw new HydraConfigException(s, e);
        }
      }
    }
    // Use IPv6 addresses if asked
    if (TestConfig.tab().booleanAt(Prms.useIPv6)) {
      args += " -D" + Prms.USE_IPV6_PROPERTY + "=true";
    }
    return args;
  }

  /**
   * (int)
   * The maximum time, in seconds, hydra should wait for the WindowTester/GFMon
   * VM to start up.  Defaults to 300;
   *
   * @throws HydraConfigException if the value is negative.
   */
  public static Long maxStartupWaitSec;

  /**
   * Returns the value of {@link #maxStartupWaitSec}.
   */
  public static int getMaxStartupWaitSec() {
    Long key = maxStartupWaitSec;;
    int val = tab().intAt(key, 300);
    if (val < 0) {
      String s = BasePrms.nameForKey(key) + " cannot be negative: " + val;
      throw new HydraConfigException(s);
    }
    return val;
  }

  /**
   * (int)
   * The maximum time, in seconds, hydra should wait for the WindowTester/GFMon
   * VM to shut down.  Defaults to 300;
   *
   * @throws HydraConfigException if the value is negative.
   */
  public static Long maxShutdownWaitSec;

  /**
   * Returns the value of {@link #maxShutdownWaitSec}.
   */
  public static int getMaxShutdownWaitSec() {
    Long key = maxShutdownWaitSec;;
    int val = tab().intAt(key, 300);
    if (val < 0) {
      String s = BasePrms.nameForKey(key) + " cannot be negative: " + val;
      throw new HydraConfigException(s);
    }
    return val;
  }

  /**
   * (String(s))
   * Log level for the log file.
   */
  public static Long logLevel;

  /**
   * Returns the value of {@link #logLevel}.  Defaults to "info".
   */
  public static String getLogLevel() {
    Long key = logLevel;;
    return tab().stringAt(key, "info");
  }
}
