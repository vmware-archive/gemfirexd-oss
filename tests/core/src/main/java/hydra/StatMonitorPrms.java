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

import java.util.Vector;

/**
 * A class used to store keys for {@link hydra.StatMonitor} configuration
 * settings.
 */

public class StatMonitorPrms extends BasePrms {

    /**
     * (int)
     * Rate at which the monitor should sample statistics and check conditions,
     * in milliseconds.  Default is DEFAULT_SAMPLE_RATE_MS.  Rate must be at
     * least 1000 (one second) or an error is thrown, to prevent inadvertent
     * oversampling.
     */
    public static Long sampleRateMs;
    public static int getSampleRateMs(TestConfig tc) {
      Long key = sampleRateMs;
      int val = tc.getParameters().intAt(key, DEFAULT_SAMPLE_RATE_MS);
      if (val < 1000) {
        String s = "Illegal value for " + nameForKey(key) + ": " + val;
        throw new HydraConfigException(s);
      }
      return val;
    }
    public static final int DEFAULT_SAMPLE_RATE_MS = 60000;

    /**
     * (List of list(s))
     * Conditions for hostagents to monitor on behalf of the test, which, if
     * violated, should cause the test to take the specified action.  The conditions must be based on
     * statistics available in hostagent VMs.  All hostagents use the same set
     * of conditions.  The conditions can be used to watch the health of the
     * host represented by the hostagent.  Default is null.  Can be specified
     * as {@link BasePrms#NONE} to turn off monitoring altogether.
     * <p>
     * A condition consists of five fields: type stat op amount action.  Valid types
     * are those available to all VMs running the statistics monitor.  Types
     * that include O/S names, such as LinuxSystemStats, should be given simply
     * as SystemStats, since the monitor prepends the appropriate O/S.  Valid
     * stats are those available for the type.  Valid ops are the constants seen
     * in the javadocs for {@link StatMonitor}.  Valid amounts are any double.
     * Valid actions are "halt" (to halt the test) and "dump" (to print stacks).
     * For example, SystemStats pagesSwappedIn hasChangedByLessThan 5000 halt".
     */
    public static Long conditions;
    public static Vector getConditions(TestConfig tc) {
      Long key = conditions;

      // first check for no conditions
      Vector val = tc.getParameters().vecAt(key, null);
      if (val == null) {
        return null;
      } else if (val.size() == 1) {
        String str = (String)val.get(0);
        if (str.equalsIgnoreCase(BasePrms.NONE)) {
          return null;
        }
      }
      // read conditions as a list of lists
      int i = 0;
      Vector vals = null;
      do {
        val = tc.getParameters().vecAt(key, i, null);
        if (val != null) {
          if (vals == null) {
            vals = new Vector();
          }
          vals.add(val);
        }
        ++i;
      } while (val != null);

      // parse the conditions
      try {
        return StatMonitor.parseConditions(vals);
      } catch(HydraConfigException e) {
        String s = "Illegal value for " + nameForKey(key);
        throw new HydraConfigException(s, e);
      }
    }

    /**
     * (boolean)
     * Whether to archive statistics in the VM running the statmonitor.  If
     * true, archives are written to "statmon_<pid>/statArchive.gfs" in the
     * master user directory.  Defaults to false.
     */
    public static Long archiveStats;
    public static boolean archiveStats(TestConfig tc) {
      Long key = archiveStats;
      return tc.getParameters().booleanAt(key, false);
    }

    /**
     * (boolean)
     * Whether to log monitoring information each time a condition check is
     * performed.
     */
    public static Long verbose;
    public static boolean verbose(TestConfig tc) {
      Long key = verbose;
      return tc.getParameters().booleanAt(key, false);
    }

    static {
        setValues( StatMonitorPrms.class );
    }

    public static void main( String args[] ) {
        Log.createLogWriter( "hostagentprms", "info" );
        dumpKeys();
    }
}
