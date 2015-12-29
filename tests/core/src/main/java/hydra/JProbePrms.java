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
 *
 * A class used to store keys for JProbe configuration settings.  The settings
 * are used to create instances of {@link JProbeDescription}.
 * <p>
 * The number of instances is gated by {@link #names}.  The remaining parameters
 * have the indicated default values.  If fewer non-default values than names
 * are given for these parameters, the remaining instances will use the last
 * value in the list.  See $JTESTS/hydra/hydra.txt for more details.
 * <p>
 * When JProbe is enabled on a VM, hydra launches it via
 * <code>$JPROBE/bin/jplauncher</code>.  For more information on hydra's JProbe
 * configuration settings, see Appendix B of a JProbe manual such as
 * <code>$JPROBE/doc/JProbeProfilerGuide.pdf</code>.
 * <p>
 * JProbe cannot be used to launch VMs with working directories that are
 * different from that of the launcher.  For example, hydra clients launched
 * with JProbe must not set {@link HostDescription#userDirs}.
 */

public class JProbePrms extends BasePrms {

    /**
     *  (String(s))
     *  Logical names of the jprobe descriptions.  Each name must be unique.
     *  Defaults to null.  See the include files in $JTESTS/hydraconfig for
     *  common configurations.
     *  <p>
     *  The special logical name "none" can be used to avoid jprobe.  For
     *  example, if a test configuration contains two gemfire systems,
     *  <code>gemfire1</code> and <code>gemfire2</code>, and only
     *  <code>gemfire1</code> is to be probed, set
     *  {@link hydra.GemFirePrms#jprobeNames} to <code>probe1 none</code>,
     *  where <code>probe1</code> is the logical name of the desired jprobe
     *  configuration.
     */
    public static Long names;

    /**
     * (String(s)) Sets the jprobe function to perform.
     * See <code>-jp_function</code> for more information.
     * For example, "performance" or "memory".
     */
    public static Long function;

    /**
     * (String(s)) Type of measurement to make, either "elapsed" or "cpu (default)".
     */
    public static Long measurement;

    /**
     * (String(s) or List of String(s)) Sets the jprobe filters to control
     * what is monitored.  See <code>-jp_collect_data</code> for more information.
     * This parameter must be enclosed in quotes in a hydra configuration file.
     * For example, use <code>*</code> to profile all methods.
     */
    public static Long filters;

    /**
     * (String(s) or List of String(s)) Sets the jprobe triggers to control
     * when monitoring occurs.  See <code>-jp_trigger</code> for more information.
     * For example, "hydra.RemoteTestModule.profile():ME:SH:snapshot" will take
     * a snapshot on entry to the method that causes a hydra client to exit.
     */
    public static Long triggers;

    /**
     * (boolean(s)) Sets whether to gather full stack traces.  Defaults to true.
     * See <code>-jp_use_deep_traces</code> for more information.
     */
    public static Long useDeepTraces;

    /**
     * (boolean(s)) Whether to include garbage-collected objects in heap snapshots.
     * See <code>-jp_garbage_keep</code> for more information.
     */
    public static Long garbageKeep;

    /**
     * (boolean(s)) Whether to collect data from the start.
     * See <code>-jp_record_from_start</code> for more information.
     */
    public static Long recordFromStart;

    /**
     * (boolean(s)) Whether to take a final snapshot on exit.
     * See <code>-jp_final_snapshot</code> for more information.
     */
    public static Long finalSnapshot;

    /**
     * (boolean(s)) Whether the user will monitor the run.
     * See <code>-jp_socket</code> for more information.
     */
    public static Long monitor;

    /**
     * (boolean(s)) Whether object allocation should be tracked for
     * the run.  See <code>-jp_track_objects</code> for more
     * information.
     *
     * @since 3.0
     */
    public static Long trackObjects;;

    static {
        setValues( JProbePrms.class );
    }

    public static void main( String args[] ) {
        Log.createLogWriter( "jprobeprms", "info" );
        dumpKeys();
    }
}
