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

package hydra.log;

import hydra.*;

/**
*
* A class used to store keys for log configuration settings.
*
*/

public class LogPrms extends BasePrms {

    /**
     *  (boolean)
     *  Whether to write log messages to a file.  Default is true.
     *  File logging occurs on a per-VM basis.
     */
    public static Long file_logging;

    /**
     *  (String)
     *  Debug level for file logging.  See {@link
     *  com.gemstone.gemfire.LogWriter} for valid options. 
     *  Default is "info".
     */
    public static Long file_logLevel;

    /**
     *  (int)
     *  Maximum size of each VM's log file in kilobytes.  A positive value
     *  creates a rolling log that overwrites the earliest entries once the
     *  maximum is reached.  Default is no maximum, so that all log entries are
     *  retained.  A zero value is equivalent to the default.  Negative values
     *  are illegal.
     */
    public static Long file_maxKBPerVM;

    /**
     *  (String)
     *  Whether to create a merged log file after the test run.
     *  Supported values are <code>"true"</code> (always merge logs),
     *  <code>"false"</code> (never merge logs), and
     *  <code>"onFailure"</code> (merge logs when a test does not
     *  pass).  By default, the log files are never merged.
     */
    public static Long mergeLogFiles;

    /**
     * (String)
     * Maximum heap, in megabytes, to use when merging log files.  The hydra
     * default is 512 (-Xmx512m).
     */
    public static Long mergeLogFilesMaxHeapMB;

    /**
     * (String)
     * Arguments to {@link com.gemstone.gemfire.internal.MergeLogFiles} to use
     * when merging log files.  The hydra default is "-dirCount 1".  Note that
     * the "-mergeFile" argument is accounted for internally.
     */
    public static Long mergeLogFilesArgs;

    static {
        setValues( LogPrms.class );
    }
    public static void main( String args[] ) {
        dumpKeys();
    }
}
