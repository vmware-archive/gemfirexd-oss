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

import com.gemstone.gemfire.LogWriter;
//import com.gemstone.gemfire.internal.LocalLogWriter;
import com.gemstone.gemfire.internal.LogWriterImpl;

import hydra.HydraRuntimeException;

import java.io.*;
//import java.util.*;

/**
 * A LogProcessOutputReader will read both stdout and stderr
 * from a {@link java.lang.Process} process and write it to
 * a {@link com.gemstone.gemfire.LogWriter} using the specified
 * log level. The constructor does not return until all output
 * from the process has been read and the process has exited.
 */
public class LogProcessOutputReader {
    private int exitCode;
    /**
     * Creates a log process output reader for the given process.
     * @param p the process whose output should be read.
     * @param log the log writer where output should be written.
     */
    public LogProcessOutputReader(final Process p, final LogWriter log, final String levelName, final String prefix) {
        final int level = LogWriterImpl.levelNameToCode( levelName );
        final String pre = (prefix == null) ? "" : prefix;

        class ProcessStreamReader extends Thread {
            private BufferedReader reader;
            public int linecount = 0; 
            public ProcessStreamReader(InputStream stream) {
                reader = new BufferedReader(new InputStreamReader(stream));
            }
            @Override
            public void run() {
                try {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        linecount++;
                        switch( level ) {
                          case LogWriterImpl.FINEST_LEVEL: log.finest( pre + line ); break;
                          case LogWriterImpl.FINER_LEVEL: log.finer( pre + line ); break;
                          case LogWriterImpl.FINE_LEVEL: log.fine( pre + line ); break;
                          case LogWriterImpl.CONFIG_LEVEL: log.config( pre + line ); break;
                          case LogWriterImpl.INFO_LEVEL: log.info( pre + line ); break;
                          case LogWriterImpl.WARNING_LEVEL: log.warning( pre + line ); break;
                          case LogWriterImpl.SEVERE_LEVEL: log.severe( pre + line ); break;
                          default: throw new HydraRuntimeException( "Illegal level to log at: " + levelName );
                        }
                    }
                    reader.close();
                } catch (Exception e) {
                  System.out.println( "Uncaught exception: " + e.getMessage() );
                  e.printStackTrace();
                  // @todo lises do something better with the exception
                }
            }
        };

        ProcessStreamReader stdout =
            new ProcessStreamReader(p.getInputStream());
        ProcessStreamReader stderr =
            new ProcessStreamReader(p.getErrorStream());
        stdout.start();
        stderr.start();
        try {stderr.join();} catch (Exception ignore) {}
        try {stdout.join();} catch (Exception ignore) {}
        this.exitCode = 0;
        int retryCount = 9;
        while (retryCount > 0) {
            retryCount--;
            try {
                exitCode = p.exitValue();
                break;
            } catch (IllegalThreadStateException e) {
                // due to bugs in Process we may not be able to get
                // a process's exit value.
                // We can't use Process.waitFor() because it can hang forever
                if (retryCount == 0) {
                    if (stderr.linecount > 0) {
                        // The process wrote to stderr so manufacture
                        // an error exist code
                        log.warning("Failed to get exit status and it wrote to stderr so setting exit status to 1.");
                        exitCode = 1;
                    }
                } else {
                    // We need to wait around to give a chance for
                    // the child to be reaped.See bug 19682
                    try {Thread.sleep(1000);}
                    catch (InterruptedException ignore) {}
                }
            }
        }
    }

    /**
     * Gets the process's exit status code. A code equal to 0 indicates
     * all is well.
     */
    public int getExitCode() {
      return exitCode;
    }
}
