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
package examples.dist.cacheOverflow;

import cacheOverflow.*;
import hydra.*;
import java.io.*;
import java.util.*;

/**
 * Hydra tasks that test the {@link CacheOverflow} example.
 *
 * @author David Whitlock
 *
 * @since 3.2.1
 */
public class CacheOverflowTasks {

  /**
   * A Hydra TASK that populates a <code>Region</code> by running the
   * {@link CacheOverflow} program using the {@link CacheOverflowPrms}
   * for configuration.
   */
  public static void populateTask() {
    List commandLine = new ArrayList();
    if (CacheOverflowPrms.getBackup()) {
      commandLine.add("-backup");
    }
    if (CacheOverflowPrms.isSynchronous()) {
      commandLine.add("-synchronous");
    }

    commandLine.add(String.valueOf(CacheOverflowPrms.getThreads()));
    commandLine.add(String.valueOf(CacheOverflowPrms.getArrays()));
    commandLine.add(String.valueOf(CacheOverflowPrms.getOverflowThreshold()));

    File dir1 = new File(System.getProperty("user.dir"));
    dir1 = new File(dir1, "dir1");
    dir1.mkdirs();
    commandLine.add(dir1.getAbsolutePath());

    File dir2 = new File(System.getProperty("user.dir"));
    dir2 = new File(dir2, "dir2");
    dir2.mkdirs();
    commandLine.add(dir2.getAbsolutePath());

    Log.getLogWriter().info("Running CacheOverflow with: " +
                            commandLine);

    String[] array = new String[commandLine.size()];
    CacheOverflow.main((String[]) commandLine.toArray(array));
  }

  /**
   * A Hydra CLOSETASK or ENDTASK that validates the contents of the
   * <code>Region</code> by running the {@link CacheOverflow} program
   * using the {@link CacheOverflowPrms} for configuration.
   */
  public static void validateTask() {
    List commandLine = new ArrayList();
    if (CacheOverflowPrms.getBackup()) {
      commandLine.add("-backup");
    }
    if (CacheOverflowPrms.isSynchronous()) {
      commandLine.add("-synchronous");
    }

    commandLine.add("-validate");
    commandLine.add(String.valueOf(CacheOverflowPrms.getThreads()));
    commandLine.add(String.valueOf(CacheOverflowPrms.getArrays()));
    commandLine.add(String.valueOf(CacheOverflowPrms.getOverflowThreshold()));

    File dir1 = new File(System.getProperty("user.dir"));
    dir1 = new File(dir1, "dir1");
    dir1.mkdirs();
    commandLine.add(dir1.getAbsolutePath());

    File dir2 = new File(System.getProperty("user.dir"));
    dir2 = new File(dir2, "dir2");
    dir2.mkdirs();
    commandLine.add(dir2.getAbsolutePath());

    Log.getLogWriter().info("Running CacheOverflow with: " +
                            commandLine);

    String[] array = new String[commandLine.size()];
    CacheOverflow.main((String[]) commandLine.toArray(array));
  }

}
