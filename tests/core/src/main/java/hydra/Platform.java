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

import com.gemstone.gemfire.internal.ProcessOutputReader;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Random;

/**
 * This abstract class defines the methods required for platform-specific
 * operations.
 */
public abstract class Platform {

  // singleton instance of the localhost platform
  private static final Platform platform =
    System.getProperty("os.name").contains("Windows") ? new WindowsPlatform()
                                                      : new UnixPlatform();

  // currently PasswordAuthentication=no is not honored due to sshd config
  // required by IS to be able to get into machines under some circumstances
  protected static final String SSH =
    "ssh -n -x -o PasswordAuthentication=no -o StrictHostKeyChecking=no";

  private static final Random rng = new Random();
  private static final int NUM_TRIES = 100;

  protected static Platform getInstance() {
    return platform;
  }

  protected String fgexec(String command, int maxWaitSec) {
    final String cmd = command;
    Runnable op = new Runnable() {
      public void run() {
        Process process = null;
        try {
          process = Runtime.getRuntime().exec(cmd);
        } catch (IOException e) {
          throw new HydraRuntimeException("Failed to create process", e);
        }
        ProcessOutputReader reader = new ProcessOutputReader(process);
        String output = reader.getOutput();
        if (output != null) {
          output = output.trim();
        }
        int exitCode = reader.getExitCode();
        if (exitCode != 0) {
          String s = output + "\n\nCommand failed with exit code: " + exitCode;
          throw new HydraRuntimeException(s);
        }
        ((ExceptionThread)Thread.currentThread()).setResult(output);
      }
    };
    return (String)ResourceOperation.doOp(op, "fgexec", cmd, "",
                                          maxWaitSec*1000, false);
  }

  protected String fgexec(String command, final String[] envp, int maxWaitSec) {
    final String cmd = command;
    Runnable op = new Runnable() {
      public void run() {
        Process process = null;
        try {
          process = Runtime.getRuntime().exec(cmd, envp);
        } catch (IOException e) {
          throw new HydraRuntimeException("Failed to create process", e);
        }
        ProcessOutputReader reader = new ProcessOutputReader(process);
        String output = reader.getOutput();
        if (output != null) {
          output = output.trim();
        }
        int exitCode = reader.getExitCode();
        if (exitCode != 0) {
          String s = output + "\n\nCommand failed with exit code: " + exitCode;
          throw new HydraRuntimeException(s);
        }
        ((ExceptionThread)Thread.currentThread()).setResult(output);
      }
    };
    return (String)ResourceOperation.doOp(op, "fgexec", cmd, "",
                                          maxWaitSec*1000, false);
  }

  protected String fgexec(String[] command, int maxWaitSec) {
    final String[] cmd = command;
    final String cmdstr = getString(command);
    Runnable op = new Runnable() {
      public void run() {
        Process process = null;
        try {
          process = Runtime.getRuntime().exec(cmd);
        } catch (IOException e) {
          throw new HydraRuntimeException("Failed to create process", e);
        }
        ProcessOutputReader reader = new ProcessOutputReader(process);
        String output = reader.getOutput();
        if (output != null) {
          output = output.trim();
        }
        int exitCode = reader.getExitCode();
        if (exitCode != 0) {
          String s = output + "\n\nCommand failed with exit code: " + exitCode;
          throw new HydraRuntimeException(s);
        }
        ((ExceptionThread)Thread.currentThread()).setResult(output);
      }
    };
    return (String)ResourceOperation.doOp(op, "fgexec", cmdstr, "",
                                          maxWaitSec*1000, false);
  }

  protected static String getString(String[] s) {
    StringBuilder str = new StringBuilder();
    for (int i = 0; i < s.length; i++) {
      str.append(s[i] + " ");
    }
    return str.toString().trim();
  }

  /**
   *
   * Starts a foreground process, waits for it to complete,
   * and returns the exit status.
   * @param cmdArray the command to run.
   * @return the exit status of the command
   * @throws HydraRuntimeException if a process cannot be created or 
   * its exit code is non-zero.
   *
   */
  protected int fgexec(String[] cmdArray, String[] output, File workdir)
  throws IOException {
    Process process = Runtime.getRuntime().exec(cmdArray, null, workdir);
    ProcessOutputReader reader = new ProcessOutputReader( process );
    output[0] = reader.getOutput();
    return reader.getExitCode();
  }

  /**
   * Starts execution of the specified string command in a separate
   * detached process in the specified
   * working directory writing output to the specified log file.
   *
   * @param command a specified system command.
   * @param workdir the current directory of the created process; null
   * causes working directory to default to the current directory.
   * @param logfile the file the created process will write
   * stdout and stderr to; null causes a default log file name
   * to be used.
   * @return the process id of the created process
   * @exception  SecurityException  if the current thread cannot create a
   *               subprocess.
   */
  protected int bgexec(String command, File workdir, File logfile)
  throws IOException {
     String cmdarray[] = command.split(" +");
     return bgexec(cmdarray, workdir, logfile);
  }

  /**
   * Starts execution of the specified command and arguments in a separate
   * detached process in the specified
   * working directory writing output to the specified log file.
   * <p>
   * If there is a security manager, its <code>checkExec</code> method
   * is called with the first component of the array
   * <code>cmdarray</code> as its argument. This may result in a security
   * exception.
   * <p>
   * Given an array of strings <code>cmdarray</code>, representing the
   * tokens of a command line,
   * this method creates a new process in which to execute
   * the specified command.
   *
   * @param cmdarray   array containing the command to call and its arguments.
   * @param workdir the current directory of the created process; null
   * causes working directory to default to the current directory.
   * @param logfile the file the created process will write
   * stdout and stderr to; null causes a default log file name
   * to be used.
   * @return the process id of the created process
   * @exception  SecurityException  if the current thread cannot create a
   *               subprocess.
   * @see     java.lang.SecurityException
   * @see     java.lang.SecurityManager#checkExec(java.lang.String)
   */
  protected int bgexec(	        String[] cmdarray, 
				File workdir, 
				File logfile)
    throws IOException {
     if (workdir != null && !workdir.isDirectory()) {
      String curDir = new File("").getAbsolutePath();
      System.out.println("WARNING: \"" + workdir + "\" is not a directory. Defaulting to current directory \"" + curDir + "\".");
      workdir = null;
    }
    if (workdir == null) {
      workdir = new File("").getAbsoluteFile();
    }
    if (logfile == null) {
      logfile = createTempFile("bgexec", ".log", workdir);
    }
    if (!logfile.isAbsolute()) {
      // put it in the working directory
      logfile = new File(workdir, logfile.getPath());
    }
    // fix for bug 24575
    if (logfile.exists()) {
      // it already exists so make sure its a file and can be written
      if (!logfile.isFile()) {
        throw new IOException("The log file \"" +
        logfile.getPath() + "\" was not a normal file.");
      }
      if (!logfile.canWrite()) {
        throw new IOException("Need write access for the log file \"" + logfile.getPath() + "\".");
      } 
    } else {
      try {
        logfile.createNewFile();
      } catch (IOException io) {
        throw new IOException("Could not create log file \"" +
                              logfile.getPath() + "\" because: " +
                              io.getMessage() + ".");
      }
    }
    int pid = bgexecInternal(cmdarray, workdir.getPath(), logfile.getPath()); 
    if (pid <= 0) {
      throw new IOException("Command failed with invalid PID: " + pid);
    }
    return pid;
  }

  private File createTempFile(String prefix, String suffix, File dir)
  throws IOException {
    if (dir == null) {
      throw new IllegalArgumentException("dir cannot be null");
    }
    if (prefix == null) {
      throw new IllegalArgumentException("prefix cannot be null");
    }
    if (suffix == null) {
      throw new IllegalArgumentException("suffix cannot be null");
    }
    for (int i = 0; i < NUM_TRIES; i++) {
      int id = 10000 + rng.nextInt(10000);
      String fn = dir.getAbsolutePath() + File.separator + prefix + id + suffix;
      File f = new File(fn);
      try {
        if (f.createNewFile()) {
          return f;
        }
      } catch (IOException io) {
        throw new IOException("Could not create temp file \"" +
                              f.getPath() + "\" because: " +
                              io.getMessage() + ".");
      }
    }
    throw new IOException("Could not create " + prefix + " temp file in "
                         + dir + " in " + NUM_TRIES + " tries");
  }

    /**
     * Starts a background command writing its stdout and stderr to
     * the specified log file.
     *
     * @param cmdarray An array of strings that specify the command to run.
     * The first element must be the executable.
     * Each additional command line argument should have its own entry
     *  in the array.
     * @param workdir the current directory of the created process
     * @param logfile the file the created process will write
     * stdout and stderr to.
     * @return the process id of the created process; -1 on failure
     * @exception IOException if a child process could not be created.
     */
    abstract protected int bgexecInternal(String[] cmdarray,
			      String workdir,
			      String logfile)
      throws IOException;

   /**
    * Checks to make sure that we are operating on a valid process id.
    * Sending signals to processes with <code>pid</code> 0 or -1 can
    * have unintended consequences.
    *
    * @throws IllegalArgumentException
    *         If <code>pid</code> is not positive
    *
    * @since 4.0
    */
   protected void checkPid(int pid) {
     if (pid <= 0) {
       String s = "Should not send a signal to pid " + pid;
       throw new IllegalArgumentException(s);
     }
   }

  /**
   * Find out if a process exists.
   * @param pid the id of the process to check for
   * @return true if the process exists; false if it does not.
   */
  abstract protected boolean exists(int pid);

  /**
   * @param pid the id of the process to shutdown
   */
  abstract protected void shutdown(int pid);

  /**
   * Kills a process.
   * @param pid the id of the process to shutdown
   */
  abstract protected void kill(int pid);

  /**
   * Tells a process to print its stacks to its standard output
   * @param pid the id of the process that will print its stacks
   */
  abstract protected void printStacks(int pid);

  /**
   * Tells a process to dump heap
   * @param pid the id of the process that will print its stacks
   */
  abstract protected void dumpHeap(int pid, String userDir, String options);

  /**
   * Returns network statistics.
   */
  abstract protected String getNetworkStatistics(int maxWaitSec);

  abstract protected String getShutdownCommand(int pid);
  abstract protected String getKillCommand(int pid);
  abstract protected String getDumpLocksCommand(int pid);
  abstract protected String getPrintStacksCommand(int pid);
  abstract protected String[] getDumpHeapCommand(int pid, String userDir, String options);
  abstract protected String getNetcontrolCommand(String target, int op);

  /**
   * Uses NTP to synchronize the local clock with the network time server.
   */
  abstract protected void restartNTP();

  /**
   * Returns the file extension for shell scripts.
   */
  abstract protected String getFileExtension();

  /**
   * Returns the header for shell scripts.
   */
  abstract protected String getScriptHeader();

  /**
   * Returns the comment prefix for shell scripts.
   */
  abstract protected String getCommentPrefix();

  /**
   * Gives user execute permission to the file.
   */
  abstract protected void setExecutePermission(String fn);

  /**
   * Gives universal read permission to all files in the directory.
   */
  abstract protected void setReadPermission(String dir);

  /**
   * Returns the process status.
   */
  abstract protected String getProcessStatus(int maxWaitSec);

  /**
   * Returns the memory status.
   */
  abstract protected String getMemoryStatus(int maxWaitSec);

  protected String getStackTrace(Throwable t) {
    StringWriter sw = new StringWriter();
    t.printStackTrace(new PrintWriter(sw, true));
    return sw.toString();
  }
}
