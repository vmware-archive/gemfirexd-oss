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
package dunit.impl;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.Task;
import org.apache.tools.ant.types.FileSet;
import org.apache.tools.ant.types.Path;

import hydra.HostHelper;
import hydra.HydraRuntimeException;

/**
 * <P>This class is an Ant task that creates a Hydra conf file for
 * running Distributed Unit Tests.  To use this task in a build file,
 * you must set up a <code>taskdef</code> such as:</P>
 *
 * <PRE>
 * &lt;taskdef name="dunitconfig" 
 *    classname="dunit.impl.DUnitConfigAntTask"
 *    classpath="${gemfire.home}/lib/tests.jar" /&gt;
 * </PRE>
 *
 * <HR>
 *
 * <h2><a name="DUnitConfig">DUnitConfig</a></h2>
 *
 * <h3>Description</h3>
 *
 * <P>Generates a Hydra config file for running Distributed Unit
 * Tests.</P> 
 *
 * <P>The location of the generated file is specified with the
 * <I>file</I> attribute.  The tests classes are specified with the
 * nested <I>tests</I> parameter (a <A href=
 * "http://jakarta.apache.org/ant/manual/CoreTypes/fileset.html">FileSet</A>).</P>
 *
 * <h3>Parameters</h3>
 *
 * <table border="1" cellpadding="2" cellspacing="0">
 *  <tr>
 *    <td valign="top"><b>Attribute</b></td>
 *    <td valign="top"><b>Description</b></td>
 *    <td align="center" valign="top"><b>Required</b></td>
 *  </tr>
 *  <tr>
 *    <td valign="top">confFile</td>
 *    <td valign="top">Specifies the name of the Hydra configuration
 *    file to be generated.</td>
 *    <td align="center" valign="top">Yes</td>
 *  </tr>
 *  <tr>
 *    <td valign="top">btFile</td>
 *    <td valign="top">Specifies the name of the battery test
 *    file to be generated.</td>
 *    <td align="center" valign="top">Yes</td>
 *  </tr>
 *  <tr>
 *    <td valign="top">tests</td>
 *    <td valign="top">Specifies the test classes to be run.</td>
 *    <td align="center" valign="top">Yes</td>
 *  </tr>
 *  <tr>
 *    <td valign="top">dunitSites</td>
 *    <td valign="top">Specifies the number of DUnit Environments to create.
 *    This is used allow parrralle dunit execution.</td>
 *    <td align="center" valign="top">Yes</td>
 *  </tr>
 *  <tr>
 *    <td valign="top">systemCount</td>
 *    <td valign="top">Specifies the number of GemFire systems that
 *        the Distributed Unit Tests should be run with.</td>
 *    <td align="center" valign="top">No</td>
 *  </tr>
 *  <tr>
 *    <td valign="top">includeFile</td>
 *    <td valign="top">Specifies the Hydra include file that contains
 *        the GemFire system configuration for the distributed unit
 *        tests. 
 *    <td align="center" valign="top">No</td>
 *  </tr>
 *  <tr>
 *    <td valign="top">logLevel</td>
 *    <td valign="top">Specifies the log level of the distribution
 *        managers started by the DUnit test.</td>
 *    <td align="center" valign="top">No</td>
 *  </tr>
 *  <tr>
 *    <td valign="top">verbose</td>
 *    <td valign="top">Should the distribution managers log
 *         information about the messages that are sent/received
 *         (<TT>true</TT> or <TT>false</TT>)?  See <code>
 *         DistributionManager#VERBOSE</code> </td>
 *    <td align="center" valign="top">No</td>
 *  </tr>
 *  <tr>
 *    <td valign="top">extraLocatorClasspath</td>
 *    <td valign="top">Specifies extra files that should be on the
 *    hydra Locator's class path.  See {@link
 *    hydra.VmPrms#extraClassPaths}. This is a <a
 *    href="http://jakarta.apache.org/ant/manual/using.html#path">path-like
 *    structure</a> and may be nested.</td>
 *    <td align="center" valign="top">No</td>
 *  </tr>
 *  <tr>
 *    <td valign="top">extraControllerClasspath</td>
 *    <td valign="top">Specifies extra files that should be on the
 *    hydra Controller's class path.  See {@link
 *    hydra.VmPrms#extraClassPaths}. This is a <a
 *    href="http://jakarta.apache.org/ant/manual/using.html#path">path-like
 *    structure</a> and may be nested.</td>
 *    <td align="center" valign="top">No</td>
 *  </tr>
 *  <tr>
 *    <td valign="top">extraClientClasspath</td>
 *    <td valign="top">Specifies extra files that should be on the
 *    hydra clients' class path.  See {@link
 *    hydra.VmPrms#extraClassPaths}. This is a <a
 *    href="http://jakarta.apache.org/ant/manual/using.html#path">path-like
 *    structure</a> and may be nested.</td>
 *    <td align="center" valign="top">No</td>
 *  </tr>
 *  <tr>
 *    <td valign="top">param</td>
 *    <td valign="top">Specifies an additional hydra parameter to be
 *     included in the generated <code>.conf</code> files.  See {@link
 *     hydra.BasePrms}.</td>
 *    <td align="center" valign="top">No</td>
 *  </tr>
 *  <tr>
 *    <td valign="top">jprobeClient</td>
 *    <td valign="top">Nested element that specified how JProbe should
 *    be run on client VMs.  See {@link hydra.JProbePrms}.</td>
 *    <td align="center" valign="top">No</td>
 *  </tr>
 *  <tr>
 *    <td valign="top">debugJavaGroups</td>
 *    <td valign="top">Should the distribution managers log
 *         copious debug information about Jgroups
 *         (<TT>true</TT> or <TT>false</TT>)?  See
 *         <code>DistributionManager#VERBOSE</code>. </td>
 *    <td align="center" valign="top">No</td>
 *  </tr>
 * </table>
 *
 *
 * <h3>Examples</h3>
 *
 * <PRE>
 * &lt;dunitconfig confFile=&quot;dunit.conf&quot; btFile=&quot;dunit.bt&quot; 
 *                 systemCount=&quot;2&quot;&gt;
 *   &lt;extraClientClasspath&gt;
 *      &lt;fileset dir="${other.jars}" includes="*.jar"/&gt;
 *   &lt;/extraClientClasspath&gt;
 *   &lt;param name="mytest.TestPrms-maxCount" value="1000"/&gt;
 *   &lt;jprobeClient/&gt;
 *   &lt;tests dir=&quot;${tests.src.dir}&quot;&gt;
 *     &lt;include name=&quot;com/tests/distributed/**&quot;/&gt;
 *   &lt;/tests&gt;
 * &lt;/dunitconfig&gt;
 * </PRE>
 *
 * @author David Whitlock
 * @author John Blum
 */
@SuppressWarnings("unused")
public class DUnitConfigAntTask extends Task {

  /** The generated configuration file */
  private File configFile;

  /** The generated battery test config file */
  private File btFile;

  /** The number of DUnit Sites, each site has 1 controller, 1 locator, and 4 worker VMs */
  private int dunitSites = 0;

  /** FileSet for test classes */
  private FileSet tests;

  /** The number of GemFire systems to configure for */
  private int systemCount = 0;

  /** The name of a hydra include file that contains all of the system
   * settings */
  private String includeFile = null;

  /** The log level for the distribution managers */
  private String logLevel = null;

  /** Should the distribution managers be in VERBOSE mode? */
  private String verboseString = null;

  /** Should Jgroups be in Debug mode? */
  private String debugJavaGroupsString = null;

  /** Path to extra classes on the Locator's classpath */
  private Path extraLocatorClasspath;

  /** Path to extra classes on the Controller's classpath */
  private Path extraControllerClasspath;

  /** Path to the extra classes on the clients' classpath */
  private Path extraClientClasspath;

  /** The additional params to be added to the generated conf files */
  private final List<Param> params = new ArrayList<Param>();

  /** The JProbe for the GemFire client VM */
  private JProbe clientJProbe;

  private String[] extraVMArgs = new String[0];
  
  /** whether IPv6 should be used in the test run, if available */
  private boolean useIPv6;

  /**
   * Executes this task.  Creates the Hydra config file for DUnit and populates with test classes to run.
   */
  @Override
  public void execute() throws BuildException {
    validateConfigurationSettings();

    final Project project = this.getProject();

    boolean configFilePathCreated = this.configFile.getParentFile().mkdirs();
    boolean btFilePathCreated = this.btFile.getParentFile().mkdirs();

    String[] filenames = this.tests.getDirectoryScanner(project).getIncludedFiles();
    List<String> testClasses = new ArrayList<String>();

    if (filenames.length == 0) {
      throw new BuildException("Didn't find any distributed unit tests");
    }

    for (String filename : filenames) {
      filename = filename.substring(0, filename.lastIndexOf('.'));
      filename = filename.replace(File.separatorChar, '.');
      log("Will generate config for class " + filename, Project.MSG_VERBOSE);
      testClasses.add(filename);
    }

    // Because the XML parsing code relies on the fact that certain
    // configuration files can be located from the current context
    // loader, we set the context class loader to be the class loader
    // with all of the GemFire classes on it.
    final ClassLoader originalContextClassLoader = Thread.currentThread().getContextClassLoader();
    //log("Current context class loader: " + originalContextClassLoader, Project.MSG_VERBOSE);

    Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
    //log("Setting the current context class loader to " + Thread.currentThread().getContextClassLoader(), Project.MSG_VERBOSE);

    try {
      log("Generating DUnit configuration file " + configFile);

      PrintWriter writer;

      try {
        writer = new PrintWriter(new FileWriter(this.configFile), true);
      } catch (IOException ex) {
        throw new BuildException("While writing to " + this.configFile + ": " + ex);
      }

      writer.println("// This file was generated by " + this.getClass().getName());
      writer.println("// on " + (new Date()));
      writer.println("");
      writer.println("INCLUDE " + this.includeFile + ";");
      writer.println();
      writer.println("hydra.Prms-manageLocatorAgents=false;");

      if (this.logLevel != null) {
        writer.println("hydra.GemFirePrms-logLevel = " + logLevel + ";");
      }

      List<String> parameters = new ArrayList<String>();

      parameters.add("-Dp2p.joinTimeout=9000");

      if (this.verboseString != null && Boolean.valueOf(this.verboseString)) {
        parameters.add("-DDistributionManager.VERBOSE=true");
      }

      if (this.debugJavaGroupsString != null && Boolean.valueOf(this.debugJavaGroupsString)) {
        parameters.add("-DJGroups.DEBUG=true");
      }

      if (!parameters.isEmpty() || this.extraVMArgs.length > 0) {
        writer.println("\nhydra.VmPrms-extraVMArgs = ");

        for (String extraVmArg : extraVMArgs) {
          writer.println("  \"" + extraVmArg + "\"");
        }

        for (String parameter : parameters) {
          writer.println("  \"" + parameter + "\"");
        }

        writer.println("  ;");
      }
      
      if (this.useIPv6) {
        System.setProperty(hydra.Prms.USE_IPV6_PROPERTY, "true");
        String address;
        try {
          address = HostHelper.getHostAddress();
        }
        catch (HydraRuntimeException e) {
          System.err.println("ERROR: " + e.getMessage());
          throw new Error(e.getMessage());
        }
        writer.println("\nhydra.Prms-useIPv6 = true;");
        writer.println("\nhydra.VmPrms-extraVMArgs += \"-Dgemfire.bind-address=" + address + "\";");
        writer.println("\nhydra.VmPrms-extraVMArgs += \"-Dgemfire.server-bind-address=" + address + "\";");
        writer.println("\nhydra.VmPrms-extraVMArgs += \"-Dgemfire.mcast-address=" + "FF38::1234" + "\";");
        writer.println("\nhydra.VmPrms-extraVMArgs += \"-Dp2p.joinTimeout=9000;");
        writer.println("\nhydra.GemFirePrms-extraLocatorVMArgs += \"-Dgemfire.bind-address=" + address + "\";");
      }

      final StringBuilder extraClasspaths = new StringBuilder();

      /*
      if (this.extraLocatorClasspath != null) {
        appendToExtraClasspaths(extraClasspaths, extraLocatorClasspath);
      }

      if (this.extraControllerClasspath != null) {
        appendToExtraClasspaths(extraClasspaths, extraControllerClasspath);
      }

      if (this.extraClientClasspath != null) {
        appendToExtraClasspaths(extraClasspaths, extraClientClasspath, 2);
      }
      */

      if (this.extraClientClasspath != null) {
        extraClasspaths.append("\"").append(this.extraClientClasspath).append("\"");
      }

      if (extraClasspaths.length() != 0) {
        // Hydra expects backslashes to be escaped in Windows (bug 28445)
        if (File.separatorChar == '\\') {
          for (int index = 0; index < extraClasspaths.length(); index++) {
            if (extraClasspaths.charAt(index) == '\\') {
              extraClasspaths.insert(index, '\\');
              index++;
            }
          }
        }
        writer.println("\nhydra.VmPrms-extraClassPaths=" + extraClasspaths + ";");
      }

      JProbe.writeToConf(this.clientJProbe, writer);

      writer.println();

      for (Param param : this.params) {
        param.writeToConf(writer);
      }

      writer.println();

      for (String testClass : testClasses) {
        writer.println("UNITTEST    testClass = " + testClass + ";");
      }

      writer.flush();
      writer.close();

      try {
        writer = new PrintWriter(new FileWriter(this.btFile), true);
      } catch (IOException ex) {
        throw new BuildException("While writing to " + this.btFile + ": " + ex);
      }

      writer.println("// This file was generated by " + this.getClass().getName());
      writer.println("// on " + (new Date()));
      writer.println("");
      writer.println(this.configFile.getAbsolutePath() + " dunitSites=" + dunitSites);
      writer.flush();
      writer.close();
    } finally {
      Thread.currentThread().setContextClassLoader(originalContextClassLoader);
    }
  }

  private void validateConfigurationSettings() {
    if (this.configFile == null) {
      throw new BuildException("No configuration file specified");
    } else if (this.btFile == null) {
      throw new BuildException("No battery test file specified");
    } else if (systemCount <= 0) {
      throw new BuildException("No system count specified");
    } else if (includeFile == null) {
      throw new BuildException("No include file specified");
    } else if (this.tests == null) {
      throw new BuildException("No tests specified");
    } else if (this.dunitSites == 0) {
      throw new BuildException("You must specify the number of dunitSites");
    }
  }

  private void appendToExtraClasspaths(final StringBuilder extraClasspaths, final Path classpath) {
    appendToExtraClasspaths(extraClasspaths, classpath, 1);
  }

  private void appendToExtraClasspaths(final StringBuilder extraClasspaths, final Path classpath, final int multiplier) {
    for (int index = 0, total = (getDunitSites() * multiplier); index < total; index++) {
      extraClasspaths.append(extraClasspaths.length() > 0 ? ", \"" : "\"").append(classpath.toString()).append("\"");
    }
  }

  /**
   * Sets the file that will contain the a battery test configuration (.bt)
   */
  public void setBtfile(File btFile) {
    this.btFile = btFile;
  }

  /**
   * Sets the file that will contain the Hydra configuration (.conf)
   */
  public void setConffile(File configFile) {
    this.configFile = configFile;
  }

  /**
   * Gets the number of dunitSites that will be created.
   * <p/>
   * @return an integer value indicating the number of dunit sites that will be created to run the DUnit test suite
   * in parallel.
   * @see #setDunitSites(int)
   */
  public int getDunitSites() {
    return this.dunitSites;
  }

  /**
   * Sets the number of dunitSites to create, this is passed into the conf
   */
  public void setDunitSites(int sites) {
    this.dunitSites = sites;
  }

  /**
   * Sets the number of GemFire systems to configure for.  This number
   * must agree with the configuration specified in the include file.
   */
  public void setSystemcount(int count) {
    this.systemCount = count;
  }

  /**
   * Sets the name of the Hydra include file that contains the GemFire
   * system configurations.
   */
  public void setIncludefile(String includeFileName) {
    this.includeFile = includeFileName;
  }

  public void setLoglevel(String logLevel) {
    this.logLevel = logLevel;
  }

  public void setVerbose(String verbose) {
    this.verboseString = verbose;
  }
  
  public void setExtraVMArgs(String arg) {
    this.extraVMArgs = arg.split("[\\s]+");
  }
  
  public void setUseIPv6(String useIt) {
    if (useIt.equals("true")) {
      this.useIPv6 = true;
    }
  }

  public void setDebugjavagroups(String debug) {
    this.debugJavaGroupsString = debug;
  }

  /**
   * Creates a nested <code>extraClientClasspath</code> path
   */
  public Path createExtraClientClasspath() {
    if (this.extraClientClasspath == null) {
      this.extraClientClasspath = new Path(this.getProject());
    }

    return this.extraClientClasspath.createPath();
  }

  /**
   * Creates a nested <code>extraControllerClasspath</code> path
   */
  public Path createExtraControllerClasspath() {
    if (this.extraControllerClasspath == null) {
      this.extraControllerClasspath = new Path(this.getProject());
    }

    return this.extraControllerClasspath.createPath();
  }

  /**
   * Creates a nested <code>extraLocatorClasspath</code> path
   */
  public Path createExtraLocatorClasspath() {
    if (this.extraLocatorClasspath == null) {
      this.extraLocatorClasspath = new Path(this.getProject());
    }

    return this.extraLocatorClasspath.createPath();
  }

  /**
   * Returns the <code>JProbe</code> for the GemFire Client VM
   */
  public JProbe createJprobeclient() {
    JProbe jprobe = new JProbe();
    this.clientJProbe = jprobe;
    return jprobe;
  }

  /**
   * Creates a nested <code>param</code>
   */
  public Param createParam() {
    Param param = new Param();
    this.params.add(param);
    return param;
  }

  /**
   * Sets the test classes for which to generate config information
   */
  public FileSet createTests() {
    if (this.tests == null) {
      this.tests = new FileSet();
    }

    return this.tests;
  }

  //////////////////////  Inner Classes  //////////////////////

  /**
   * Holds the name and value of a {@link hydra.BasePrms hydra param}
   * to be added to the generated <code>.conf</code> file.
   */
  public static class Param {

    /** The name of the param */
    private String name;

    /** The value of the param */
    private String value;

    ////////////////////  Instance Methods  ////////////////////

    /**
     * Sets the name of this <code>Param</code>
     */
    public void setName(String name) {
      this.name = name;
    }

    /**
     * Sets the value of this <code>Param</code>
     */
    public void setValue(String value) {
      this.value = value;
    }

    /**
     * Dumps a description of this <code>Param</code> to a Hydra conf file.
     * <p/>
     * @throws BuildException if this <code>Param</code> is not correctly configured or specified.
     */
    void writeToConf(PrintWriter pw) throws BuildException {
      if (name == null) {
        String s = "Missing param name";
        throw new BuildException(s);
      }
      
      if (value == null) {
        String s = "Missing value for param \"" + name + "\"";
        throw new BuildException(s);
      }

      pw.println(name + " = " + value + ";");
    }

  }

  /**
   * Represents a JProbe configuration.
   */
  public static class JProbe {
    
    /**
     * Creates a new <code>JProbe</code>
     */
    JProbe() {
    }

    /**
     * Writes the configuration for the client
     * <code>JProbe</code> to the hydra configuration file.
     */
    static void writeToConf(JProbe client, PrintWriter writer) {
      if (client == null) {
        return;
      }

      writer.println();
      writer.println("hydra.JProbePrms-names = clientJProbe;");
      writer.println("hydra.JProbePrms-function = performance;");
      writer.println("hydra.JProbePrms-recordFromStart = true;");
      writer.println("hydra.JProbePrms-finalSnapshot   = true;");
      writer.println("hydra.JProbePrms-monitor         = false;");

      writer.println("hydra.JProbePrms-filters =");
      for (String filter : client.getFilters()) {
        writer.println("  " + filter);
      }
      writer.println(";");

      writer.println("hydra.ClientPrms-jprobeNames = clientJProbe;");
      writer.println();
    }

    /**
     * Returns the "filters" for this JProbe
     */
    List<String> getFilters() {
      // Right now, profile all methods
      return Arrays.asList("*.*.*():method");
    }
  }

}
