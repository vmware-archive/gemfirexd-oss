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

import hydra.HostHelper.OSType;
import java.util.*;

/**
 *
 *  Computes hydra runtime environment default values.
 *
 */
public class EnvHelper {

  private static final char UNIX_FILE_SEP  = '/';
  private static final char WINDOWS_FILE_SEP  = '\\';

  private static final char UNIX_PATH_SEP    = ':';
  private static final char WINDOWS_PATH_SEP = ';';

  protected static String getOptionalProperty(String name) {
    return System.getProperty(name);
  }

  protected static String getRequiredProperty(String name) {
    String property = getOptionalProperty(name);
    if ( property == null ) {
      throw new HydraConfigException("No value found for property " + name);
    }
    return property;
  }

  public static String convertHostName( String host ) {
    if ( host.equalsIgnoreCase( "localhost" ) )
      return HostHelper.getLocalHost();
    else
      return host;
  }

  /**
   * Converts the given vector of path elements to a path using an O/S-specific
   * path separator based on the host description.
   */
  protected static String asPath(Vector<String> paths, HostDescription hd) {
    if (paths == null || paths.size() == 0) {
      return null;
    }
    StringBuffer pathStr = new StringBuffer();
    for (String path : paths) {
      if (pathStr.length() != 0) {
        pathStr.append(hd.getPathSep());
      }
      pathStr.append(path);
    }
    return pathStr.toString();
  }

  protected static char getPathSep(OSType os) {
    return os == OSType.unix ? UNIX_PATH_SEP : WINDOWS_PATH_SEP;
  }

  protected static char getFileSep(OSType os) {
    return os == OSType.unix ? UNIX_FILE_SEP : WINDOWS_FILE_SEP;
  }

  /**
   *  This method is for use with post-mortem tools and requires all
   *  "environment variables" used in parsed input files to be passed
   *  in as system properties on the tool's command line.
   *  EXTRA_JTESTS is for use by Gemfire addons like the tools projects.
   *  <p>
   *  For <code>$JAVA_HOME</code> use <code>JAVA_HOME</code>.
   *  For <code>$JTESTS</code> use <code>JTESTS</code>.
   *  For <code>$EXTRA_JTESTS</code> use <code>EXTRA_JTESTS</code>.
   *  For <code>$GEMFIRE</code> use <code>gemfire.home</code>.
   *  <code>$PWD</code> automatically uses <code>user.dir</code>.
   */
  public static String expandEnvVars(String str) {
    return expandEnvVars(str, null);
  }

  /**
   * Expands the environment variables in the path vector using the host
   * description.
   */
  protected static Vector expandEnvVars(Vector<String> paths,
                                        HostDescription hd) {
    if (paths == null || paths.size() == 0) {
      return null;
    }
    Vector expandedPaths = new Vector();
    for (String path : paths) {
      String expandedPath = expandEnvVars(path, hd);
      expandedPaths.addElement(expandedPath);
    }
    return expandedPaths;
  }

  /**
   * Expands $JTESTS in the given path using the host description and version
   * for this hydra client JVM.  If the path does not exist in the versioned
   * test tree (e.g., $JTESTS/../tests651 for version 651), it uses the current
   * test tree regardless of whether the path is valid.
   * Also expands $PWD using user.dir.
   */
  public static String expandPath(String path) {
    String clientName = System.getProperty(ClientPrms.CLIENT_NAME_PROPERTY);
    ClientDescription cd = TestConfig.getInstance()
                                     .getClientDescription(clientName);
    HostDescription hd = cd.getVmDescription().getHostDescription();
    VersionDescription vd = cd.getVersionDescription();
    String jtests = hd.getTestDir();
    String userdir = System.getProperty("user.dir");
    if (vd == null) {
      return String.valueOf(path).replace("$JTESTS", jtests).replace("$PWD", userdir);
    } else { // try the versioned path first
      String vpath = jtests + hd.getFileSep() + ".." + hd.getFileSep() + ".." + hd.getFileSep()
                            + "tests" + vd.getVersion() + hd.getFileSep() + "classes";
      String vstr = String.valueOf(path).replace("$JTESTS", vpath).replace("$PWD", userdir);
      if (FileUtil.exists(vstr)) {
        return vstr;
      } else {
        return String.valueOf(path).replace("$JTESTS", jtests).replace("$PWD", userdir);
      }
    }
  }

  /**
   * Expands the environment variables in the path using the host description.
   * If the host description is null, the path is expanded using system
   * properties.
   */
  public static String expandEnvVars(String path, HostDescription hd) {
    String str = String.valueOf(path);
    if ( str.startsWith( "$JAVA_HOME" ) ) {
      String javaHome = hd == null ? System.getProperty("JAVA_HOME")
                                   : hd.getJavaHome();
      if ( javaHome == null )
        throw new HydraConfigException( "Unable to expand $JAVA_HOME" );
      str = str.replace("$JAVA_HOME", javaHome);
    }
    else if ( str.startsWith( "$JTESTS" ) ) {
      String jtests = hd == null ? System.getProperty("JTESTS")
                                 : hd.getTestDir();
      if ( jtests == null )
        throw new HydraConfigException( "Unable to expand $JTESTS" );
      str = str.replace("$JTESTS", jtests);
    }
    else if ( str.startsWith( "$EXTRA_JTESTS" ) ) {
      String extra_jtests = hd == null ? System.getProperty("EXTRA_JTESTS")
                                       : hd.getExtraTestDir();
      if ( extra_jtests == null )
        throw new HydraConfigException( "Unable to expand $EXTRA_JTESTS" );
      str = str.replace("$EXTRA_JTESTS", extra_jtests);
    }
    else if ( str.startsWith( "$HADOOP_DIST" ) ) {
      String hadoopDist = System.getProperty("HADOOP_DIST",
                                 HadoopPrms.DEFAULT_HADOOP_DIST);
      str = str.replace("$HADOOP_DIST", hadoopDist);
    }
    else if ( str.startsWith( "$GEMFIRE" ) ) {
      String gemfire = hd == null ? System.getProperty("gemfire.home")
                                  : hd.getGemFireHome();
      if ( gemfire == null )
        throw new HydraConfigException( "Unable to expand $GEMFIRE" );
      str = str.replace("$GEMFIRE", gemfire);
    }
    else if ( str.contains( "$REGRESSION_EXTRA_PATH" ) ) {
      String rdir = System.getProperty("REGRESSION_EXTRA_PATH");
      if ( rdir == null )
        throw new HydraConfigException( "Unable to expand $REGRESSION_EXTRA_PATH" );
      str = str.replace("$REGRESSION_EXTRA_PATH", rdir);
    }
    else if ( str.startsWith( "$PWD" ) ) {
      String pwd = hd == null ? System.getProperty("user.dir")
                              : TestConfig.getInstance().getMasterDescription()
                                   .getVmDescription().getHostDescription()
                                   .getUserDir();
      if ( pwd == null )
        throw new HydraConfigException( "Unable to expand $PWD" );
      str = str.replace("$PWD", pwd);
    }
    else if ( str.startsWith( "$ANT_HOME" ) ) {
      String antHome = hd == null ? System.getProperty("ANT_HOME")
                                  : hd.getAntHome();
      if ( antHome == null )
        throw new HydraConfigException( "Unable to expand $ANT_HOME" );
      str = str.replace("$ANT_HOME", antHome);
    }
    if ( str.contains( "$USER" ) ) {
      String user = System.getProperty( "user.name" );
      if ( user == null )
        throw new HydraConfigException( "Unable to expand $USER" );
      str = str.replace("$USER", user);
    }
    return str;
  }
}
