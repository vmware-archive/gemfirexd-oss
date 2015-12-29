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

import hydra.BasePrms;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * A class used to store keys for Hadoop cluster configuration settings. The
 * settings are used to create instances of {@link HadoopDescription}.
 * A given hydra test will typically use a single cluster, though some tests
 * could possibly use a separate cluster to store data for validation.
 * <p>
 * The number of description instances is gated by {@link #names}.  For other
 * parameters, if fewer values than names are given, the remaining instances
 * will use the last value in the list.  See $JTESTS/hydra/hydra.txt for more
 * details.
 * <p>
 * Unused parameters default to null, except where noted.  This uses the
 * product default, except where noted.
 * <p>
 * Values, fields, and subfields of a parameter can be set to {@link #DEFAULT},
 * except where noted.  This uses the product default, except where noted.
 * <p>
 * Values, fields, and subfields can be set to {@link #NONE} where noted, with
 * the documented effect.
 * <p>
 * Values, fields, and subfields of a parameter can use oneof, range, or robing
 * except where noted, but each description created will use a fixed value
 * chosen at test configuration time.  Use as a task attribute is illegal.
 * <p>
 * Subfields are order-dependent, as stated in the javadocs for parameters that
 * use them.
 * <p>
 * Example:
 * <code>
 *   hydra.HadoopPrms-names = hdfs;
 *
 *   hydra.HadoopPrms-nameNodeHosts = shep;
 *   hydra.HadoopPrms-nameNodeLogDrives = a;
 *   hydra.HadoopPrms-nameNodeDataDrives = a;
 *
 *   hydra.HadoopPrms-dataNodeHosts = larry moe curly;
 *   hydra.HadoopPrms-dataNodeLogDrives = a b c;
 *   hydra.HadoopPrms-dataNodeDataDrives = b:c a:c a:b;
 * </code>
 * <p>
 * For best performance, put the NameNode on its own host and DataNode logs
 * on different drives from the data.
 */
public class HadoopPrms extends BasePrms {

  static {
    setValues(HadoopPrms.class); // initialize parameters
  }

  public static enum NodeType {DataNode, NameNode, ResourceManager, NodeManager;}

  private static final String HBASE_VERSION = "0.94.4-gemfire-r45047";
  private static final String SLF4J_VERSION = "1.7.5";

  public static final String PHD3000_138 = "/export/gcm/where/java/hadoop/hadoop-2.2.0-gphd-3.0.0.0-138";
  public static final String PHD3000_138_OPT = "/opt/hadoop-2.2.0-gphd-3.0.0.0-138";
  public static final String PHD3000_138_VERSION = "2.2.0-gphd-3.0.0.0";
  protected static final String PHD3000_138_Jars =
    PHD3000_138 + "/share/hadoop/common/lib/commons-cli-1.2.jar " +
    PHD3000_138 + "/share/hadoop/common/lib/commons-codec-1.4.jar " +
    PHD3000_138 + "/share/hadoop/common/lib/commons-configuration-1.6.jar " +
    PHD3000_138 + "/share/hadoop/common/lib/commons-io-2.1.jar " +
    PHD3000_138 + "/share/hadoop/common/lib/commons-lang-2.5.jar " +
    PHD3000_138 + "/share/hadoop/common/lib/commons-logging-1.1.1.jar " +
    PHD3000_138 + "/share/hadoop/common/lib/guava-11.0.2.jar " +
    PHD3000_138 + "/share/hadoop/common/lib/jsr305-1.3.9.jar " +
    PHD3000_138 + "/share/hadoop/common/lib/log4j-1.2.17.jar " +
    PHD3000_138 + "/share/hadoop/common/lib/slf4j-api-1.7.5.jar " +
    "/export/gcm/where/java/slf4j/jdk14/" + SLF4J_VERSION + "/slf4j-jdk14-" + SLF4J_VERSION + ".jar " +
    PHD3000_138 + "/share/hadoop/common/lib/hadoop-annotations-" + PHD3000_138_VERSION + ".jar " +
    PHD3000_138 + "/share/hadoop/common/lib/hadoop-auth-" + PHD3000_138_VERSION + ".jar " +
    PHD3000_138 + "/share/hadoop/common/hadoop-common-" + PHD3000_138_VERSION + ".jar " +
    PHD3000_138 + "/share/hadoop/hdfs/hadoop-hdfs-" + PHD3000_138_VERSION + ".jar " +
    "/export/gcm/where/java/hbase/" + HBASE_VERSION + "/hbase-" + HBASE_VERSION + ".jar " +
    PHD3000_138 + "/share/hadoop/common/lib/protobuf-java-2.5.0.jar"
  ;

  public static final String PHD3100_175 = "/export/gcm/where/java/hadoop/hadoop-2.2.0-gphd-3.1.0.0-175";
  public static final String PHD3100_175_OPT = "/opt/hadoop-2.2.0-gphd-3.1.0.0-175";
  public static final String PHD3100_175_VERSION = "2.2.0-gphd-3.1.0.0";
  protected static final String PHD3100_175_Jars =
    PHD3100_175 + "/share/hadoop/common/lib/commons-cli-1.2.jar " +
    PHD3100_175 + "/share/hadoop/common/lib/commons-codec-1.4.jar " +
    PHD3100_175 + "/share/hadoop/common/lib/commons-configuration-1.6.jar " +
    PHD3100_175 + "/share/hadoop/common/lib/commons-io-2.1.jar " +
    PHD3100_175 + "/share/hadoop/common/lib/commons-lang-2.5.jar " +
    PHD3100_175 + "/share/hadoop/common/lib/commons-logging-1.1.1.jar " +
    PHD3100_175 + "/share/hadoop/common/lib/guava-11.0.2.jar " +
    PHD3100_175 + "/share/hadoop/common/lib/jsr305-1.3.9.jar " +
    PHD3100_175 + "/share/hadoop/common/lib/log4j-1.2.17.jar " +
    PHD3100_175 + "/share/hadoop/common/lib/slf4j-api-1.7.5.jar " +
    "/export/gcm/where/java/slf4j/jdk14/" + SLF4J_VERSION + "/slf4j-jdk14-" + SLF4J_VERSION + ".jar " +
    PHD3100_175 + "/share/hadoop/common/lib/hadoop-annotations-" + PHD3100_175_VERSION + ".jar " +
    PHD3100_175 + "/share/hadoop/common/lib/hadoop-auth-" + PHD3100_175_VERSION + ".jar " +
    PHD3100_175 + "/share/hadoop/common/hadoop-common-" + PHD3100_175_VERSION + ".jar " +
    PHD3100_175 + "/share/hadoop/hdfs/hadoop-hdfs-" + PHD3100_175_VERSION + ".jar " +
    "/export/gcm/where/java/hbase/" + HBASE_VERSION + "/hbase-" + HBASE_VERSION + ".jar " +
    PHD3100_175 + "/share/hadoop/common/lib/protobuf-java-2.5.0.jar"
  ;

  public static final String PHD3200_54 = "/export/gcm/where/java/hadoop/hadoop-2.4.1-gphd-3.2.0.0-54";
  public static final String PHD3200_54_OPT = "/opt/hadoop-2.4.1-gphd-3.2.0.0-54";
  public static final String PHD3200_54_VERSION = "2.4.1-gphd-3.2.0.0";
  protected static final String PHD3200_54_Jars =
    PHD3200_54 + "/share/hadoop/common/lib/commons-cli-1.2.jar " +
    PHD3200_54 + "/share/hadoop/common/lib/commons-codec-1.4.jar " +
    PHD3200_54 + "/share/hadoop/common/lib/commons-configuration-1.6.jar " +
    PHD3200_54 + "/share/hadoop/common/lib/commons-io-2.4.jar " +
    PHD3200_54 + "/share/hadoop/common/lib/commons-lang-2.6.jar " +
    PHD3200_54 + "/share/hadoop/common/lib/commons-logging-1.1.3.jar " +
    PHD3200_54 + "/share/hadoop/common/lib/commons-collections-3.2.1.jar " +
    PHD3200_54 + "/share/hadoop/common/lib/guava-11.0.2.jar " +
    PHD3200_54 + "/share/hadoop/common/lib/jsr305-1.3.9.jar " +
    PHD3200_54 + "/share/hadoop/common/lib/log4j-1.2.17.jar " +
    PHD3200_54 + "/share/hadoop/common/lib/slf4j-api-1.7.5.jar " +
    "/export/gcm/where/java/slf4j/jdk14/" + SLF4J_VERSION + "/slf4j-jdk14-" + SLF4J_VERSION + ".jar " +
    PHD3200_54 + "/share/hadoop/common/lib/hadoop-annotations-" + PHD3200_54_VERSION + ".jar " +
    PHD3200_54 + "/share/hadoop/common/lib/hadoop-auth-" + PHD3200_54_VERSION + ".jar " +
    PHD3200_54 + "/share/hadoop/common/hadoop-common-" + PHD3200_54_VERSION + ".jar " +
    PHD3200_54 + "/share/hadoop/hdfs/hadoop-hdfs-" + PHD3200_54_VERSION + ".jar " +
    "/export/gcm/where/java/hbase/" + HBASE_VERSION + "/hbase-" + HBASE_VERSION + ".jar " +
    PHD3200_54 + "/share/hadoop/common/lib/protobuf-java-2.5.0.jar"
  ;
  
  public static final String APACHE220 = "/export/gcm/where/java/hadoop/hadoop-2.2.0";
  public static final String APACHE220_OPT = "/opt/hadoop-2.2.0";
  public static final String APACHE220_VERSION = "2.2.0";
  protected static final String APACHE220_Jars =
    APACHE220 + "/share/hadoop/common/lib/commons-cli-1.2.jar " +
    APACHE220 + "/share/hadoop/common/lib/commons-codec-1.4.jar " +
    APACHE220 + "/share/hadoop/common/lib/commons-configuration-1.6.jar " +
    APACHE220 + "/share/hadoop/common/lib/commons-io-2.1.jar " +
    APACHE220 + "/share/hadoop/common/lib/commons-lang-2.5.jar " +
    APACHE220 + "/share/hadoop/common/lib/commons-logging-1.1.1.jar " +
    APACHE220 + "/share/hadoop/common/lib/guava-11.0.2.jar " +
    APACHE220 + "/share/hadoop/common/lib/jsr305-1.3.9.jar " +
    APACHE220 + "/share/hadoop/common/lib/log4j-1.2.17.jar " +
    APACHE220 + "/share/hadoop/common/lib/slf4j-api-1.7.5.jar " +
    "/export/gcm/where/java/slf4j/jdk14/" + SLF4J_VERSION + "/slf4j-jdk14-" + SLF4J_VERSION + ".jar " +
    APACHE220 + "/share/hadoop/common/lib/hadoop-annotations-" + APACHE220_VERSION + ".jar " +
    APACHE220 + "/share/hadoop/common/lib/hadoop-auth-" + APACHE220_VERSION + ".jar " +
    APACHE220 + "/share/hadoop/common/hadoop-common-" + APACHE220_VERSION + ".jar " +
    APACHE220 + "/share/hadoop/hdfs/hadoop-hdfs-" + APACHE220_VERSION + ".jar " +
    "/export/gcm/where/java/hbase/" + HBASE_VERSION + "/hbase-" + HBASE_VERSION + ".jar " +
    APACHE220 + "/share/hadoop/common/lib/protobuf-java-2.5.0.jar"
  ;

  public static final String APACHE241 = "/export/gcm/where/java/hadoop/hadoop-2.4.1";
  public static final String APACHE241_OPT = "/opt/hadoop-2.4.1";
  public static final String APACHE241_VERSION = "2.4.1";
  protected static final String APACHE241_Jars =
    APACHE241 + "/share/hadoop/common/lib/commons-cli-1.2.jar " +
    APACHE241 + "/share/hadoop/common/lib/commons-codec-1.4.jar " +
    APACHE241 + "/share/hadoop/common/lib/commons-configuration-1.6.jar " +
    APACHE241 + "/share/hadoop/common/lib/commons-io-2.4.jar " +
    APACHE241 + "/share/hadoop/common/lib/commons-lang-2.6.jar " +
    APACHE241 + "/share/hadoop/common/lib/commons-logging-1.1.3.jar " +
    APACHE241 + "/share/hadoop/common/lib/commons-collections-3.2.1.jar " +
    APACHE241 + "/share/hadoop/common/lib/guava-11.0.2.jar " +
    APACHE241 + "/share/hadoop/common/lib/jsr305-1.3.9.jar " +
    APACHE241 + "/share/hadoop/common/lib/log4j-1.2.17.jar " +
    APACHE241 + "/share/hadoop/common/lib/slf4j-api-1.7.5.jar " +
    "/export/gcm/where/java/slf4j/jdk14/" + SLF4J_VERSION + "/slf4j-jdk14-" + SLF4J_VERSION + ".jar " +
    APACHE241 + "/share/hadoop/common/lib/hadoop-annotations-" + APACHE241_VERSION + ".jar " +
    APACHE241 + "/share/hadoop/common/lib/hadoop-auth-" + APACHE241_VERSION + ".jar " +
    APACHE241 + "/share/hadoop/common/hadoop-common-" + APACHE241_VERSION + ".jar " +
    APACHE241 + "/share/hadoop/hdfs/hadoop-hdfs-" + APACHE241_VERSION + ".jar " +
    "/export/gcm/where/java/hbase/" + HBASE_VERSION + "/hbase-" + HBASE_VERSION + ".jar " +
    APACHE241 + "/share/hadoop/common/lib/protobuf-java-2.5.0.jar"
  ;

  public static final String HORTONWORKS26 = "/export/gcm/where/java/hadoop/hdp-2.2.0.2.0.6.0-102";
  public static final String HORTONWORKS26_OPT = "/opt/hdp-2.2.0.2.0.6.0-102";
  public static final String HORTONWORKS26_version = "2.2.0.2.0.6.0-102";
  protected static final String HORTONWORKS26_Jars =
		          HORTONWORKS26 + "/share/hadoop/common/lib/commons-cli-1.2.jar " +
				  HORTONWORKS26 + "/share/hadoop/common/lib/commons-codec-1.4.jar " +
				  HORTONWORKS26 + "/share/hadoop/common/lib/commons-configuration-1.6.jar " +
				  HORTONWORKS26 + "/share/hadoop/common/lib/commons-io-2.1.jar " +
				  HORTONWORKS26 + "/share/hadoop/common/lib/commons-lang-2.5.jar " +
				  HORTONWORKS26 + "/share/hadoop/common/lib/commons-logging-1.1.1.jar " +
				  HORTONWORKS26 + "/share/hadoop/common/lib/commons-collections-3.2.1.jar " +
				  HORTONWORKS26 + "/share/hadoop/common/lib/guava-11.0.2.jar " +
				  HORTONWORKS26 + "/share/hadoop/common/lib/jsr305-1.3.9.jar " +
				  HORTONWORKS26 + "/share/hadoop/common/lib/log4j-1.2.17.jar " +
				  //HORTONWORKS26 + "/share/hadoop/common/lib/htrace-core-3.0.4.jar " +
				  HORTONWORKS26 + "/share/hadoop/common/lib/slf4j-api-1.7.5.jar " +
		    "/export/gcm/where/java/slf4j/jdk14/" + SLF4J_VERSION + "/slf4j-jdk14-" + SLF4J_VERSION + ".jar " +
		    HORTONWORKS26 + "/share/hadoop/common/lib/hadoop-annotations-" + HORTONWORKS26_version + ".jar " +
		    HORTONWORKS26 + "/share/hadoop/common/lib/hadoop-auth-" + HORTONWORKS26_version + ".jar " +
		    HORTONWORKS26 + "/share/hadoop/common/hadoop-common-" + HORTONWORKS26_version + ".jar " +
		    HORTONWORKS26 + "/share/hadoop/hdfs/hadoop-hdfs-" + HORTONWORKS26_version + ".jar " +
		    "/export/gcm/where/java/hbase/" + HBASE_VERSION + "/hbase-" + HBASE_VERSION + ".jar " +
		    HORTONWORKS26 + "/share/hadoop/common/lib/protobuf-java-2.5.0.jar"
		  ;
  public static final String DEFAULT_HADOOP_DIST = HORTONWORKS26;
  public static final String DEFAULT_APACHE_DIST = APACHE220;
  public static final String DEFAULT_BASE_HDFS_DIR_NAME = "scratch";
  public static final int DEFAULT_REPLICATION = 1;

  public static final String KERBEROS = "kerberos";
  public static final String KERBEROS_KINIT = "kerberosKinit";
  public static final String SIMPLE = "simple";

  public static final String LOGICAL_HOST_PREFIX = "hydra_hadoop_";

  /**
   * (String(s))
   * Logical names of the Hadoop cluster descriptions. Each name must be unique.
   * This is a required parameter. Not for use with oneof, range, or robing.
   */
  public static Long names;

  /**
   * (boolean(s))
   * Whether to add the HDFS configuration to the classpath of all hydra client
   * JVMs. Defaults to false. Hydra constructs the classpaths to point to the
   * NameNode configuration directory, so only JVMs with NFS access to this
   * directory will benefit from the classpath addition. This parameter is
   * typically used in conjunction with setting {@link #securityAuthentication}
   * to {@link #KERBEROS}.
   */
  public static Long addHDFSConfigurationToClassPath;

  /**
   * (String)
   * The name of a scratch directory to include on the path of various HDFS
   * directories for logs and data. Defaults to {@link
   * #DEFAULT_BASE_HDFS_DIR_NAME}.
   */
  public static Long baseHDFSDirName;

  /**
   * (Comma-separated Lists of String(s))
   * Physical host names for the DataNodes. The local host can be specified
   * as "localhost", which is the default. YARN NodeManagers will use the
   * same hosts.
   */
  public static Long dataNodeHosts;

  /**
   * (Comma-separated Lists of colon-separated String(s))
   * Drives to use for data on each of the {@link #dataNodeHosts}. Defaults
   * to the drive used by the test result directory.
   */
  public static Long dataNodeDataDrives;

  /**
   * (Comma-separated Lists of String(s))
   * Drives to use for logs on each of the {@link #dataNodeHosts}. Defaults
   * to the drive used by the test result directory.
   */
  public static Long dataNodeLogDrives;

  /**
   * (String(s))
   * Path to the Hadoop distribution for each logical cluster. Defaults to
   * the value of <code>-DHADOOP_DIST</code> in the hydra master controller,
   * passed in through batterytest. The value of this property defaults to
   * the latest PivotalHD at {@link #DEFAULT_HADOOP_DIST}. For Apache Hadoop,
   * use {@link #DEFAULT_APACHE_DIST} or any other supported distribution.
   * <p>
   * Be sure to use the same distribution when configuring the {@link VmPrms
   * #extraClassPaths}. See {@link #getServerJars(String,int)} for a handy
   * test configuration function which can be used to pass in the desired
   * distribution.
   */
  public static Long hadoopDist;

  /**
   * (Comma-separated Lists of String(s))
   * Physical host names for the NameNodes. The local host can be specified
   * as "localhost", which is the default.
   */
  public static Long nameNodeHosts;

  /**
   * (Comma-separated Lists of colon-separated String(s))
   * Drives to use for data on each of the {@link #nameNodeHosts}. Defaults
   * to the drive used by the test result directory.
   */
  public static Long nameNodeDataDrives;

  /**
   * (Comma-separated Lists of String(s))
   * Drives to use for logs on each of the {@link #nameNodeHosts}. Defaults
   * to the drive used by the test result directory.
   */
  public static Long nameNodeLogDrives;

  /**
   * (String(s))
   * NameNode URL for each logical cluster. Defaults to an autogenerated port
   * on the first of the {@link #nameNodeHosts}. Set this parameter to attach
   * to an existing cluster. Otherwise, simply allow it to default.
   */
  public static Long nameNodeURL;

  /**
   * (Comma-separated Lists of colon-separated String(s))
   * Drives to use for NodeManager data on each of the {@link #dataNodeHosts}.
   * Defaults to the drive used by the test result directory.
   */
  public static Long nodeManagerDataDrives;

  /**
   * (Comma-separated Lists of String(s))
   * Drives to use for NodeManager logs on each of the {@link #dataNodeHosts}.
   * Defaults to the drive used by the test result directory.
   */
  public static Long nodeManagerLogDrives;

  /**
   * (int(s))
   * HDFS block replication default. Defaults to {@link #DEFAULT_REPLICATION}.
   */
  public static Long replication;

  /**
   * (List of colon-separated String(s))
   * Drives to use for ResourceManager data on the {@link #resourceManagerHost}.
   * Defaults to the drive used by the test result directory.
   */
  public static Long resourceManagerDataDrives;

  /**
   * (List of String(s))
   * Physical host name for the ResourceManager. The local host can be specified
   * as "localhost", which is the default.
   */
  public static Long resourceManagerHost;

  /**
   * (List of String(s))
   * Drive to use for ResourceManager log on the {@link #resourceManagerHost}.
   * Defaults to the drive used by the test result directory.
   */
  public static Long resourceManagerLogDrive;

  /**
   * (String(s))
   * ResourceManager URL for each logical cluster. Defaults to an autogenerated
   * port on the {@link #resourceManagerHost}. Set this parameter to attach
   * to an existing resource manager. Otherwise, simply allow it to default.
   */
  public static Long resourceManagerURL;

  /**
   * (String(s))
   * ResourceManager resource tracker host:port for each logical cluster.
   * Defaults to an autogenerated port on the {@link #resourceManagerHost}.
   * Set this parameter to attach to an existing resource manager. Otherwise,
   * simply allow it to default.
   */
  public static Long resourceTrackerAddress;

  /**
   * (String(s))
   * ResourceManager scheduler host:port for each logical cluster.
   * Defaults to an autogenerated port on the {@link #resourceManagerHost}.
   * Set this parameter to attach to an existing resource manager. Otherwise,
   * simply allow it to default.
   */
  public static Long schedulerAddress;

  /**
   * (String(s))
   * Type of Hadoop security authentication to use. Valid values are {@link
   * #KERBEROS} and {@link #KERBEROS_KINIT}, which use Kerberos, and
   * {@link #SIMPLE} (default), which disables security.
   * <p>
   * When using Kerberos, you must follow the special instructions at
   * <a href="https://wiki.gemstone.com/display/Hydra/Secure+Hadoop">https://wiki.gemstone.com/display/Hydra/Secure+Hadoop</a>
   * <p>
   * When using authentication, hydra automatically turns on authorization using
   * the ACLs in <code>$JTESTS/hydra/hadoop/conf/hadoop-policy.xml</code>.
   * <p>
   * {@link #KERBEROS_KINIT} adds basic security configuration for
   * Kerberos to core-site.xml and hdfs-site.xml. The kinit utility is used at
   * runtime to obtain temporary tokens. This option requires using an {@link
   * HDFSStorePrms#clientConfigFile} to set additional security properties,
   * including GemFireXD-specific properties.
   * <p>
   * {@link #KERBEROS} adds <code>hadoop.security.auth_to_local
   * </code> to the basic security configuration in core-site.xml and puts
   * additional security properties in hdfs-site.xml. This is the strategy used
   * by production applications. If {@link #addHDFSConfigurationToClassPath} is
   * set true and servers have NFS access to the NameNode configuration
   * directory, then no {@link HDFSStorePrms#clientConfigFile} is required for
   * configuring security.
   */
  public static Long securityAuthentication;

//------------------------------------------------------------------------------
// Classpath support
//------------------------------------------------------------------------------

  /**
   * Hydra test configuration function that returns the jars needed for a
   * Hadoop-enabled server using the specified Hadoop distribution. This
   * function is for use in hydra configuration files to set {@link
   * VmPrms#extraClassPaths}. Specify the number of comma-separated lists
   * of jars needed (such as the number of servers).
   */
  public static String getServerJars(String hadoopDist, int n) {
    String jars = null;
    String dist = EnvHelper.expandEnvVars(hadoopDist);
    if (dist.equals(PHD3000_138) || dist.equals(PHD3000_138_OPT)) {
      jars = PHD3000_138_Jars;
    } else if (dist.equals(PHD3100_175) || dist.equals(PHD3100_175_OPT)) {
      jars = PHD3100_175_Jars;
    } else if (dist.equals(PHD3200_54) || dist.equals(PHD3200_54_OPT)) {
      jars = PHD3200_54_Jars;
    } else if (dist.equals(APACHE220) || dist.equals(APACHE220_OPT)) {
      jars = APACHE220_Jars;
    } else if (dist.equals(APACHE241) || dist.equals(APACHE241_OPT)) {
      jars = APACHE241_Jars;
    } else if (dist.equals(HORTONWORKS26) || dist.equals(HORTONWORKS26_OPT)) {
    	jars = HORTONWORKS26_Jars;
    } else {
      String s = "Unsupported Hadoop distribution: " + dist;
      throw new HydraConfigException(s);
    }
    return TestConfigFcns.duplicate(jars, n, true);
  }

}
