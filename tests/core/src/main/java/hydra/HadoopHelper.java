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

import hydra.HadoopDescription.DataNodeDescription;
import hydra.HadoopDescription.NodeDescription;
import hydra.HadoopPrms.NodeType;
import java.io.*;
import java.util.*;
import java.rmi.RemoteException;

/**
 * Helps hydra clients use {@link HadoopDescription} to manage a hadoop cluster.
 * Methods are thread-safe but not distributed-thread-safe, so must be called
 * by a single hydra client JVM to avoid problems at the hadoop cluster level.
 */
public class HadoopHelper {

  private static final String SHELL = "/bin/bash";
  private static final String HADOOP_DAEMON = "/sbin/hadoop-daemon.sh";
  private static final String YARN_DAEMON = "/sbin/yarn-daemon.sh";

  // Required for running Hadoop in secure mode
  private static final String JSVC_HOME = "/export/gcm/where/java/hadoop/hadoop-secure-utils/bigtop-utils";

  // Required Kerberos files for running Hadoop in secure mode
  public static final String GFXD_SECURE_KEYTAB_FILE = "/export/gcm/where/java/hadoop/hadoop-secure-keytabs/gfxd-secure.keytab";
  public static final String HDFS_SECURE_KEYTAB_FILE = "/export/gcm/where/java/hadoop/hadoop-secure-keytabs/hdfs-secure.keytab";
  public static final String MAPRED_SECURE_KEYTAB_FILE = "/export/gcm/where/java/hadoop/hadoop-secure-keytabs/mapred.keytab";
  public static final String YARN_SECURE_KEYTAB_FILE = "/export/gcm/where/java/hadoop/hadoop-secure-keytabs/yarn.keytab";
  private static final String JAVA_SECURITY_KRB5_CONF = "/export/gcm/where/java/hadoop/hadoop-secure-utils/krb5.conf";

  // Required for managing secure processes as root user
  private static final String ROOT_SCRIPT = "/export/localnew/scripts/commands_for_secure_hdfs.sh";
  private static final String KINIT_CMD = "/usr/bin/kinit";
  private static final String KINIT = "/usr/bin/kinit -k -t";
  private static final String KDESTROY_CMD = "/usr/bin/kdestroy";
  private static final String KDESTROY = "/usr/bin/kdestroy -q";
  private static final String SUDO = "/usr/bin/sudo";

  private static final String[] ConfigurationFiles = {
    "capacity-scheduler.xml",
    "commons-logging.properties",
    "configuration.xsl",
    "hadoop-metrics.properties",
    "hadoop-metrics2.properties",
    "hadoop-policy.xml",
    "log4j.properties",
    "mapred-queues.xml"
  };

//------------------------------------------------------------------------------
// Configuration
//------------------------------------------------------------------------------

  /**
   * Builds the Hadoop configuration files for the given cluster configuration.
   * The configuration uses the {@link HadoopDescription} corresponding to the
   * cluster configuration from {@link HadoopPrms#names}.
   * <p>
   * The files are placed in the test directory in a subdirectory named for
   * the cluster, node type, and host. For example:
   * <code>mycluster_DataNode_w2-2013-lin-08_conf</code>
   * <p>
   * The subdirectory includes generated configuration files such as
   * hadoop-env.sh and slaves, and other files that are copied from <code>
   * $JTESTS/hydra/hadoop/conf</code>.
   */
  public static synchronized void configureHadoop(String hadoopConfig) {
    Log.getLogWriter().info("Configuring hadoop for " + hadoopConfig);
    HadoopDescription hdd = getHadoopDescription(hadoopConfig);

    // build slaves file contents
    StringBuilder sb = new StringBuilder();
    for (DataNodeDescription dnd : hdd.getDataNodeDescriptions()) {
      sb.append(dnd.getHostDescription().getCanonicalHostName()).append("\n");
    }
    String slaves = sb.toString();
    Log.getLogWriter().info("Configured hadoop slaves file for " + hadoopConfig
                           + "\n" + slaves);

    // fill the conf dir for each node description
    List<NodeDescription> nds = new ArrayList();
    nds.addAll(hdd.getNameNodeDescriptions());
    nds.addAll(hdd.getDataNodeDescriptions());
    if (hdd.getResourceManagerDescription() != null) {
      nds.add(hdd.getResourceManagerDescription());
      nds.addAll(hdd.getNodeManagerDescriptions());
    }
    for (NodeDescription nd : nds) {
      String confDir = nd.getConfDir();
      Log.getLogWriter().info("Generating hadoop configuration files for "
                              + nd.getName() + " in " + confDir);
      generateHadoopEnv(hdd, nd);
      generateYarnEnv(hdd, nd);
      generateCoreSiteXMLFile(hdd, nd);
      generateHDFSSiteXMLFile(hdd, nd);
      generateMapRedSiteXMLFile(hdd, nd);
      generateYarnSiteXMLFile(hdd, nd);
      if (hdd.isSecure()) {
        generateContainerExecutorCFGFile(hdd, nd);
      }
      writeSlavesFile(nd, slaves);
      copyMiscFiles(nd);
      Log.getLogWriter().info("Generated hadoop configuration files for "
                             + nd.getName() + " in " + confDir);
    }
  }

  /**
   * Builds the Hadoop configuration files for the given cluster configuration.
   * The configuration uses the {@link HadoopDescription} corresponding to the
   * cluster configuration from {@link HadoopPrms#names}.
   * <p>
   * The files are placed in the test directory in a subdirectory named for
   * the cluster, node type, and host. For example:
   * <code>mycluster_DataNode_w2-2013-lin-08_conf</code>
   * <p>
   * The subdirectory includes generated configuration files such as
   * hadoop-env.sh and slaves, and other files that are copied from <code>
   * $JTESTS/hydra/hadoop/conf</code>.
   */
  public static synchronized void configureHDFS(String hadoopConfig) {
    Log.getLogWriter().info("Configuring HDFS for " + hadoopConfig);
    HadoopDescription hdd = getHadoopDescription(hadoopConfig);

    // build slaves file contents
    StringBuilder sb = new StringBuilder();
    for (String host : hdd.getDataNodeHosts()) {
      sb.append(host).append("\n");
    }
    String slaves = sb.toString();
    Log.getLogWriter().info("Configured hadoop slaves file for " + hadoopConfig
                           + "\n" + slaves);

    // fill the conf dir for each node description
    List<NodeDescription> nds = new ArrayList();
    nds.addAll(hdd.getNameNodeDescriptions());
    nds.addAll(hdd.getDataNodeDescriptions());
    for (NodeDescription nd : nds) {
      String confDir = nd.getConfDir();
      Log.getLogWriter().info("Generating hadoop configuration files for "
                              + nd.getName() + " in " + confDir);
      generateHadoopEnv(hdd, nd);
      generateCoreSiteXMLFile(hdd, nd);
      generateHDFSSiteXMLFile(hdd, nd);
      writeSlavesFile(nd, slaves);
      copyMiscFiles(nd);
      Log.getLogWriter().info("Generated hadoop configuration files for "
                             + nd.getName() + " in " + confDir);
    }
  }

  private static void generateHadoopEnv(HadoopDescription hdd,
                                        NodeDescription nd) {
    Log.getLogWriter().info("Generating hadoop-env for " + nd.getName());
    StringBuilder opts = new StringBuilder();
    opts.append("-Djava.net.preferIPv4Stack=true");
    opts.append(" -Dfs.defaultFS=").append(hdd.getNameNodeURL());
    opts.append(" -Dyarn.resourcemanager.address=")
        .append(hdd.getResourceManagerURL());
    switch (nd.getNodeType()) {
      case DataNode:
        opts.append(" -Ddfs.data.dir=").append(nd.getDataDirsAsString());
        break;
      case NameNode:
        opts.append(" -Ddfs.name.dir=").append(nd.getDataDirsAsString());
        break;
    }
    if (hdd.isSecure()) {
      opts.append(" -Djava.security.krb5.conf=")
          .append(JAVA_SECURITY_KRB5_CONF);
      //opts.append(" -Dsun.security.krb5.debug=true");
    }

    HostDescription hd = nd.getHostDescription();
    StringBuilder sb = new StringBuilder();
    sb.append("export JAVA_HOME=").append(hd.getJavaHome())
      .append("\n")
      .append("export HADOOP_CONF_DIR=").append(nd.getConfDir())
      .append("\n")
      .append("export HADOOP_LOG_DIR=").append(nd.getLogDir())
      .append("\n")
      .append("export HADOOP_OPTS=\"").append(opts.toString().trim()).append("\"")
      .append("\n")
      .append("export HADOOP_ZKFC_OPTS=\"-Xmx256m\"")
      .append("\n")
      .append("export HADOOP_PID_DIR=").append(nd.getPIDDir())
      .append("\n")
      .append("export HADOOP_NAMENODE_OPTS=\"-Dhadoop.security.logger=INFO,RFAS -Dhdfs.audit.logger=INFO,NullAppender\"")
      .append("\n")
      .append("export HADOOP_SECONDARYNAMENODE_OPTS=\"-Dhadoop.security.logger=INFO,RFAS -Dhdfs.audit.logger=INFO,NullAppender\"")
      .append("\n")
      .append("export HADOOP_DATANODE_OPTS=\"-Dhadoop.security.logger=ERROR,RFAS -Dhdfs.audit.logger=INFO,NullAppender\"")
      .append("\n")
      .append("export HADOOP_CLIENT_OPTS=\"-Xmx128m\"")
      .append("\n")
      .append("export HADOOP_IDENT_STRING=").append(System.getProperty("user.name"))
      .append("\n")
      .append("for f in $HADOOP_HOME/contrib/capacity-scheduler/*.jar; do\n")
      .append("  if [ \"$HADOOP_CLASSPATH\" ]; then\n")
      .append("    export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$f\n")
      .append("  else\n")
      .append("    export HADOOP_CLASSPATH=$f\n")
      .append("  fi\n")
      .append("done\n")
      .append("export HADOOP_CLASSPATH=").append(hd.getTestDir()).append(":$HADOOP_CLASSPATH").append("\n")
      // TBD add gemfirexd.jar to the hadoop classpath
      ;
    if (hdd.isSecure()) {
      sb.append("export HADOOP_SECURE_DN_USER=").append(System.getProperty("user.name"))
        .append("\n")
        .append("export HADOOP_SECURE_DN_LOG_DIR=").append(nd.getLogDir())
        .append("\n")
        .append("export HADOOP_SECURE_DN_PID_DIR=").append(nd.getPIDDir())
        .append("\n")
        .append("export JSVC_HOME=" + JSVC_HOME)
        .append("\n");
    }
    String env = sb.toString();
    String fn = nd.getConfDir() + "/hadoop-env.sh";
    FileUtil.writeToFile(fn, env);
    Log.getLogWriter().info("Generated hadoop-env.sh for "
                           + nd.getClusterName() + " in " + fn + ":\n" + env);
  }

  private static void generateYarnEnv(HadoopDescription hdd,
                                      NodeDescription nd) {
    Log.getLogWriter().info("Generating yarn-env for " + nd.getName());
    HostDescription hd = nd.getHostDescription();
    StringBuilder opts = new StringBuilder();
    opts.append(" -Dfs.defaultFS=")
        .append(hdd.getNameNodeURL());
    opts.append(" -Dyarn.resourcemanager.address=")
        .append(hdd.getResourceManagerURL());
    opts.append(" -Dyarn.resourcemanager.resource-tracker.address=")
        .append(hdd.getResourceTrackerAddress());
    opts.append(" -Dyarn.resourcemanager.scheduler.address=")
        .append(hdd.getSchedulerAddress());
    StringBuilder sb = new StringBuilder();
    sb.append("export JAVA_HOME=").append(hd.getJavaHome())
      .append("\n")
      .append("export JSVC_HOME=" + JSVC_HOME)
      .append("\n")
      .append("export HADOOP_CONF_DIR=").append(nd.getConfDir())
      .append("\n")
      .append("export YARN_USER=yarn")
      .append("\n")
      .append("export YARN_CONF_DIR=").append(nd.getConfDir())
      .append("\n")
      .append("export YARN_LOG_DIR=").append(nd.getLogDir())
      .append("\n")
      .append("export YARN_IDENT_STRING=").append(System.getProperty("user.name"))
      .append("\n")
      .append("export YARN_PID_DIR=").append(nd.getPIDDir())
      .append("\n")
      .append("export YARN_OPTS=\"").append(opts.toString().trim()).append("\"")
      .append("\n")
      ;
    String env = sb.toString();
    String fn = nd.getConfDir() + "/yarn-env.sh";
    FileUtil.writeToFile(fn, env);
    Log.getLogWriter().info("Generated yarn-env.sh for "
                           + nd.getClusterName() + " in " + fn + ":\n" + env);
  }

  private static void generateCoreSiteXMLFile(HadoopDescription hdd,
                                              NodeDescription nd) {
    String confDir = nd.getConfDir();
    HostDescription hd = nd.getHostDescription();
    String infn;
    String security = hdd.getSecurityAuthentication();
    if (security.equals(HadoopPrms.SIMPLE)) {
      infn = "$JTESTS/hydra/hadoop/conf/core-site.xml";
    } else if (security.equals(HadoopPrms.KERBEROS)) {
      infn = "$JTESTS/hydra/hadoop/conf/core-site-secure.xml";
    } else if (security.equals(HadoopPrms.KERBEROS_KINIT)) {
      infn = "$JTESTS/hydra/hadoop/conf/core-site-secure-kinit.xml";
    } else {
      String s = "Unsupported security: " + security;
      throw new UnsupportedOperationException(s);
    }
    String xml = readFile(infn, hd);
    xml = xml.replace("${fs.defaultFS}", hdd.getNameNodeURL());
    if (hdd.isSecure()) {
      xml = xml.replace("${securityAuthentication}", HadoopPrms.KERBEROS);
      xml = xml.replace("${securityAuthorization}", "true");
    } else {
      xml = xml.replace("${securityAuthorization}", "false");
    }
    String outfn = confDir + "/core-site.xml";
    Log.getLogWriter().info("Writing core-site.xml file for " + nd.getName()
                           + " to " + outfn);
    FileUtil.writeToFile(outfn, xml);
    Log.getLogWriter().info("Wrote core-site.xml file for " + nd.getName()
                           + " to " + outfn);
  }

  private static void generateHDFSSiteXMLFile(HadoopDescription hdd,
                                              NodeDescription nd) {
    String confDir = nd.getConfDir();
    HostDescription hd = nd.getHostDescription();
    String infn;
    String security = hdd.getSecurityAuthentication();
    if (security.equals(HadoopPrms.SIMPLE)) {
      infn = "$JTESTS/hydra/hadoop/conf/hdfs-site.xml";
    } else if (security.equals(HadoopPrms.KERBEROS)) {
      infn = "$JTESTS/hydra/hadoop/conf/hdfs-site-secure.xml";
    } else if (security.equals(HadoopPrms.KERBEROS_KINIT)) {
      infn = "$JTESTS/hydra/hadoop/conf/hdfs-site-secure-kinit.xml";
    } else {
      String s = "Unsupported security: " + security;
      throw new UnsupportedOperationException(s);
    }
    String xml = readFile(infn, hd);
    xml = xml.replace("${dfs.replication}", String.valueOf(hdd.getReplication()));
    xml = xml.replace("${dn.ipc.port}", getRandomPort());
    xml = xml.replace("${nn.http.port}", getRandomPort());
    xml = xml.replace("${nn.secondary.http.port}", getRandomPort());
    if (hdd.isSecure()) {
      // use these hardwired ports unless there's a problem, since
      // they must be under 1024 and only root can bind to them
      xml = xml.replace("${dn.port}", "1004");
      xml = xml.replace("${dn.http.port}", "1006");
      xml = xml.replace("${hdfs.secure.keytab.file}", HDFS_SECURE_KEYTAB_FILE);
    } else {
      xml = xml.replace("${dn.port}", getRandomPort());
      xml = xml.replace("${dn.http.port}", getRandomPort());
    }
    String outfn = confDir + "/hdfs-site.xml";
    Log.getLogWriter().info("Writing hdfs-site.xml file for " + nd.getName()
                          + " to " + outfn);
    FileUtil.writeToFile(outfn, xml);
    Log.getLogWriter().info("Wrote hdfs-site.xml file for " + nd.getName()
                          + " to " + outfn);
  }

  private static void generateMapRedSiteXMLFile(HadoopDescription hdd,
                                                NodeDescription nd) {
    String confDir = nd.getConfDir();
    HostDescription hd = nd.getHostDescription();
    String infn;
    if (hdd.isSecure()) {
      infn = "$JTESTS/hydra/hadoop/conf/mapred-site-secure.xml";
    } else {
      infn = "$JTESTS/hydra/hadoop/conf/mapred-site.xml";
    }
    String xml = readFile(infn, hd);
    xml = xml.replace("${yarn.resourcemanager.address}",
                       hdd.getResourceManagerURL());
    xml = xml.replace("${mapreduce.shuffle.port}", getRandomPort());
    if (hdd.isSecure()) {
      xml = xml.replace("${mapred.secure.keytab.file}", MAPRED_SECURE_KEYTAB_FILE);
    }
    String outfn = confDir + "/mapred-site.xml";
    Log.getLogWriter().info("Writing mapred-site.xml file for " + nd.getName()
                           + " to " + outfn);
    FileUtil.writeToFile(outfn, xml);
    Log.getLogWriter().info("Wrote mapred-site.xml file for " + nd.getName()
                           + " to " + outfn);
  }

  /**
   * This method is only executed for secure Hadoop.
   */
  private static void generateContainerExecutorCFGFile(
          HadoopDescription hdd, NodeDescription nd) {
    String infn = "$JTESTS/hydra/hadoop/conf/container-executor-secure.cfg";
    String xml = readFile(infn, nd.getHostDescription());
    xml = xml.replace("${yarn.nodemanager.local-dirs}", nd.getLogDir());
    xml = xml.replace("${yarn.nodemanager.log-dirs}", nd.getLogDir());

    String hadoopDist = hdd.getHadoopDist();
    String src = System.getProperty("user.dir") + "/container-executor.cfg";
    String dst = hadoopDist + "/etc/hadoop/container-executor.cfg";

    String host = nd.getHostName();
    String user = System.getProperty("user.name");

    Log.getLogWriter().info("Writing container-executor.cfg file for "
                           + nd.getName() + " to " + src);
    FileUtil.writeToFile(src, xml);
    Log.getLogWriter().info("Wrote container-executor.cfg file for "
                           + nd.getName() + " to " + src);

    copyFile(src, dst, host);
    setReadPermission(dst, host);
  }

  private static void generateYarnSiteXMLFile(HadoopDescription hdd,
                                              NodeDescription nd) {
    String confDir = nd.getConfDir();
    HostDescription hd = nd.getHostDescription();
    String infn;
    if (hdd.isSecure()) {
      infn = "$JTESTS/hydra/hadoop/conf/yarn-site-secure.xml";
    } else {
      infn = "$JTESTS/hydra/hadoop/conf/yarn-site.xml";
    }
    String xml = readFile(infn, hd);
    xml = xml.replace("${yarn.resourcemanager.address}",
                       hdd.getResourceManagerURL());
    xml = xml.replace("${yarn.resourcemanager.resource-tracker.address}",
                       hdd.getResourceTrackerAddress());
    xml = xml.replace("${yarn.resourcemanager.scheduler.address}",
                       hdd.getSchedulerAddress());
    xml = xml.replace("${yarn.nodemanager.local-dirs}", nd.getLogDir());
    xml = xml.replace("${yarn.nodemanager.log-dirs}", nd.getLogDir());
    if (hdd.isSecure()) {
      xml = xml.replace("${yarn.secure.keytab.file}", YARN_SECURE_KEYTAB_FILE);
    }
    String outfn = confDir + "/yarn-site.xml";
    Log.getLogWriter().info("Writing yarn-site.xml file for " + nd.getName()
                           + " to " + outfn);
    FileUtil.writeToFile(outfn, xml);
    Log.getLogWriter().info("Wrote yarn-site.xml file for " + nd.getName()
                           + " to " + outfn);
  }

  private static void writeSlavesFile(NodeDescription nd, String slaves) {
    String confDir = nd.getConfDir();
    String fn = confDir + "/slaves";
    Log.getLogWriter().info("Writing slaves file for " + nd.getName()
                           + " to " + fn);
    FileUtil.writeToFile(fn, slaves);
    Log.getLogWriter().info("Wrote slaves file for " + nd.getName()
                           + " to " + fn);
  }

  private static void copyMiscFiles(NodeDescription nd) {
    String confDir = nd.getConfDir();
    Log.getLogWriter().info("Copying miscellaneous files for " + nd.getName()
                           + " to " + confDir);
    String src = TestConfig.getInstance().getMasterDescription()
                           .getVmDescription().getHostDescription()
                           .getTestDir() + "/hydra/hadoop/conf/";
    String dst = confDir + "/";
    for (String fn : ConfigurationFiles) {
      Log.getLogWriter().info("Copying " + fn + " to " + confDir);
      FileUtil.copyFile(src + fn, dst + fn);
    }
    Log.getLogWriter().info("Copied miscellaneous files for " + nd.getName()
                           + " to " + confDir);
  }

//------------------------------------------------------------------------------
// Cluster
//------------------------------------------------------------------------------

  /**
   * Starts the Hadoop cluster using the given cluster configuration.
   * <p>
   * The cluster is configured using the {@link HadoopDescription}
   * corresponding to the cluster configuration from {@link HadoopPrms#names}.
   */
  public static synchronized void startCluster(String hadoopConfig) {
    formatNameNodes(hadoopConfig);
    startNameNodes(hadoopConfig);
    startDataNodes(hadoopConfig);
    startResourceManager(hadoopConfig);
    startNodeManagers(hadoopConfig);
  }

  /**
   * Stops the Hadoop cluster using the given cluster configuration, as found
   * in {@link HadoopPrms#names}.
   */
  public static synchronized void stopCluster(String hadoopConfig) {
    stopNodeManagers(hadoopConfig);
    stopResourceManager(hadoopConfig);
    stopDataNodes(hadoopConfig);
    stopNameNodes(hadoopConfig);
  }

  /**
   * Starts the HDFS cluster using the given cluster configuration.
   * <p>
   * The cluster is configured using the {@link HadoopDescription}
   * corresponding to the cluster configuration from {@link HadoopPrms#names}.
   */
  public static synchronized void startHDFSCluster(String hadoopConfig) {
    formatNameNodes(hadoopConfig);
    startNameNodes(hadoopConfig);
    startDataNodes(hadoopConfig);
  }

  /**
   * Stops the HDFS cluster using the given cluster configuration, as found
   * in {@link HadoopPrms#names}.
   */
  public static synchronized void stopHDFSCluster(String hadoopConfig) {
    stopDataNodes(hadoopConfig);
    stopNameNodes(hadoopConfig);
  }

//------------------------------------------------------------------------------
// NameNodes : format
//------------------------------------------------------------------------------

  /**
   * Formats Hadoop NameNodes using the given cluster configuration.
   * <p>
   * The nodes are configured using the {@link HadoopDescription}
   * corresponding to the cluster configuration from {@link HadoopPrms#names}.
   */
  public static synchronized void formatNameNodes(String hadoopConfig) {
    HadoopDescription hdd = getHadoopDescription(hadoopConfig);
    List<HadoopDescription.NameNodeDescription> nnds =
                           hdd.getNameNodeDescriptions();
    Log.getLogWriter().info("Formatting name nodes: " + nnds);
    for (NodeDescription nnd : nnds) {
      formatNameNode(hdd, nnd);
    }
    Log.getLogWriter().info("Formatted name nodes: " + nnds);
  }

  /**
   * Formats the Hadoop NameNode on the given host using the given node
   * configuration.
   * <p>
   * The node is configured using the {@link HadoopDescription}
   * corresponding to the cluster configuration from {@link HadoopPrms#names}.
   */
  public static synchronized void formatNameNode(String hadoopConfig,
                                                 String host) {
    HadoopDescription hdd = getHadoopDescription(hadoopConfig);
    List<HadoopDescription.NameNodeDescription> nnds =
                           hdd.getNameNodeDescriptions();
    for (NodeDescription nnd : nnds) {
      if (nnd.getHostName().equals(host)) {
        formatNameNode(hdd, nnd);
        return;
      }
    }
    String s = "No name node found for host: " + host;
    throw new HydraRuntimeException(s);
  }

  /**
   * Formats the Hadoop NameNode on the given host using the given node
   * configuration.
   */
  private static synchronized void formatNameNode(HadoopDescription hdd,
                                                  NodeDescription nd) {
    String cmd = SHELL + " " + hdd.getHadoopDist() + "/bin/hdfs"
               + " --config " + nd.getConfDir()
               + " namenode -format " + nd.getClusterName()
               ;
    String host = nd.getHostName();
    Log.getLogWriter().info("Formatting name node on host " + host
                            + " using command: " + cmd);
    HostDescription hd = nd.getHostDescription();
    String logfn = nd.getLogDir() + "/namenode-format.log";
    int pid = ProcessMgr.bgexec(host, cmd, hd.getUserDir(), logfn);
    recordPID("formatNameNode", hd, pid, false);
    int maxWaitSec = 180;
    if (!ProcessMgr.waitForDeath(host, pid, maxWaitSec)) {
      String s = "Waited more than " + maxWaitSec
               + " seconds for NameNode to be formatted";
      throw new HydraTimeoutException(s);
    }
    removePID("formatNameNode", hd, pid, false);
    Log.getLogWriter().info("Formatted name node on host " + host
                            + " using command: " + cmd + ", see "
                            + logfn + " for output");
  }

//------------------------------------------------------------------------------
// NameNodes : start
//------------------------------------------------------------------------------

  /**
   * Starts Hadoop NameNodes using the given cluster configuration.
   * <p>
   * The nodes are configured using the {@link HadoopDescription}
   * corresponding to the cluster configuration from {@link HadoopPrms#names}.
   */
  public static synchronized void startNameNodes(String hadoopConfig) {
    HadoopDescription hdd = getHadoopDescription(hadoopConfig);
    List<HadoopDescription.NameNodeDescription> nnds =
                           hdd.getNameNodeDescriptions();
    Log.getLogWriter().info("Starting name nodes: " + nnds);
    for (NodeDescription nnd : nnds) {
      startNameNode(hdd, nnd);
    }
    Log.getLogWriter().info("Started name nodes: " + nnds);
  }

  /**
   * Starts the Hadoop NameNode on the given host using the given cluster
   * configuration.
   * <p>
   * The node is configured using the {@link HadoopDescription}
   * corresponding to the cluster configuration from {@link HadoopPrms#names}.
   */
  public static synchronized void startNameNode(String hadoopConfig,
                                                String host) {
    HadoopDescription hdd = getHadoopDescription(hadoopConfig);
    List<HadoopDescription.NameNodeDescription> nnds =
                           hdd.getNameNodeDescriptions();
    for (NodeDescription nnd : nnds) {
      if (nnd.getHostName().equals(host)) {
        startNameNode(hdd, nnd);
        return;
      }
    }
    String s = "No name node found for host: " + host;
    throw new HydraRuntimeException(s);
  }

  /**
   * Starts the Hadoop NameNode using the given node configuration.
   */
  public static synchronized void startNameNode(HadoopDescription hdd,
                                                NodeDescription nd) {
    String cmd = SHELL + " " + hdd.getHadoopDist() + HADOOP_DAEMON
               + " --config " + nd.getConfDir()
               + " --script hdfs start namenode";
    String host = nd.getHostName();
    Log.getLogWriter().info("Starting name node on host " + host
                            + " using command: " + cmd);
    String result = null;
    try {
      result = executeUserCommand(host, cmd, 120);
    } catch (HydraRuntimeException e) {
      String s = "Failed to start NameNode on " + host + ", see "
               + nd.getLogDir() + ": " + result;
      throw new HydraRuntimeException(s, e);
    }
    String pidfn = nd.getPIDDir() + "/hadoop-"
                 + System.getProperty("user.name") + "-namenode.pid";
    Integer pid = readPID("startNameNode", pidfn);
    recordHadoop("startNameNode", hdd, nd, pid, false);
    if (!ProcessMgr.processExists(host, pid)) {
      String s = "Failed to start name node on host " + host
               + " using command: " + cmd + ", see "
               + nd.getLogDir() + " for output";
      throw new HydraRuntimeException(s);
    }
    Log.getLogWriter().info("Started name node on host " + host
                            + " using command: " + cmd + ", see "
                            + nd.getLogDir() + " for output");
  }

//------------------------------------------------------------------------------
// NameNodes : stop
//------------------------------------------------------------------------------

  /**
   * Stops the Hadoop NameNodes using the given cluster configuration, as found
   * in {@link HadoopPrms#names}.
   */
  public static synchronized void stopNameNodes(String hadoopConfig) {
    HadoopDescription hdd = getHadoopDescription(hadoopConfig);
    List<HadoopDescription.NameNodeDescription> nnds =
                            hdd.getNameNodeDescriptions();
    Log.getLogWriter().info("Stopping name nodes: " + nnds);
    for (NodeDescription nnd : nnds) {
      stopNameNode(hdd, nnd);
    }
    Log.getLogWriter().info("Stopped name nodes: " + nnds);
  }

  /**
   * Stops the Hadoop NameNode on the given host using the given cluster
   * configuration, as found in {@link HadoopPrms#names}.
   */
  public static synchronized void stopNameNode(String hadoopConfig,
                                               String host) {
    HadoopDescription hdd = getHadoopDescription(hadoopConfig);
    List<HadoopDescription.NameNodeDescription> nnds =
                            hdd.getNameNodeDescriptions();
    for (NodeDescription nnd : nnds) {
      if (nnd.getHostName().equals(host)) {
        stopNameNode(hdd, nnd);
        return;
      }
    }
    String s = "No name node found for host: " + host;
    throw new HydraRuntimeException(s);
  }

  /**
   * Stops the Hadoop NameNode using the given node configuration.
   */
  private static synchronized void stopNameNode(HadoopDescription hdd,
                                                NodeDescription nd) {
    String cmd = SHELL + " " + hdd.getHadoopDist() + HADOOP_DAEMON
               + " --config " + nd.getConfDir()
               + " --script hdfs stop namenode";
    String host = nd.getHostName();
    Log.getLogWriter().info("Stopping name node on host " + host
                            + " using command: " + cmd);
    //HostDescription hd = nd.getHostDescription();
    String result = null;
    try {
      result = executeUserCommand(host, cmd, 120);
    } catch (HydraRuntimeException e) {
      String s = "Failed to stop NameNode on " + host + ", see "
               + nd.getLogDir() + ": " + result;
      throw new HydraRuntimeException(s, e);
    }
    String pidfn = nd.getPIDDir() + "/hadoop-"
                 + System.getProperty("user.name") + "-namenode.pid";
    Integer pid = readPID("stopNameNode", pidfn);
    removeHadoop("stopNameNode", hdd, nd, pid, false);
    Log.getLogWriter().info("Stopped name node on host " + host
                            + " using command: " + cmd + ", see "
                            + nd.getLogDir() + " for output");
  }

//------------------------------------------------------------------------------
// DataNodes : start
//------------------------------------------------------------------------------

  /**
   * Starts Hadoop DataNodes using the given cluster configuration.
   * <p>
   * The nodes are configured using the {@link HadoopDescription}
   * corresponding to the cluster configuration from {@link HadoopPrms#names}.
   */
  public static synchronized void startDataNodes(String hadoopConfig) {
    HadoopDescription hdd = getHadoopDescription(hadoopConfig);
    List<HadoopDescription.DataNodeDescription> dnds =
                           hdd.getDataNodeDescriptions();
    Log.getLogWriter().info("Starting data nodes: " + dnds);
    for (NodeDescription dnd : dnds) {
      startDataNode(hdd, dnd);
    }
    Log.getLogWriter().info("Started data nodes: " + dnds);
  }

  /**
   * Starts the Hadoop DataNode on the given host using the given cluster
   * configuration.
   * <p>
   * The node is configured using the {@link HadoopDescription}
   * corresponding to the cluster configuration from {@link HadoopPrms#names}.
   */
  public static synchronized void startDataNode(String hadoopConfig,
                                                String host) {
    HadoopDescription hdd = getHadoopDescription(hadoopConfig);
    List<HadoopDescription.DataNodeDescription> dnds =
                           hdd.getDataNodeDescriptions();
    for (NodeDescription dnd : dnds) {
      if (dnd.getHostName().equals(host)) {
        startDataNode(hdd, dnd);
        return;
      }
    }
    String s = "No data node found for host: " + host;
    throw new HydraRuntimeException(s);
  }

  /**
   * Starts the Hadoop DataNode using the given node configuration.
   */
  public static synchronized void startDataNode(HadoopDescription hdd,
                                                NodeDescription nd) {
    HostDescription hd = nd.getHostDescription();
    String host = nd.getHostName();
    Log.getLogWriter().info("Starting data node on host " + host);
    String result = null;
    // the pid files cannot be written to or have file permissions/ownership changed by root across NFS (so execute on host running Master)
    String masterHostName = TestConfig.getInstance().getMasterDescription().getVmDescription().getHostDescription().getHostName();
    try {
      if (hdd.isSecure()) {
        // required when secure data node is running remotely (as root does not have permissions to write across NFS directories)
        setWritePermission(nd.getPIDDir(), masterHostName);

        Log.getLogWriter().info("Using secure mode (kerberos) as root");
        String cmd = "startSecureDataNode " + hdd.getHadoopDist() 
                   + " " + nd.getConfDir();
        result = executeRootCommand(host, cmd, 120);
      } else {
        String cmd = SHELL + " " + hdd.getHadoopDist() + HADOOP_DAEMON
                   + " --config " + nd.getConfDir()
                   + " --script hdfs start datanode";
        result = executeUserCommand(host, cmd, 120);
      }
    } catch (HydraRuntimeException e) {
      String s = "Failed to start data node on " + host + ", see "
               + nd.getLogDir() + ": " + result;
      throw new HydraRuntimeException(s, e);
    }
    if (hdd.isSecure()) {
      setOwnership(nd.getPIDDir(), masterHostName, System.getProperty("user.name"));
      setOwnership(nd.getLogDir(), host, System.getProperty("user.name"));

      setReadPermission(nd.getPIDDir(), masterHostName);
      setReadPermission(nd.getLogDir(), host);

      String spidfn = nd.getPIDDir() + "/hadoop_secure_dn.pid";
      Integer spid = readPID("startDataNode", spidfn);
      Log.getLogWriter().info("DataNode secure PID=" + spid);
      recordHadoop("startDataNode", hdd, nd, spid, true);
      boolean exists = processExists(host, spid);
      if (!exists) {
        String s = "Failed to start DataNode on host " + host + ", see "
                 + nd.getLogDir();
        throw new HydraRuntimeException(s);
      }

      if (hdd.getSecurityAuthentication().equals(HadoopPrms.KERBEROS_KINIT)) {
        String cmd1 = HDFS_SECURE_KEYTAB_FILE + " hdfs/" + hd.getCanonicalHostName() + "@GEMSTONE.COM";
        executeKinitCommand(host, cmd1, 120);
      }

      String cmd3 = "fs -chown gfxd-secure:users /"; // @todo why the slash?
      String outputFile = nd.getLogDir() + "/secure_chown.log";
      executeHadoopCommand(host, hdd, cmd3, outputFile);                // must execute on host running dataNode

      if (hdd.getSecurityAuthentication().equals(HadoopPrms.KERBEROS_KINIT)) {
        String cmd2 = GFXD_SECURE_KEYTAB_FILE + " gfxd-secure@GEMSTONE.COM";
        executeKinitCommand(host, cmd2, 120);
      }

    } else {
      String pidfn = nd.getPIDDir() + "/hadoop-"
                   + System.getProperty("user.name") + "-datanode.pid";
      Integer pid = readPID("startDataNode", pidfn);
      Log.getLogWriter().info("DataNode PID=" + pid);
      recordHadoop("startDataNode", hdd, nd, pid, false);
      {
        boolean exists = ProcessMgr.processExists(host, pid);
        if (!exists) {
          String s = "Failed to start DataNode on host " + host + ", see "
                   + nd.getLogDir();
          throw new HydraRuntimeException(s);
        }
      }
    }

    Log.getLogWriter().info("Started data node on host " + host
                            + ", see " + nd.getLogDir() + " for output");
  }

//------------------------------------------------------------------------------
// DataNodes : stop
//------------------------------------------------------------------------------

  /**
   * Stops Hadoop DataNodes using the given cluster configuration, as found in
   * {@link HadoopPrms#names}.
   */
  public static synchronized void stopDataNodes(String hadoopConfig) {
    HadoopDescription hdd = getHadoopDescription(hadoopConfig);
    List<HadoopDescription.DataNodeDescription> dnds =
                            hdd.getDataNodeDescriptions();
    Log.getLogWriter().info("Stopping data nodes: " + dnds);
    for (NodeDescription dnd : dnds) {
      stopDataNode(hdd, dnd);
    }
    Log.getLogWriter().info("Stopped data nodes: " + dnds);
  }

  /**
   * Stops the Hadoop DataNode on the given host using the given cluster
   * configuration, as found in {@link HadoopPrms#names}.
   */
  public static synchronized void stopDataNode(String hadoopConfig,
                                               String host) {
    HadoopDescription hdd = getHadoopDescription(hadoopConfig);
    List<HadoopDescription.DataNodeDescription> dnds =
                            hdd.getDataNodeDescriptions();
    for (NodeDescription dnd : dnds) {
      if (dnd.getHostName().equals(host)) {
        stopDataNode(hdd, dnd);
        return;
      }
    }
    String s = "No data node found for host: " + host;
    throw new HydraRuntimeException(s);
  }

  /**
   * Stops the Hadoop DataNode using the given node configuration.
   */
  private static synchronized void stopDataNode(HadoopDescription hdd,
                                                NodeDescription nd) {
    HostDescription hd = nd.getHostDescription();
    String host = nd.getHostName();
    Log.getLogWriter().info("Stopping data node on host " + host);
    Integer spid = 0;
    Integer pid =0;
    String result = null;
    try {
      if (hdd.isSecure()) {
        Log.getLogWriter().info("Using secure mode (kerberos) as root");
        String spidfn = nd.getPIDDir() + "/hadoop_secure_dn.pid";
        File pidFile = new File(spidfn);
        if (pidFile.exists()){
        spid = readPID("stopDataNode", spidfn); // @todo only if file exists?
        }
        Log.getLogWriter().info("DataNode secure PID=" + spid);
        String cmd = "stopSecureDataNode " + hdd.getHadoopDist() 
                   + " " + nd.getConfDir();
        result = executeRootCommand(host, cmd, 120);
      } else {
        String pidfn = nd.getPIDDir() + "/hadoop-"
            + System.getProperty("user.name") + "-datanode.pid";
        File pidFile= new File(pidfn);
        if (pidFile.exists()){
        pid = readPID("stopDataNode", pidfn);
         }
        String cmd = SHELL + " " + hdd.getHadoopDist() + HADOOP_DAEMON
                   + " --config " + nd.getConfDir()
                   + " --script hdfs stop datanode";
        result = executeUserCommand(host, cmd, 120);
      }
    } catch (HydraRuntimeException e) {
      String s = "Failed to stop DataNode on " + host + ", see "
               + nd.getLogDir() + ": " + result;
      throw new HydraRuntimeException(s, e);
    }
    
    Log.getLogWriter().info("DataNode PID=" + pid);
    if (hdd.isSecure() && spid > 0) {
      removeHadoop("stopDataNode", hdd, nd, spid, true);
    } else {
      removeHadoop("stopDataNode", hdd, nd, pid, false);
    }

    if (hdd.getSecurityAuthentication().equals(HadoopPrms.KERBEROS_KINIT)) {
      executeKdestroyCommand(host, 120);
    }
    Log.getLogWriter().info("Stopped data node on host " + host
                            + ", see " + nd.getLogDir() + " for output");
  }

//------------------------------------------------------------------------------
// ResourceManager : start
//------------------------------------------------------------------------------

  /**
   * Starts the YARN ResourceManager using the given cluster configuration.
   * <p>
   * The manager is configured using the {@link HadoopDescription}
   * corresponding to the cluster configuration from {@link HadoopPrms#names}.
   */
  public static synchronized void startResourceManager(String hadoopConfig) {
    HadoopDescription hd = getHadoopDescription(hadoopConfig);
    HadoopDescription.ResourceManagerDescription rmd =
                      hd.getResourceManagerDescription();
    Log.getLogWriter().info("Starting YARN resource manager: " + rmd);
    startResourceManager(hd, rmd);
    Log.getLogWriter().info("Started YARN resource manager: " + rmd);
  }

  /**
   * Starts the YARN ResourceManager on the given host using the given cluster
   * configuration.
   * <p>
   * The manager is configured using the {@link HadoopDescription}
   * corresponding to the cluster configuration from {@link HadoopPrms#names}.
   */
  public static synchronized void startResourceManager(String hadoopConfig,
                                                       String host) {
    HadoopDescription hd = getHadoopDescription(hadoopConfig);
    HadoopDescription.ResourceManagerDescription rmd =
                            hd.getResourceManagerDescription();
    if (rmd.getHostName().equals(host)) {
      startResourceManager(hd, rmd);
      return;
    }
    String s = "No resource manager found for host: " + host;
    throw new HydraRuntimeException(s);
  }

  /**
   * Starts the YARN ResourceManager using the given node configuration.
   */
  public static synchronized void startResourceManager(HadoopDescription hdd,
                                                       NodeDescription nd) {
    String hadoopDist = hdd.getHadoopDist();
    String cmd = SHELL + " " + hadoopDist + YARN_DAEMON + " --config "
               + nd.getConfDir() + " start resourcemanager";
    String host = nd.getHostName();
    Log.getLogWriter().info("Starting YARN resource manager on host " + host
                            + " using command: " + cmd);
    HostDescription hd = nd.getHostDescription();
    String result = null;
    try {
      result = executeUserCommand(host, cmd, 120);
    } catch (HydraRuntimeException e) {
      String s = "Failed to start ResourceManager on " + host + ", see "
               + nd.getLogDir() + ": " + result;
      throw new HydraRuntimeException(s, e);
    }
    String pidfn = nd.getPIDDir() + "/yarn-"
                 + System.getProperty("user.name") + "-resourcemanager.pid";
    Integer pid = readPID("startResourceManager", pidfn);
    recordHadoop("startResourceManager", hdd, nd, pid, false);
    if (!ProcessMgr.processExists(host, pid)) {
      String s = "Failed to start resource manager on host " + host
               + " using command: " + cmd + ", see "
               + nd.getLogDir() + " for output";
      throw new HydraRuntimeException(s);
    }
    Log.getLogWriter().info("Started resource manager on host " + host
                            + " using command: " + cmd + ", see "
                            + nd.getLogDir() + " for output");
  }

//------------------------------------------------------------------------------
// ResourceManager : stop
//------------------------------------------------------------------------------

  /**
   * Stops the YARN ResourceManager using the given cluster configuration, as
   * found in {@link HadoopPrms#names}.
   */
  public static synchronized void stopResourceManager(String hadoopConfig) {
    HadoopDescription hd = getHadoopDescription(hadoopConfig);
    HadoopDescription.ResourceManagerDescription rmd =
                      hd.getResourceManagerDescription();
    Log.getLogWriter().info("Stopping resource manager: " + rmd);
    stopResourceManager(hd, rmd);
    Log.getLogWriter().info("Stopped resource manager: " + rmd);
  }

  /**
   * Stops the YARN ResourceManager on the given host using the given cluster
   * configuration, as found in {@link HadoopPrms#names}.
   */
  public static synchronized void stopResourceManager(String hadoopConfig,
                                                      String host) {
    HadoopDescription hd = getHadoopDescription(hadoopConfig);
    HadoopDescription.ResourceManagerDescription rmd =
                      hd.getResourceManagerDescription();
    if (rmd.getHostName().equals(host)) {
      stopResourceManager(hd, rmd);
      return;
    }
    String s = "No resource manager found for host: " + host;
    throw new HydraRuntimeException(s);
  }

  /**
   * Stops the YARN ResourceManager using the given node configuration.
   */
  private static synchronized void stopResourceManager(HadoopDescription hdd,
                                                       NodeDescription nd) {
    String cmd = SHELL + " " + hdd.getHadoopDist() + YARN_DAEMON + " --config "
               + nd.getConfDir() + " stop resourcemanager";
    String host = nd.getHostName();
    Log.getLogWriter().info("Stopping resource manager on host " + host
                            + " using command: " + cmd);
    HostDescription hd = nd.getHostDescription();
    String pidfn = nd.getPIDDir() + "/yarn-"
            + System.getProperty("user.name") + "-resourcemanager.pid";
     Integer pid = readPID("stopResourceManager", pidfn);

    String result = null;
    try {
      result = executeUserCommand(host, cmd, 120);
    } catch (HydraRuntimeException e) {
      String s = "Failed to stop resource manager on " + host + ", see "
               + nd.getLogDir() + ": " + result;
      throw new HydraRuntimeException(s, e);
    }
  
    Log.getLogWriter().info("ResourceManager pid=" + pid);
    removeHadoop("stopResourceManager", hdd, nd, pid, false);
    Log.getLogWriter().info("Stopped resource manager on host " + host
                            + " using command: " + cmd + ", see "
                            + nd.getLogDir() + " for output");
  }

//------------------------------------------------------------------------------
// NodeManagers : start
//------------------------------------------------------------------------------

  /**
   * Starts YARN NodeManagers using the given cluster configuration.
   * <p>
   * The managers are configured using the {@link HadoopDescription}
   * corresponding to the cluster configuration from {@link HadoopPrms#names}.
   */
  public static synchronized void startNodeManagers(String hadoopConfig) {
    HadoopDescription hdd = getHadoopDescription(hadoopConfig);
    List<HadoopDescription.NodeManagerDescription> nmds =
                           hdd.getNodeManagerDescriptions();
    Log.getLogWriter().info("Starting node managers: " + nmds);
    for (NodeDescription nmd : nmds) {
      startNodeManager(hdd, nmd);
    }
    Log.getLogWriter().info("Started node managers: " + nmds);
  }

  /**
   * Starts the YARN NodeManager on the given host using the given cluster
   * configuration.
   * <p>
   * The node is configured using the {@link HadoopDescription}
   * corresponding to the cluster configuration from {@link HadoopPrms#names}.
   */
  public static synchronized void startNodeManager(String hadoopConfig,
                                                   String host) {
    HadoopDescription hdd = getHadoopDescription(hadoopConfig);
    List<HadoopDescription.NodeManagerDescription> nmds =
                           hdd.getNodeManagerDescriptions();
    for (NodeDescription nmd : nmds) {
      if (nmd.getHostName().equals(host)) {
        startNodeManager(hdd, nmd);
        return;
      }
    }
    String s = "No node manager node found for host: " + host;
    throw new HydraRuntimeException(s);
  }

  /**
   * Starts the YARN NodeManager using the given node configuration.
   */
  public static synchronized void startNodeManager(HadoopDescription hdd,
                                                   NodeDescription nd) {
    HostDescription hd = nd.getHostDescription();
    String host = nd.getHostName();
    Log.getLogWriter().info("Starting node manager on host " + host);
    String result = null;
    try {
      if (hdd.isSecure()) {
        Log.getLogWriter().info("Using secure mode (kerberos) as root");
        String cmd = SHELL + " " + hdd.getHadoopDist() + YARN_DAEMON
                   + " --config " + nd.getConfDir() + " start nodemanager";
        result = executeUserCommand(host, cmd, 120);
        recordOwnershipCommand(hd, nd.getLogDir(),
                               System.getProperty("user.name"));
        recordReadPermissionCommand(hd, nd.getLogDir());
      } else {
        String cmd = SHELL + " " + hdd.getHadoopDist() + YARN_DAEMON
                   + " --config " + nd.getConfDir() + " start nodemanager";
        result = executeUserCommand(host, cmd, 120);
      }
    } catch (HydraRuntimeException e) {
      String s = "Failed to start node manager on " + host + ", see "
               + nd.getLogDir() + ": " + result;
      throw new HydraRuntimeException(s, e);
    }
    String pidfn = nd.getPIDDir() + "/yarn-"
                 + System.getProperty("user.name") + "-nodemanager.pid";
    Integer pid = readPID("startNodeManager", pidfn);
    Log.getLogWriter().info("NodeManager PID=" + pid);
    recordHadoop("startNodeManager", hdd, nd, pid, false);
    {
      boolean exists = ProcessMgr.processExists(host, pid);
      if (!exists) {
        String s = "Failed to start NodeManager on host " + host + ", see "
                 + nd.getLogDir();
        throw new HydraRuntimeException(s);
      }
    }
    Log.getLogWriter().info("Started node manager on host " + host
                            + ", see " + nd.getLogDir() + " for output");
  }

//------------------------------------------------------------------------------
// NodeManagers : stop
//------------------------------------------------------------------------------

  /**
   * Stops YARN NodeManagers using the given cluster configuration, as found in
   * {@link HadoopPrms#names}.
   */
  public static synchronized void stopNodeManagers(String hadoopConfig) {
    HadoopDescription hdd = getHadoopDescription(hadoopConfig);
    List<HadoopDescription.NodeManagerDescription> nmds =
                            hdd.getNodeManagerDescriptions();
    Log.getLogWriter().info("Stopping node managers: " + nmds);
    for (NodeDescription nmd : nmds) {
      stopNodeManager(hdd, nmd);
    }
    Log.getLogWriter().info("Stopped node managers: " + nmds);
  }

  /**
   * Starts the YARN NodeManager on the given host using the given cluster
   * configuration, as found in {@link HadoopPrms#names}.
   */
  public static synchronized void stopNodeManager(String hadoopConfig,
                                                  String host) {
    HadoopDescription hdd = getHadoopDescription(hadoopConfig);
    List<HadoopDescription.NodeManagerDescription> nmds =
                            hdd.getNodeManagerDescriptions();
    for (NodeDescription nmd : nmds) {
      if (nmd.getHostName().equals(host)) {
        stopNodeManager(hdd, nmd);
        return;
      }
    }
    String s = "No node manager found for host: " + host;
    throw new HydraRuntimeException(s);
  }

  /**
   * Stops the YARN NodeManager using the given node configuration.
   */
  private static synchronized void stopNodeManager(HadoopDescription hdd,
                                                   NodeDescription nd) {
    HostDescription hd = nd.getHostDescription();
    String host = nd.getHostName();
    Log.getLogWriter().info("Stopping node manager on host " + host);
    String pidfn = nd.getPIDDir() + "/yarn-"
            + System.getProperty("user.name") + "-nodemanager.pid";
    Integer pid = readPID("stopNodeManager", pidfn);
    String result = null;
    try {
      String cmd = SHELL + " " + hdd.getHadoopDist() + YARN_DAEMON
                 + " --config " + nd.getConfDir() + " stop nodemanager";
      result = executeUserCommand(host, cmd, 120);
    } catch (HydraRuntimeException e) {
      String s = "Failed to stop NodeManager on " + host + ", see "
               + nd.getLogDir() + ": " + result;
      throw new HydraRuntimeException(s, e);
    }


    Log.getLogWriter().info("NodeManager PID=" + pid);
    removeHadoop("stopNodeManager", hdd, nd, pid, false);
    Log.getLogWriter().info("Stopped data node on host " + host
                            + ", see " + nd.getLogDir() + " for output");
  }

//------------------------------------------------------------------------------
// Generic start
//------------------------------------------------------------------------------

  /**
   * Starts all Hadoop process using the given list of configurations. This is
   * a convenience method useful with the {@link RebootMgr} API. It is up to
   * the test to use this method in a thread-safe and timely fashion.
   */
  public static synchronized void startHadoopProcesses(
                                       List<HadoopInfo> hadoops) {
    startNameNodes(hadoops);
    startDataNodes(hadoops);
    startResourceManagers(hadoops);
    startNodeManagers(hadoops);
  }

  /**
   * Starts all name nodes in the given list of configurations. This is
   * a convenience method useful with the {@link RebootMgr} API. It is up to
   * the test to use this method in a thread-safe and timely fashion.
   */
  public static synchronized void startNameNodes(List<HadoopInfo> hadoops) {
    for (HadoopInfo hadoop : hadoops) {
      if (hadoop.getNodeDescription().getNodeType() == NodeType.NameNode) {
        startNameNode(hadoop.getHadoopDescription(),
                      hadoop.getNodeDescription());
      }
    }
  }

  /**
   * Starts all data nodes in the given list of configurations. This is
   * a convenience method useful with the {@link RebootMgr} API. It is up to
   * the test to use this method in a thread-safe and timely fashion.
   */
  public static synchronized void startDataNodes(List<HadoopInfo> hadoops) {
    for (HadoopInfo hadoop : hadoops) {
      if (hadoop.getNodeDescription().getNodeType() == NodeType.DataNode) {
        startDataNode(hadoop.getHadoopDescription(),
                      hadoop.getNodeDescription());
      }
    }
  }

  /**
   * Starts all resource managers in the given list of configurations. This is
   * a convenience method useful with the {@link RebootMgr} API. It is up to
   * the test to use this method in a thread-safe and timely fashion.
   */
  public static synchronized void startResourceManagers(
                                       List<HadoopInfo> hadoops) {
    for (HadoopInfo hadoop : hadoops) {
      if (hadoop.getNodeDescription().getNodeType() == NodeType.ResourceManager) {
        startResourceManager(hadoop.getHadoopDescription(),
                             hadoop.getNodeDescription());
      }
    }
  }

  /**
   * Starts all node managers in the given list of configurations. This is
   * a convenience method useful with the {@link RebootMgr} API. It is up to
   * the test to use this method in a thread-safe and timely fashion.
   */
  public static synchronized void startNodeManagers(List<HadoopInfo> hadoops) {
    for (HadoopInfo hadoop : hadoops) {
      if (hadoop.getNodeDescription().getNodeType() == NodeType.NodeManager) {
        startResourceManager(hadoop.getHadoopDescription(),
                             hadoop.getNodeDescription());
      }
    }
  }

  /**
   * Starts the Hadoop process using the given node configuration. This is a
   * convenience method useful with the {@link RebootMgr} API. It is up to
   * the test to use this method in a thread-safe and timely fashion.
   */
  public static synchronized void startHadoopProcess(HadoopDescription hdd,
                                                     NodeDescription nd) {
    switch (nd.getNodeType()) {
      case DataNode:
        startDataNode(hdd, nd);
        break;
      case NameNode:
        startNameNode(hdd, nd);
        break;
      case NodeManager:
        startNodeManager(hdd, nd);
        break;
      case ResourceManager:
        startResourceManager(hdd, nd);
        break;
      default:
        String s = "Should not happen";
        throw new HydraInternalException(s);
    }
  }

//------------------------------------------------------------------------------
// Hadoop commands
//------------------------------------------------------------------------------

  /**
   * Runs a Hadoop command using the given cluster configuration from {@link
   * HadoopPrms#names}. Writes the output to the specified file name.
   */
  public static synchronized void runHadoopCommand(String hadoopConfig,
                                           String command, String outfn) {
    HadoopDescription hdd = getHadoopDescription(hadoopConfig);
    runHadoopCommand(hdd, command, outfn);
  }

  /**
   * Runs a Hadoop command using the given HadoopDescription from {@link
   * HadoopPrms#names}. Writes the output to the specified file name.
   */
  public static synchronized void runHadoopCommand(HadoopDescription hdd,
                                           String command, String outfn) {
    NodeDescription nd = hdd.getNameNodeDescriptions().get(0);
    String cmd = SHELL + " " + hdd.getHadoopDist() + "/bin/hadoop --config "
               + nd.getConfDir() + " " + command;

    Log.getLogWriter().info("Running hadoop command: " + cmd);
    HostDescription hd = TestConfig.getInstance()
                   .getAnyPhysicalHostDescription(HostHelper.getLocalHost());
    int pid = ProcessMgr.bgexec(cmd, System.getProperty("user.dir"), outfn);
    recordPID("runHadoopCommand", hd, pid, false);
    int maxWaitSec = 180;
    if (!ProcessMgr.waitForDeath(hd.getHostName(), pid, maxWaitSec)) {
      String s = "Waited more than " + maxWaitSec
               + " seconds for " + cmd + " to be executed";
      throw new HydraTimeoutException(s);
    }
    removePID("runHadoopCommand", hd, pid, false);
    Log.getLogWriter().info("Ran hadoop command: " + cmd + ", see " + outfn);
  }

  /**
   * Executes a Hadoop command on given host using the given cluster configuration from {@link
   * HadoopPrms#names}. Writes the output to the specified file name.
   */
  public static synchronized void executeHadoopCommand(String host, String hadoopConfig,
                                           String command, String outfn) {
    HadoopDescription hdd = getHadoopDescription(hadoopConfig);
    executeHadoopCommand(host, hdd, command, outfn);
  }

  /**
   * Executes a Hadoop command on a given host using the given HadoopDescription from {@link
   * HadoopPrms#names}. Writes the output to the specified file name.
   */
  public static synchronized void executeHadoopCommand(String host, HadoopDescription hdd,
                                           String command, String outfn) {
    NodeDescription nd = hdd.getNameNodeDescriptions().get(0);
    String cmd = SHELL + " " + hdd.getHadoopDist() + "/bin/hadoop --config "
               + nd.getConfDir() + " " + command;
    Log.getLogWriter().info("Executing hadoop command: " + cmd + " on host " + host);
    HostDescription hd = TestConfig.getInstance()
                   .getAnyPhysicalHostDescription(host);
    int pid = ProcessMgr.bgexec(host, cmd, System.getProperty("user.dir"), outfn);
    recordPID("runHadoopCommand", hd, pid, false);
    int maxWaitSec = 180;
    if (!ProcessMgr.waitForDeath(hd.getHostName(), pid, maxWaitSec)) {
      String s = "Waited more than " + maxWaitSec
               + " seconds for " + cmd + " to be executed on host " + host;
      throw new HydraTimeoutException(s);
    }
    removePID("runHadoopCommand", hd, pid, false);
    Log.getLogWriter().info("Executed hadoop command: " + cmd + " on host " + host + ", see " + outfn);
  }

//------------------------------------------------------------------------------
// YARN commands
//------------------------------------------------------------------------------

  /**
   * Runs a YARN command using the given cluster configuration from {@link
   * HadoopPrms#names}. Writes the output to the specified file name.
   */
  public static synchronized void runYarnCommand(String hadoopConfig,
                                         String command, String outfn) {
    HadoopDescription hdd = getHadoopDescription(hadoopConfig);
    NodeDescription rmd = hdd.getResourceManagerDescription();

    String cmd = "env CLASSPATH=" + System.getProperty("java.class.path") + " "
               + hdd.getHadoopDist() + "/bin/yarn"
               + " --config " + rmd.getConfDir() + " " + command;
    Log.getLogWriter().info("Running yarn command: " + cmd);
    HostDescription hd = TestConfig.getInstance()
                   .getAnyPhysicalHostDescription(HostHelper.getLocalHost());
    int pid = ProcessMgr.bgexec(cmd, System.getProperty("user.dir"), outfn);
    recordPID("runYarnCommand", hd, pid, false);
    int maxWaitSec = 180;
    if (!ProcessMgr.waitForDeath(hd.getHostName(), pid, maxWaitSec)) {
      String s = "Waited more than " + maxWaitSec
               + " seconds for yarn " + cmd + " to be executed";
      throw new HydraTimeoutException(s);
    }
    removePID("runYarnCommand", hd, pid, false);
    Log.getLogWriter().info("Ran yarn command: " + cmd + ", see " + outfn);
  }

//------------------------------------------------------------------------------
// HadoopDescription
//------------------------------------------------------------------------------

  /**
   * Returns the {@link HadoopDescription} for the given cluster configuration.
   */
  public static HadoopDescription getHadoopDescription(String hadoopConfig) {
    return TestConfig.getInstance().getHadoopDescription(hadoopConfig);
  }

//------------------------------------------------------------------------------
// Utility methods
//------------------------------------------------------------------------------

  private static String readFile(String fn, HostDescription hd) {
    try {
      return FileUtil.getText(EnvHelper.expandEnvVars(fn, hd));
    } catch (FileNotFoundException e) {
      String s = fn + " not found";
      throw new HydraRuntimeException(s, e);
    } catch (IOException e) {
      String s = "Problem reading: " + fn;
      throw new HydraRuntimeException(s, e);
    }
  }

  private static String getRandomPort() {
    return String.valueOf(PortHelper.getRandomPort());
  }

  private static Integer readPID(String caller, String pidfn) {
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine(caller + " reading PID from: " + pidfn);
    }
    Integer pid = null;
    try {
      pid = new Integer(FileUtil.getContents(pidfn));
    } catch (NumberFormatException e) {
      String s = "Cannot read PID from file: " + pidfn;
      throw new HydraRuntimeException(s, e);
    } catch (IOException e) {
      String s = "Cannot read PID file: " + pidfn;
      throw new HydraRuntimeException(s, e);
    }
    return pid;
  }

  private static void recordHadoop(String caller, HadoopDescription hdd,
        NodeDescription nd, Integer pid, boolean secure) {
    try {
      if (Log.getLogWriter().fineEnabled()) {
        Log.getLogWriter().fine(caller + " recording PID: " + pid + " on host "
                               + nd.getHostDescription().getHostName());
      }
      RemoteTestModule.Master.recordHadoop(hdd, nd, pid, secure);
    } catch (RemoteException e) {
      String s = "Failed to record PID: " + pid;
      throw new HydraRuntimeException(s, e);
    }
  }

  private static void removeHadoop(String caller, HadoopDescription hdd,
        NodeDescription nd, Integer pid, boolean secure) {
    try {
      if (Log.getLogWriter().fineEnabled()) {
        Log.getLogWriter().fine(caller + " removing PID: " + pid + " on host "
                               + nd.getHostDescription().getHostName());
      }
      RemoteTestModule.Master.removeHadoop(hdd, nd, pid, secure);
    } catch (RemoteException e) {
      String s = "Failed to remove PID: " + pid;
      throw new HydraRuntimeException(s, e);
    }
  }

  private static void recordPID(String caller, HostDescription hd, Integer pid,
                                boolean secure) {
    try {
      if (Log.getLogWriter().fineEnabled()) {
        Log.getLogWriter().fine(caller + " recording PID: " + pid + " on host " + hd.getHostName());
      }
      RemoteTestModule.Master.recordHDFSPIDNoDumps(hd, pid, secure);
    } catch (RemoteException e) {
      String s = "Failed to record PID: " + pid;
      throw new HydraRuntimeException(s, e);
    }
  }

  private static void removePID(String caller, HostDescription hd, Integer pid,
                                boolean secure) {
    try {
      if (Log.getLogWriter().fineEnabled()) {
        Log.getLogWriter().fine(caller + " removing PID: " + pid + " on host " + hd.getHostName());
      }
      RemoteTestModule.Master.removeHDFSPIDNoDumps(hd, pid, secure);
    } catch (RemoteException e) {
      String s = "Failed to remove PID: " + pid;
      throw new HydraRuntimeException(s, e);
    }
  }

  /**
   * Answers whether the root process exists.
   */
  private static boolean processExists(String host, int pid) {
    String command = "processExists " + pid;
    String cmd = null;
    String result = null;
    try {
      cmd = getRootCommand(command);
      result = ProcessMgr.fgexec(host, cmd, 120);
      int exitCode = Integer.parseInt(result);
      return exitCode == 0;
    } catch (NumberFormatException e) {
      String s = "Unable to parse exit code for command: " + cmd
               + " from script output: \"" + result + "\"";
      throw new HydraRuntimeException(s, e);
    }
  }

  /**
   * Executes the command using the special script used to manage root
   * processes and files, on the specified host.
   * @throw HydraRuntimeException if the command has a non-zero exit code.
   */
  private static String executeRootCommand(String host, String command,
                                           int maxWaitSec) {
    String cmd = getRootCommand(command);
    return ProcessMgr.fgexec(host, cmd, maxWaitSec);
  }

  /**
   * Executes the command using as the current user.
   */
  private static String executeUserCommand(String host, String command,
                                           int maxWaitSec) {
    return ProcessMgr.fgexec(host, command, maxWaitSec);
  }

  /**
   * Executes "kinit" on the given command on the specified host.
   */
  public static void executeKinitCommand(String host, String command,
                                         int maxWaitSec) {
    String cmd = kinit() + " " + command;
    String result = null;
    try {
      result = executeUserCommand(host, cmd, 120);
    } catch (HydraRuntimeException e) {
      String s = "Failed to run " + cmd + " on " + host + ":" + result;
      throw new HydraRuntimeException(s, e);
    }
  }

  /**
   * Executes "kdestroy" on the specified host.
   */
  public static void executeKdestroyCommand(String host, int maxWaitSec) {
    String cmd = kdestroy();
    String result = null;
    try {
      result = executeUserCommand(host, cmd, 120);
    } catch (HydraRuntimeException e) {
      if (!e.getMessage().startsWith("kdestroy: No credentials cache found")) {
        String s = "Failed to run " + cmd + " on " + host + ":" + result;
        throw new HydraRuntimeException(s, e);
      }
    }
  }

  /**
   * Executes "hadoop command" on the given command on the specified host.
   */
  private static void executeHadoopCommand(String host, String command,
                                            int maxWaitSec) {
    String result = null;
    try {
      result = executeUserCommand(host, command, 120);
    } catch (HydraRuntimeException e) {
      String s = "Failed to run " + command + " on " + host + ":" + result;
      throw new HydraRuntimeException(s, e);
    }
  }

  /**
   * Gives ownership of the file on the host to the user.
   */
  private static void setOwnership(String fn, String host, String user) {
    Log.getLogWriter().info("Setting ownership of " + fn + " to " + user
                           + " on " + host);
    String cmd = "setOwnership " + user + " " + fn;
    String result = null;
    try {
      result = executeRootCommand(host, cmd, 120);
    } catch (HydraRuntimeException e) {
      String s = "Failed to set ownership of " + fn + " to " + user
               + " on " + host + ": " + result;
      throw new HydraRuntimeException(s, e);
    }
    Log.getLogWriter().info("Set ownership of " + fn + " to " + user
                           + " on " + host);
  }

  /**
   * Gives the current user read permission for the file on the host.
   */
  private static void setReadPermission(String fn, String host) {
    Log.getLogWriter().info("Setting read permission for " + fn
                           + " on " + host);
    String cmd = "setReadPermission " + fn;
    String result = null;
    try {
      result = executeRootCommand(host, cmd, 120);
    } catch (HydraRuntimeException e) {
      String s = "Failed to set read permission for " + fn + " on " + host
               + ": " + result;
      throw new HydraRuntimeException(s, e);
    }
    Log.getLogWriter().info("Set read permission for " + fn
                           + " on " + host);
  }

   /**
    * Gives the all users write permission for the file on the host.
    */
  private static void setWritePermission(String fn, String host) {
    Log.getLogWriter().info("Setting write permission for " + fn
                           + " on " + host);
    String cmd = "setWritePermission " + fn;
    String result = null;
    try {
      result = executeRootCommand(host, cmd, 120);
    } catch (HydraRuntimeException e) {
      String s = "Failed to set write permission for " + fn + " on " + host
               + ": " + result;
      throw new HydraRuntimeException(s, e);
    }
    Log.getLogWriter().info("Set write permission for " + fn
                           + " on " + host);
  }

  /**
   * Records the command to set ownership of the directory on the host to the
   * user in the moveHadoop.sh file.
   */
  private static void recordOwnershipCommand(HostDescription hd, String dir,
                                             String user) {
    recordRootCommand(hd, "setOwnership " + user + " " + dir, dir);
  }

  /**
   * Records the command to give the current user read permission for the
   * directory on the host in the moveHadoop.sh file.
   */
  private static void recordReadPermissionCommand(HostDescription hd,
                                                  String dir) {
    recordRootCommand(hd, "setReadPermission " + dir, dir);
  }

  /**
   * Records the root command in the moveHadoop.sh file.
   */
  private static void recordRootCommand(HostDescription hd, String cmd, String dir) {
    String rootCmd = getRootCommand(cmd);
    try {
      Log.getLogWriter().info("Recording root command on host "
                             + hd.getHostName() + ": " + rootCmd);
      RemoteTestModule.Master.recordRootCommand(hd, rootCmd, dir);
      Log.getLogWriter().info("Recorded root command on host "
                             + hd.getHostName() + ": " + rootCmd);
    } catch (RemoteException e) {
      String s = "Failed to record root command on host " + hd.getHostName()
               + ": " + rootCmd;
      throw new HydraRuntimeException(s, e);
    }
  }

  /**
   * Copies the src to the dst.
   */
  private static void copyFile(String src, String dst, String host) {
    Log.getLogWriter().info("Copying " + src + " to " + dst + " on " + host);
    String cmd = "copyFile " + src + " " + dst;
    String result = null;
    try {
      result = executeRootCommand(host, cmd, 120);
    } catch (HydraRuntimeException e) {
      String s = "Failed to copy " + src + " to " + dst + " on " + host + ": "
               + result;
      throw new HydraRuntimeException(s, e);
    }
    Log.getLogWriter().info("Copied " + src + " to " + dst + " on " + host);
  }

  private static String getRootCommand(String command) {
    return sudo() + " " + rootScript() + " " + command;
  }

  private static String rootScript() {
    if (!FileUtil.fileExists(ROOT_SCRIPT)) {
      String s = "Script needed to manage root processes and files not found: "
               + ROOT_SCRIPT;
      throw new HydraRuntimeException(s);
    }
    return ROOT_SCRIPT;
  }

  private static String kinit() {
    if (!FileUtil.fileExists(KINIT_CMD)) {
      String s = "Unable to find kinit: " + KINIT_CMD;
      throw new HydraRuntimeException(s);
    }
    return KINIT;
  }

  private static String kdestroy() {
    if (!FileUtil.fileExists(KDESTROY_CMD)) {
      String s = "Unable to find kdestroy: " + KDESTROY_CMD;
      throw new HydraRuntimeException(s);
    }
    return KDESTROY;
  }

  private static String sudo() {
    if (!FileUtil.exists(SUDO)) {
      String s = "Unable to find sudo: " + SUDO;
      throw new HydraRuntimeException(s);
    }
    return SUDO;
  }
}
