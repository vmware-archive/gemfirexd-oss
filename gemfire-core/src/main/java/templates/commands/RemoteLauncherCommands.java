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
package templates.commands;

import java.io.IOException;
import java.io.InputStream;

import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.management.cli.CliMetaData;
import com.gemstone.gemfire.management.cli.ConverterHint;
import com.gemstone.gemfire.management.cli.Result;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.UserInfo;

/**
 * Gfsh commands to start a Server or Locator on a remote host using SSH.
 * 
 * @author David Hoots
 * @since 7.5
 */
public class RemoteLauncherCommands implements CommandMarker {

  @CliCommand(value = "start remote-locator", help = "Start a Locator on a remote host using SSH.")
  @CliMetaData(shellOnly = true, relatedTopic = { "Locator", "Lifecycle" })
  public Result startRemoteLocator(@CliOption(key = "host",
                    mandatory = true,
                    help = "Host on which to start the server.")
         final String host,
         @CliOption(key = "user",
                    mandatory = true,
                    help = "User to login to the host with.")
         final String user,
         @CliOption(key = "password",
                    unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                    help = "Password for the user.  If not specifed SSH keys must have been installed to support password-less login.")
         final String password,
         @CliOption(key = "gemfire-bin",
                    unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                    help = "Path to GemFire's \"bin\" directory.  If not specifed \"gfsh\" must be in the command search path.")
         final String gemfireBin,@CliOption(key = "name",
                    mandatory = true,
                    unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                    help = "Member name for this Locator service.")
         final String memberName,
         @CliOption(key = "bind-address",
                    unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                    help = "IP address on which the Locator will be bound. The default is to bind to all local addresses.")
         final String bindAddress,
         @CliOption(key = "force",
                    unspecifiedDefaultValue = "false",
                    specifiedDefaultValue = "true",
                    help = "Whether to allow the PID file from a previous Locator run to be overwritten.")
         final Boolean force,
         @CliOption(key = "group",
                    optionContext = ConverterHint.MEMBERGROUP,
                    unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                    help = "Group(s) the Locator will be a part of.")
         final String group,
         @CliOption(key = "hostname-for-clients",
                    unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                    help = "Hostname or IP address that will be sent to clients so they can connect to this Locator. The default is the bind-address of the Locator.")
         final String hostnameForClients,
         @CliOption(key = "locators",
                    unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                    help = "Sets the list of Locators used by this Locator to join the appropriate GemFire cluster.")
         final String locators,
         @CliOption(key = "log-level",
                    optionContext = ConverterHint.LOG_LEVEL,
                    unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                    help = "Sets the level of output logged to the Locator log file.  Possible values for log-level include: finest, finer, fine, config, info, warning, severe, none.")
         final String logLevel,
         @CliOption(key = "mcast-address",
                    unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                    help = "The IP address or hostname used to bind the UPD socket for multi-cast networking so the Locator can locate other members in the GemFire cluster.  If mcast-port is zero, then mcast-address is ignored.")
         final String mcastBindAddress,
         @CliOption(key = "mcast-port",
                    unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                    help = "Sets the port used for multi-cast networking so the Locator can locate other members of the GemFire cluster.  A zero value disables mcast.")
         final Integer mcastPort,
         @CliOption(key = "port",
                    unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                    help = "Port the Locator will listen on.")
         final Integer port,
         @CliOption(key = "dir",
                    optionContext = ConverterHint.DIR_PATHSTRING,
                    unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                    help = "Directory in which the Locator will be run. The default is the current directory.")
         String workingDirectory,
         @CliOption(key = "properties-file",
                    optionContext = ConverterHint.FILE_PATHSTRING,
                    unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                    help = "The gemfire.properties file for configuring the Locator's distributed system. The file's path can be absolute or relative to the Locator's directory (--dir=).")
         final String gemfirePropertiesPathname,
         @CliOption(key = "initial-heap",
                    unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                    help = "Initial size of the heap in the same format as the JVM -Xms parameter.")
         final String initialHeap,
         @CliOption(key = "max-heap",
                    unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                    help = "Maximum size of the heap in the same format as the JVM -Xmx parameter.")
         final String maxHeap,
         @CliOption(key = "J",
                    optionContext = ConverterHint.STRING_LIST,
                    unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                    help = "Argument passed to the JVM on which the Locator will run. For example, --J=-Dfoo.bar=true will set the property \"foo.bar\" to \"true\".")
         @CliMetaData(valueSeparator = ",")
         final String[] jvmArgsOpts)
 {
    StringBuilder stringBuilder = new StringBuilder();
    
    if (gemfireBin != null) {
      stringBuilder.append("cd ").append(gemfireBin).append("; ");
    }
    stringBuilder.append("gfsh start locator");

    if (memberName != null && !memberName.isEmpty()) {
      stringBuilder.append(" --name=").append(memberName);
    }
    if (bindAddress != null && !bindAddress.isEmpty()) {
      stringBuilder.append(" --bind-address=").append(bindAddress);
    }
    if (force != null) {
      stringBuilder.append(" --force=").append(force);
    }
    if (group != null && !group.isEmpty()) {
      stringBuilder.append(" --group=").append(group);
    }
    if (hostnameForClients != null && !hostnameForClients.isEmpty()) {
      stringBuilder.append(" --hostname-for-clients=").append(hostnameForClients);
    }
    if (locators != null && !locators.isEmpty()) {
      stringBuilder.append(" --locators=").append(locators);
    }
    if (logLevel != null && !logLevel.isEmpty()) {
      stringBuilder.append(" --log-level=").append(logLevel);
    }
    if (mcastBindAddress != null && !mcastBindAddress.isEmpty()) {
      stringBuilder.append(" --mcast-address=").append(mcastBindAddress);
    }
    if (mcastPort != null) {
      stringBuilder.append(" --mcast-port=").append(mcastPort);
    }
    if (port != null) {
      stringBuilder.append(" --port=").append(port);
    }
    if (workingDirectory != null && !workingDirectory.isEmpty()) {
      stringBuilder.append(" --dir=").append(workingDirectory);
    }
    if (gemfirePropertiesPathname != null && !gemfirePropertiesPathname.isEmpty()) {
      stringBuilder.append(" --properties-file=").append(gemfirePropertiesPathname);
    }
    if (initialHeap != null && !initialHeap.isEmpty()) {
      stringBuilder.append(" --initial-heap=").append(initialHeap);
    }
    if (maxHeap != null && !maxHeap.isEmpty()) {
      stringBuilder.append(" --max-heap=").append(maxHeap);
    }

    if (jvmArgsOpts != null) {
      for (String jvmArgsOpt : jvmArgsOpts) {
        stringBuilder.append("--J=").append(jvmArgsOpt);
      }
    }
    
    String command = stringBuilder.toString();
    return executeSshCommand(host, user, password, command);
 }

  @CliCommand(value = "start remote-server", help = "Start a Server on a remote host using SSH.")
  @CliMetaData(shellOnly = true, relatedTopic = { "Server", "Lifecycle" })
  public Result startServer(@CliOption(key = "host",
                    mandatory = true,
                    help = "Host on which to start the server.")
          final String host,
          @CliOption(key = "user",
                    mandatory = true,
                    help = "User to login to the host with.")
          final String user,
          @CliOption(key = "password",
                    unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                    help = "Password for the user.  If not specifed SSH keys must have been installed to support password-less login.")
          final String password,
          @CliOption(key = "gemfire-bin",
                    unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                    help = "Path to GemFire's \"bin\" directory.  If not specifed \"gfsh\" must be in the command search path.")
          final String gemfireBin,
          @CliOption(key = "assign-buckets",
                    unspecifiedDefaultValue = "false",
                    specifiedDefaultValue = "true",
                    help = "Whether to assign buckets to the partitioned regions of the cache on server start.")
          final Boolean assignBuckets,
          @CliOption(key = "cache-xml-file",
                    optionContext = ConverterHint.FILE_PATHSTRING,
                    unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                    help = "Specifies the name of the XML file or resource to initialize the cache with when it is created.")
          final String cacheXmlPathname,
          @CliOption(key = "classpath",
                    unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                    help = "Location of user classes required by the Cache Server. This path is appended to the current classpath.")
          final String classpath,
          @CliOption(key = "disable-default-server",
                    unspecifiedDefaultValue = "false",
                    specifiedDefaultValue = "true",
                    help = "Whether the Cache Server will be started by default.")
          final Boolean disableDefaultServer,
          @CliOption(key = "enable-time-statistics",
                    unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                    specifiedDefaultValue = "true",
                    help = "Causes additional time-based statistics to be gathered for GemFire operations.")
          final Boolean enableTimeStatistics,
          @CliOption(key = "force",
                    unspecifiedDefaultValue = "false",
                    specifiedDefaultValue = "true",
                    help = "Whether to allow the PID file from a previous Cache Server run to be overwritten.")
          final Boolean force,
          @CliOption(key = "properties-file",
                    optionContext = ConverterHint.FILE_PATHSTRING,
                    unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                    help = "The gemfire.properties file for configuring the Cache Server's distributed system. The file's path can be absolute or relative to the Locator's directory (--dir=).")
          final String gemfirePropertiesPathname,
          @CliOption(key = "group",
                    optionContext = ConverterHint.MEMBERGROUP,
                    unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                    help = "Group(s) the Cache Server will be a part of.")
          final String group,
          @CliOption(key = "locators",
                    unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                    help = "Sets the list of Locators used by the Cache Server to join the appropriate GemFire cluster.")
          final String locators,
          @CliOption(key = "log-level",
                    optionContext = ConverterHint.LOG_LEVEL,
                    unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                    help = "Sets the level of output logged to the Cache Server log file.  Possible values for log-level include: finest, finer, fine, config, info, warning, severe, none.")
          final String logLevel,
          @CliOption(key = "mcast-address",
                    unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                    help = "The IP address or hostname used to bind the UPD socket for multi-cast networking so the Cache Server can locate other members in the GemFire cluster.  If mcast-port is zero, then mcast-address is ignored.")
          final String mcastBindAddress,
          @CliOption(key = "mcast-port",
                    unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                    help = "Sets the port used for multi-cast networking so the Cache Server can locate other members of the GemFire cluster.  A zero value disables mcast.")
          final Integer mcastPort,
          @CliOption(key = "name",
                    mandatory = true,
                    unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                    help = "Member name for this Cache Server service.")
          final String memberName,
          @CliOption(key = "memcached-port",
                    unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                    help = "Sets the port that the GemFire memcached service listens on for memcached clients.")
          final Integer memcachedPort,
          @CliOption(key = "memcached-protocol",
                    unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                    help = "Sets the protocol that the GemFire memcached service uses (ASCII or BINARY).")
          final String memcachedProtocol,
          @CliOption(key = "rebalance",
                    unspecifiedDefaultValue = "false",
                    specifiedDefaultValue = "true",
                    help = "Whether to initiate rebalancing across the GemFire cluster.")
          final Boolean rebalance,
          @CliOption(key = "server-bind-address",
                    unspecifiedDefaultValue = CacheServer.DEFAULT_BIND_ADDRESS,
                    help = "The IP address that this distributed system's server sockets in a client-server topology will be bound. If set to an empty string then all of the local machine's addresses will be listened on.")
          final String serverBindAddress,
          @CliOption(key = "server-port",
                    unspecifiedDefaultValue = ("" + CacheServer.DEFAULT_PORT),
                    help = "The port that the distributed system's server sockets in a client-server topology will listen on.  The default server-port is "
                        + CacheServer.DEFAULT_PORT + ".")
          final Integer serverPort,
          @CliOption(key = "statistic-archive-file",
                    unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                    help = "The file that statistic samples are written to.  An empty string (default) disables statistic archival.")
          final String statisticsArchivePathname,
          @CliOption(key = "dir",
                    optionContext = ConverterHint.DIR_PATHSTRING,
                    unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                    help = "Directory in which the Server will be run. The default is the current directory.")
          String workingDirectory,
          @CliOption(key = "initial-heap",
                   unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                   help = "Initial size of the heap in the same format as the JVM -Xms parameter.")
          final String initialHeap,
          @CliOption(key = "max-heap",
                   unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                   help = "Maximum size of the heap in the same format as the JVM -Xmx parameter.")
          final String maxHeap,
          @CliOption(key = "J",
                  optionContext = ConverterHint.STRING_LIST,
                  unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
                  help = "Argument passed to the JVM on which the Locator will run. For example, --J=-Dfoo.bar=true will set the property \"foo.bar\" to \"true\".")
          @CliMetaData(valueSeparator = ",")
          final String[] jvmArgsOpts)
  {

    StringBuilder stringBuilder = new StringBuilder();
    
    if (gemfireBin != null) {
      stringBuilder.append("cd ").append(gemfireBin).append("; ");
    }
    stringBuilder.append("gfsh start server");
    
    if (assignBuckets != null) {
      stringBuilder.append(" --assign-buckets=").append(assignBuckets);
    }
    if (cacheXmlPathname != null && !cacheXmlPathname.isEmpty()) {
      stringBuilder.append(" --cache-xml-file=").append(cacheXmlPathname);
    }
    if (classpath != null && !classpath.isEmpty()) {
      stringBuilder.append(" --classpath=").append(classpath);
    }
    if (disableDefaultServer != null) {
      stringBuilder.append(" --disable-default-server=").append(disableDefaultServer);
    }
    if (enableTimeStatistics != null) {
      stringBuilder.append(" --enable-time-statistics=").append(enableTimeStatistics);
    }
    if (force != null) {
      stringBuilder.append(" --force=").append(force);
    }
    if (gemfirePropertiesPathname != null && !gemfirePropertiesPathname.isEmpty()) {
      stringBuilder.append(" --properties-file=").append(gemfirePropertiesPathname);
    }
    if (group != null && !group.isEmpty()) {
      stringBuilder.append(" --group=").append(group);
    }
    if (locators != null && !locators.isEmpty()) {
      stringBuilder.append(" --locators=").append(locators);
    }
    if (logLevel != null && !logLevel.isEmpty()) {
      stringBuilder.append(" --log-level=").append(logLevel);
    }
    if (mcastBindAddress != null && !mcastBindAddress.isEmpty()) {
      stringBuilder.append(" --mcast-address=").append(mcastBindAddress);
    }
    if (mcastPort != null) {
      stringBuilder.append(" --mcast-port=").append(mcastPort);
    }
    if (memberName != null && !memberName.isEmpty()) {
      stringBuilder.append(" --name=").append(memberName);
    }
    if (memcachedPort != null) {
      stringBuilder.append(" --memcached-port=").append(memcachedPort);
    }
    if (memcachedProtocol != null && !memcachedProtocol.isEmpty()) {
      stringBuilder.append(" --memcached-protocol=").append(memcachedProtocol);
    }
    if (rebalance != null) {
      stringBuilder.append(" --rebalance=").append(rebalance);
    }
    if (serverBindAddress != null && !serverBindAddress.isEmpty()) {
      stringBuilder.append(" --server-bind-address=").append(serverBindAddress);
    }
    if (serverPort != null) {
      stringBuilder.append(" --server-port=").append(serverPort);
    }
    if (statisticsArchivePathname != null && !statisticsArchivePathname.isEmpty()) {
      stringBuilder.append(" --statistic-archive-file=").append(statisticsArchivePathname);
    }
    if (workingDirectory != null && !workingDirectory.isEmpty()) {
      stringBuilder.append(" --dir=").append(workingDirectory);
    }
    if (initialHeap != null && !initialHeap.isEmpty()) {
      stringBuilder.append(" --initial-heap=").append(initialHeap);
    }
    if (maxHeap != null && !maxHeap.isEmpty()) {
      stringBuilder.append(" --max-heap=").append(maxHeap);
    }
    
    if (jvmArgsOpts != null) {
      for (String jvmArgsOpt : jvmArgsOpts) {
        stringBuilder.append("--J=").append(jvmArgsOpt);
      }
    }

    String command = stringBuilder.toString();
    return executeSshCommand(host, user, password, command);
  }

  /**
   * Indicates that these commands are available at all times, regardless
   * connection status.
   */
  @CliAvailabilityIndicator({"start remote-locator", "start remote-server"})
  public boolean launcherCommandsAvailable() {
    return true;
  }
  
  /**
   * Create a simple Result object that will hold the provided message.
   * 
   * @param status
   *          Whether the commands was successful.
   * @param message
   *          Message to include in the result.
   * @return The newly created result.
   */
  private Result createResult(final Result.Status status, final String message) {
    return new Result() {
      private boolean done = false;;

      @Override
      public Status getStatus() {
        return status;
      }

      @Override
      public void resetToFirstLine() {
        this.done = true;
      }

      @Override
      public boolean hasNextLine() {
        return !this.done;
      }

      @Override
      public String nextLine() {
        this.done = true;
        return message;
      }

      @Override
      public boolean hasIncomingFiles() {
        return false;
      }

      @Override
      public void saveIncomingFiles(String directory) throws IOException {
        // No incoming files to handle
      }
    };
  }
  
  /**
   * Create a UserInfo object for use by the JSCH library.
   * 
   * @param password
   *          Password to use when connecting using JSCH.
   * @return The new created UserInfo object.
   */
  private UserInfo createUserInfo(final String password) {

    return new UserInfo() {
      @Override
      public String getPassword() {
        return password;
      }

      @Override
      public boolean promptYesNo(String str) {
        return true;
      }

      @Override
      public String getPassphrase() {
        return null;
      }

      @Override
      public boolean promptPassphrase(String message) {
        return false;
      }

      @Override
      public boolean promptPassword(String message) {
        return true;
      }

      @Override
      public void showMessage(String message) {
        // This will never run interactively
      }
    };
  }

  /**
   * Connect to a remote host via SSH and execute a command.
   * 
   * @param host
   *          Host to connect to
   * @param user
   *          User to login with
   * @param password
   *          Password for the user
   * @param command
   *          Command to execute
   * @return The result of the command execution
   */
  private Result executeSshCommand(final String host, final String user,
      final String password, final String command) {
    
    StringBuilder result = new StringBuilder();
    
    try {
      JSch jsch = new JSch();
      Session session = jsch.getSession(user, host, 22);
      session.setUserInfo(createUserInfo(password));
      session.connect(5000);

      ChannelExec channel = (ChannelExec) session.openChannel("exec");
      channel.setCommand(command);
      channel.setInputStream(null);
      channel.setErrStream(System.err);
      InputStream in = channel.getInputStream();

      channel.connect();

      byte[] tmp = new byte[1024];
      while (true) {
        while (in.available() > 0) {
          int i = in.read(tmp, 0, 1024);
          if (i < 0)
            break;
          result.append(new String(tmp, 0, i));
        }
        if (channel.isClosed()) {
          break;
        }
      }
      channel.disconnect();
      session.disconnect();
    } catch (Exception jex) {
      return createResult(Result.Status.ERROR, jex.getMessage());
    }
 
    return createResult(Result.Status.OK, result.toString());
  }
}
