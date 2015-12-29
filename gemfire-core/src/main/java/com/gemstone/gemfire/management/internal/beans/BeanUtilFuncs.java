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
package com.gemstone.gemfire.management.internal.beans;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.management.GemFireProperties;
import com.gemstone.gemfire.management.internal.cli.CliUtil;

/**
 * Various Utility Functions to be used by MBeans
 * 
 * @author rishim
 * 
 */
public class BeanUtilFuncs {

  /**
   * returns the tail of the log file
   * 
   * @param logFile
   * @return tail of the log file
   * @throws IOException
   */
  public static String tailSystemLog(File logFile, final int numLines)
      throws IOException {
    if (logFile == null || logFile.equals(new File(""))) {
      return null;
    }
    int maxBuffer = 65500; // DataOutput.writeUTF will only accept 65535 bytes
    long fileLength = logFile.length();
    if (fileLength == 0) {
      return null;
    }
    byte[] buffer = (fileLength > maxBuffer) ? new byte[maxBuffer]
        : new byte[(int) fileLength];

    int readSize = buffer.length;
    RandomAccessFile f = new RandomAccessFile(logFile, "r");

    int linesRead = 0;
    long seekPosition = fileLength - readSize;
    StringBuilder returnStr = new StringBuilder();
    StringBuilder workingString = new StringBuilder();
    String separator = System.getProperty("line.separator");

    while (linesRead < numLines) {

      f.seek(seekPosition);
      f.read(buffer, 0, readSize);

      workingString.insert(0, new String(buffer, 0, readSize));

      String[] splits = workingString.toString().split("\\r?\\n");

      // The first element may be part of a line, so we don't count that
      // but we need to save it for the next time around.
      if (splits.length > 1) {
        for (int i = splits.length - 1; i > 0; i--) {
          returnStr.insert(0, separator).insert(0, splits[i]);
          if (++linesRead >= numLines) {
            break;
          }
        }
      }

      if (seekPosition == 0 && linesRead < numLines) {
        returnStr.insert(0, separator).insert(0, splits[0]);
        break;
      }

      workingString = new StringBuilder(splits[0]);

      if (readSize < seekPosition) {
        seekPosition = seekPosition - readSize;
      } else {
        readSize = (int) seekPosition;
        seekPosition = 0;
      }
    }

    f.close();

    returnStr.insert(0, separator).insert(0, "SystemLog:");

    return returnStr.toString();
  }
  
 
 

  /**
   * 
   * @param sc
   * @return tail of log
   * @throws IOException
   */
  public static String tailSystemLog(DistributionConfig sc, final int numLines) throws IOException {
    File logFile = sc.getLogFile();
    if (logFile == null || logFile.equals(new File(""))) {
      return null;
    }
    if (!logFile.isAbsolute()) {
      logFile = new File(logFile.getAbsolutePath());
    }
    return tailSystemLog(logFile, numLines);
  }
  
  public static DistributedMember getDistributedMemberByNameOrId(
      String memberNameOrId) {
    DistributedMember memberFound = null;

    if (memberNameOrId != null) {
      Cache cache = CacheFactory.getAnyInstance();
      Set<DistributedMember> memberSet = CliUtil.getAllMembers(cache);
      for (DistributedMember member : memberSet) {
        if (memberNameOrId.equals(member.getId())
            || memberNameOrId.equals(member.getName())) {
          memberFound = member;
          break;
        }
      }
    }
    return memberFound;
  }
  
  public static GemFireProperties initGemfireProperties(DistributionConfig config) {

    // **TODO **/
    String memberGroups = "";
    // **TODO **/
    String configFile = null;
    // ** TODO **//
    String includeFile = null;

    GemFireProperties gemFirePropertyData = new GemFireProperties();

    gemFirePropertyData.setMemberName(config.getName());
    gemFirePropertyData.setMemberGroups(memberGroups);
    gemFirePropertyData.setMcastPort(config.getMcastPort());
    gemFirePropertyData.setMcastAddress(config.getMcastAddress()
        .getHostAddress());
    gemFirePropertyData.setBindAddress(config.getBindAddress());
    gemFirePropertyData.setTcpPort(config.getTcpPort());
    gemFirePropertyData.setCacheXMLFile(config.getCacheXmlFile()
        .getAbsolutePath());
    gemFirePropertyData.setConfigFile(configFile);
    gemFirePropertyData.setMcastTTL(config.getMcastTtl());
    gemFirePropertyData.setServerBindAddress(config.getServerBindAddress());
    gemFirePropertyData.setLocators(config.getLocators());
    gemFirePropertyData.setStartLocator(config.getStartLocator());
    gemFirePropertyData.setLogFile(config.getLogFile().getAbsolutePath());
    gemFirePropertyData.setLogLevel(config.getLogLevel());
    gemFirePropertyData.setStatisticSamplingEnabled(config
        .getStatisticSamplingEnabled());

    gemFirePropertyData.setStatisticArchiveFile(config
        .getStatisticArchiveFile().getAbsolutePath());
    gemFirePropertyData.setIncludeFile(includeFile);
    gemFirePropertyData.setAckWaitThreshold(config.getAckWaitThreshold());

    gemFirePropertyData.setAckSevereAlertThreshold(config
        .getAckSevereAlertThreshold());

    gemFirePropertyData.setArchiveFileSizeLimit(config
        .getArchiveFileSizeLimit());

    gemFirePropertyData.setArchiveDiskSpaceLimit(config
        .getArchiveDiskSpaceLimit());
    gemFirePropertyData.setLogFileSizeLimit(config.getLogFileSizeLimit());
    gemFirePropertyData.setLogDiskSpaceLimit(config.getLogDiskSpaceLimit());

    gemFirePropertyData.setSslEnabled(config.getSSLEnabled());
    gemFirePropertyData.setSslCiphers(config.getSSLCiphers());
    gemFirePropertyData.setSslProtocols(config.getSSLProtocols());
    gemFirePropertyData.setSslRequireAuthentication(config
        .getSSLRequireAuthentication());
    gemFirePropertyData.setSocketLeaseTime(config.getSocketLeaseTime());

    gemFirePropertyData.setSocketBufferSize(config.getSocketBufferSize());
    gemFirePropertyData.setMcastSendBufferSize(config.getMcastSendBufferSize());
    gemFirePropertyData.setMcastRecvBufferSize(config.getMcastRecvBufferSize());

    gemFirePropertyData.setMcastByteAllowance(config.getMcastFlowControl()
        .getByteAllowance());
    gemFirePropertyData.setMcastRechargeThreshold(config.getMcastFlowControl()
        .getRechargeThreshold());
    gemFirePropertyData.setMcastRechargeBlockMs(config.getMcastFlowControl()
        .getRechargeBlockMs());
    gemFirePropertyData.setUdpFragmentSize(config.getUdpFragmentSize());

    gemFirePropertyData.setUdpRecvBufferSize(config.getUdpRecvBufferSize());
    gemFirePropertyData.setDisableTcp(config.getDisableTcp());

    gemFirePropertyData.setEnableTimeStatistics(config
        .getEnableTimeStatistics());

    gemFirePropertyData.setEnableNetworkPartitionDetection(config
        .getEnableNetworkPartitionDetection());

    gemFirePropertyData.setMemberTimeout(config.getMemberTimeout());
    gemFirePropertyData.setMembershipPortRange(config.getMembershipPortRange());
    gemFirePropertyData.setConserveSockets(config.getConserveSockets());

    gemFirePropertyData.setRoles(config.getRoles());
    gemFirePropertyData.setMaxWaitTimeForReconnect(config
        .getMaxWaitTimeForReconnect());

    gemFirePropertyData.setMaxNumReconnectTries(config
        .getMaxNumReconnectTries());

    gemFirePropertyData.setAsyncDistributionTimeout(config
        .getAsyncDistributionTimeout());

    gemFirePropertyData.setAsyncQueueTimeout(config.getAsyncQueueTimeout());
    gemFirePropertyData.setAsyncMaxQueueSize(config.getAsyncMaxQueueSize());
    gemFirePropertyData.setClientConflation(config.getClientConflation());

    gemFirePropertyData.setDurableClientId(config.getDurableClientId());
    gemFirePropertyData.setDurableClientTimeout(config
        .getDurableClientTimeout());
    gemFirePropertyData.setSecurityClientAuthInit(config
        .getSecurityClientAuthInit());
    gemFirePropertyData.setSecurityClientAuthenticator(config
        .getSecurityClientAuthenticator());
    gemFirePropertyData.setSecurityClientDHAlgo(config
        .getSecurityClientDHAlgo());
    gemFirePropertyData.setSecurityPeerAuthInit(config
        .getSecurityPeerAuthInit());
    gemFirePropertyData.setSecurityPeerAuthenticator(config
        .getSecurityPeerAuthenticator());
    gemFirePropertyData.setSecurityClientAccessor(config
        .getSecurityClientAccessor());
    gemFirePropertyData.setSecurityClientAccessorPP(config
        .getSecurityClientAccessorPP());
    gemFirePropertyData.setSecurityLogLevel(config.getSecurityLogLevel());

    gemFirePropertyData.setSecurityLogFile(config.getSecurityLogFile()
        .getAbsolutePath());

    gemFirePropertyData.setSecurityPeerMembershipTimeout(config
        .getSecurityPeerMembershipTimeout());

    gemFirePropertyData.setRemoveUnresponsiveClient(config
        .getRemoveUnresponsiveClient());

    gemFirePropertyData.setDeltaPropagation(config.getDeltaPropagation());
    gemFirePropertyData.setRedundancyZone(config.getRedundancyZone());
    gemFirePropertyData.setEnforceUniqueHost(config.getEnforceUniqueHost());

    gemFirePropertyData.setStatisticSampleRate(config.getStatisticSampleRate());
    gemFirePropertyData.setUdpSendBufferSize(config.getUdpSendBufferSize());

    gemFirePropertyData.setJmxManager(config.getJmxManager());
    gemFirePropertyData.setJmxManagerStart(config.getJmxManagerStart());
    gemFirePropertyData.setJmxManagerSSL(config.getJmxManagerSSL());
    gemFirePropertyData.setJmxManagerPort(config.getJmxManagerPort());
    gemFirePropertyData.setJmxManagerBindAddress(config.getJmxManagerBindAddress());
    gemFirePropertyData.setJmxManagerHostnameForClients(config.getJmxManagerHostnameForClients());
    gemFirePropertyData.setJmxManagerPasswordFile(config.getJmxManagerPasswordFile());
    gemFirePropertyData.setJmxManagerAccessFile(config.getJmxManagerAccessFile());
    gemFirePropertyData.setJmxManagerHttpPort(config.getJmxManagerHttpPort());
    gemFirePropertyData.setJmxManagerUpdateRate(config.getJmxManagerUpdateRate());

    return gemFirePropertyData;

  }
  

  /**
   * Compresses a given String. It is encoded using ISO-8859-1, So any
   * decompression of the compressed string should also use ISO-8859-1
   * 
   * @param str
   *          String to be compressed.
   * @return compressed bytes
   * @throws IOException
   */
  public static byte[] compress(String str) throws IOException {
    if (str == null || str.length() == 0) {
      return null;
    }
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    GZIPOutputStream gzip = new GZIPOutputStream(out);
    gzip.write(str.getBytes("UTF-8"));
    gzip.close();
    byte[] outBytes = out.toByteArray();
    return outBytes;
  }

  /**
   * 
   * @param bytes
   *          bytes to be decompressed
   * @return a decompressed String
   * @throws IOException
   */
  public static String decompress(byte[] bytes) throws IOException {
    if (bytes == null || bytes.length == 0) {
      return null;
    }
    GZIPInputStream gis = new GZIPInputStream(new ByteArrayInputStream(bytes));
    BufferedReader bf = new BufferedReader(new InputStreamReader(gis,"UTF-8"));
    String outStr = "";
    String line;
    while ((line = bf.readLine()) != null) {
      outStr += line;
    }
    return outStr;
  }
  
}
