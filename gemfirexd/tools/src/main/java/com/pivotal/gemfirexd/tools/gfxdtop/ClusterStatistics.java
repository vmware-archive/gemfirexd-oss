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
package com.pivotal.gemfirexd.tools.gfxdtop;

import static com.pivotal.gemfirexd.tools.gfxdtop.JMXHelper.getLongAttribute;
import static com.pivotal.gemfirexd.tools.gfxdtop.JMXHelper.isQuoted;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

public class ClusterStatistics {

  private ObjectName distrStatementON;
  private MBeanServerConnection mbs;

  public MBeanServerConnection getMbs() {
    return mbs;
  }

  public void setMbs(MBeanServerConnection mbs) {    
    this.mbs = mbs;
  }

  private GfxdLogger logger = new GfxdLogger();
  private Map<String, Statement> statementMap = new HashMap<String, Statement>();
  // private ReadWriteLock lock = new ReentrantReadWriteLock();

  public ClusterStatistics() {
    try {
      distrStatementON = new ObjectName(GfxdConstants.OBJECT_NAME_STATEMENT_DISTRIBUTED);
    } catch (MalformedObjectNameException e) {
      throw new RuntimeException(e);
    } catch (NullPointerException e) {
      throw new RuntimeException(e);
    }
  }

  public synchronized void queryClusterStatistics() {
    Set<ObjectName> statementObjectNames;
    try {
      statementObjectNames = this.mbs.queryNames(this.distrStatementON, null);
      //logger.info("queryClusterStatistics : statementObjectNames = " + statementObjectNames.size());
      for (ObjectName stmtObjectName : statementObjectNames) {
        updateClusterStatement(stmtObjectName);
      }
      //logger.info("queryClusterStatistics : updated " + statementObjectNames.size() + " statements");
    } catch (IOException e) {
      logger.warning(e);
    }
  }

  private void updateClusterStatement(ObjectName mbeanName) throws IOException {

    try {
      AttributeList attributeList = this.mbs.getAttributes(mbeanName, GfxdConstants.STATEMENT_MBEAN_ATTRIBUTES);
      String statementDefinition = mbeanName.getKeyProperty("name");
      if (isQuoted(statementDefinition)) {
        statementDefinition = ObjectName.unquote(statementDefinition);
      }
      
      Statement statement = statementMap.get(statementDefinition);

      if (null == statement) {
        statement = new Statement();
        statement.setQueryDefinition(statementDefinition);
      }

      for (int i = 0; i < attributeList.size(); i++) {
       Attribute attribute = (Attribute) attributeList.get(i);
        updateAttribute(statement,attribute);
      }

      addClusterStatement(statementDefinition, statement);
    } catch (InstanceNotFoundException infe) {
      logger.warning(infe);
    } catch (ReflectionException re) {
      logger.warning(re);
    }
  }

  private void updateAttribute(Statement statement, Attribute attribute) {
    if (attribute.getName().equals(GfxdConstants.MBEAN_ATTRIBUTE_NUMTIMESCOMPILED)) {
      statement.setNumTimesCompiled(getLongAttribute(attribute.getValue(), attribute.getName()));
    } else if (attribute.getName().equals(GfxdConstants.MBEAN_ATTRIBUTE_NUMEXECUTION)) {
      statement.setNumExecution(getLongAttribute(attribute.getValue(), attribute.getName()));
    } else if (attribute.getName().equals(GfxdConstants.MBEAN_ATTRIBUTE_NUMEXECUTIONSINPROGRESS)) {
      statement.setNumExecutionsInProgress(getLongAttribute(attribute.getValue(), attribute.getName()));
    } else if (attribute.getName().equals(GfxdConstants.MBEAN_ATTRIBUTE_NUMTIMESGLOBALINDEXLOOKUP)) {
      statement.setNumTimesGlobalIndexLookup(getLongAttribute(attribute.getValue(), attribute.getName()));
    } else if (attribute.getName().equals(GfxdConstants.MBEAN_ATTRIBUTE_NUMROWSMODIFIED)) {
      statement.setNumRowsModified(getLongAttribute(attribute.getValue(), attribute.getName()));
    } else if (attribute.getName().equals(GfxdConstants.MBEAN_ATTRIBUTE_PARSETIME)) {
      statement.setParseTime(getLongAttribute(attribute.getValue(), attribute.getName()));
    } else if (attribute.getName().equals(GfxdConstants.MBEAN_ATTRIBUTE_BINDTIME)) {
      statement.setBindTime(getLongAttribute(attribute.getValue(), attribute.getName()));
    } else if (attribute.getName().equals(GfxdConstants.MBEAN_ATTRIBUTE_OPTIMIZETIME)) {
      statement.setOptimizeTime(getLongAttribute(attribute.getValue(), attribute.getName()));
    } else if (attribute.getName().equals(GfxdConstants.MBEAN_ATTRIBUTE_ROUTINGINFOTIME)) {
      statement.setRoutingInfoTime(getLongAttribute(attribute.getValue(), attribute.getName()));
    } else if (attribute.getName().equals(GfxdConstants.MBEAN_ATTRIBUTE_GENERATETIME)) {
      statement.setGenerateTime(getLongAttribute(attribute.getValue(), attribute.getName()));
    } else if (attribute.getName().equals(GfxdConstants.MBEAN_ATTRIBUTE_TOTALCOMPILATIONTIME)) {
      statement.setTotalCompilationTime(getLongAttribute(attribute.getValue(), attribute.getName()));
    } else if (attribute.getName().equals(GfxdConstants.MBEAN_ATTRIBUTE_EXECUTIONTIME)) {
      statement.setExecutionTime(getLongAttribute(attribute.getValue(), attribute.getName()));
    } else if (attribute.getName().equals(GfxdConstants.MBEAN_ATTRIBUTE_PROJECTIONTIME)) {
      statement.setProjectionTime(getLongAttribute(attribute.getValue(), attribute.getName()));
    } else if (attribute.getName().equals(GfxdConstants.MBEAN_ATTRIBUTE_TOTALEXECUTIONTIME)) {
      statement.setTotalExecutionTime(getLongAttribute(attribute.getValue(), attribute.getName()));
    } else if (attribute.getName().equals(GfxdConstants.MBEAN_ATTRIBUTE_ROWSMODIFICATIONTIME)) {
      statement.setRowsModificationTime(getLongAttribute(attribute.getValue(), attribute.getName()));
    } else if (attribute.getName().equals(GfxdConstants.MBEAN_ATTRIBUTE_QNNUMROWSSEEN)) {
      statement.setqNNumRowsSeen(getLongAttribute(attribute.getValue(), attribute.getName()));
    } else if (attribute.getName().equals(GfxdConstants.MBEAN_ATTRIBUTE_QNMSGSENDTIME)) {
      statement.setqNMsgSendTime(getLongAttribute(attribute.getValue(), attribute.getName()));
    } else if (attribute.getName().equals(GfxdConstants.MBEAN_ATTRIBUTE_QNMSGSERTIME)) {
      statement.setqNMsgSerTime(getLongAttribute(attribute.getValue(), attribute.getName()));
    }
    if (attribute.getName().equals(GfxdConstants.MBEAN_ATTRIBUTE_QNRESPDESERTIME)) {
      statement.setqNRespDeSerTime(getLongAttribute(attribute.getValue(), attribute.getName()));
    }
  }

  public Map<String, Statement> getGfxdStatements() {
    queryClusterStatistics();
    return this.statementMap;
  }

  public void addClusterStatement(String name, Statement stmt) {
    try {
      statementMap.put(name, stmt);
    } finally {
      //
    }
  }

  public static void main(String[] args) throws InterruptedException {
    MBeanServerConnection mbs = standAloneConnection(args);
    ClusterStatistics stats = new ClusterStatistics();
    stats.setMbs(mbs);
    while (true) {
      stats.queryClusterStatistics();
      Thread.sleep(5000);
    }
  }

  private static MBeanServerConnection standAloneConnection(String[] args) {
    return connect(args[0], Integer.valueOf(args[1]));
  }

  private static MBeanServerConnection connect(String hostname, int port) {
    String urlPath = "/jndi/rmi://" + hostname + ":" + port + "/jmxrmi";
    MBeanServerConnection server = null;
    try {
      JMXServiceURL url = new JMXServiceURL("rmi", "", 0, urlPath);
      JMXConnector jmxc = JMXConnectorFactory.connect(url);
      server = jmxc.getMBeanServerConnection();
    } catch (MalformedURLException e) {
      // should not reach here
    } catch (IOException e) {
      System.err.println("\nCommunication error: " + e.getMessage());
      System.exit(1);
    }
    return server;
  }

}
