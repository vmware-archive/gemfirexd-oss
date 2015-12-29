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
package sql.ecotests.wls.xa;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.rmi.RemoteException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.Hashtable;
import javax.ejb.CreateException;
import javax.jms.JMSException;
import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.rmi.PortableRemoteObject;
import javax.sql.DataSource;

public class Client {
    private static final String queueConnFactoryName = "weblogic.gfxdtest.jms.QueueConnectionFactory";
    private static final String queueName = "weblogic.gfxdtest.jms.gfxdQueue";
    private static final String BMXAHomeJNDIName = "weblogic-gfxdtest-BMXATestHome";
    private static final String CMXAHomeJNDIName = "weblogic-gfxdtest-CMXATestHome";
    private static final String gfxdLocatorDataSrcName = "gfxdtest-thin-loc-xads";
    private static final String gfxdMulticastDataSrcName = "gfxdtest-thin-mc-xads";
    private static final String otherDataSrcName = "derby-thin-loc-xads";
    private static final String tableName = "gfxdXAtest";
    private static final String tableName2 = "gfxdXAtest2";

    private static final int THIN_XADS_TO_LOCATOR = 0;
    private static final int PEER_XADS_TO_LOCATOR = 1;
    private static final int THIN_XADS_TO_MULTICAST = 2;
    private static final int PEER_XADS_TO_MULTICAST = 3;
    private static final int THIN_XADS_TO_DERBY = 10;

    private static final int BEAN_MANAGED_TXN = 0;
    private static final int CONTAINER_MANAGED_TXN = 1;

    private static String url = "t3://localhost:7001";
    private static InitialContext ic = null;
    private static DataSource gfxdlocds = null;
    private static DataSource gfxdmcds = null;
    private static DataSource otherds = null;
    private static JMSQueueHelper jms = null;
    private static BMXATest beanManagedXATest = null;
    private static CMXATest containerManagedXATest = null;

    private static boolean commonEnvSetup = false;

    public static void main(String[] args) throws Exception {
        if (args.length == 0 || args[0].trim().equals("")) {
            log("Need specify property testsuite with value: locator, peer_locator,");
            log("client_locator, multicast, peer_multicast, or client_multicast.");
            log("Example: -Dtestsuite=peer_locator,client_locator");
            log("peer_locator, peer_multicast can not be run together");
            return;
        }

        if (System.getProperty("url") != null) {
          url = System.getProperty("url");
        }

        int iterations = 1;
        if (System.getProperty("iterations") != null) {
          try {
            iterations = Integer.parseInt(System.getProperty("iterations"));
          } catch(NumberFormatException nfe) {
            log("property iterations is not in right format, set to 1");
          }
        }
   
        int threads = 1;
        if (System.getProperty("runthreads") != null) {
          try {
            threads = Integer.parseInt(System.getProperty("runthreads"));
          } catch(NumberFormatException nfe) {
            log("property threads is not in right format, set to 1");
          }
        }
        
        final String [] testCases = args[0].split(",");
        final int runTimes = iterations;
        Runnable task = new Runnable() { 
          public void run() {
            for (String testCase : testCases) {
              for (int i = 0; i < runTimes; i++) {
                  if (testCase.equalsIgnoreCase("client_locator") || testCase.equalsIgnoreCase("locator")) {
                      testXADSLocatorByThinXADS();
                  }
                  if (testCase.equalsIgnoreCase("peer_locator") || testCase.equalsIgnoreCase("locator")) {
                      testXADSLocatorByPeerXADS();
                  }
      
                  if (testCase.equalsIgnoreCase("client_multicast") || testCase.equalsIgnoreCase("multicast")) {
                      testXADSMulticastByThinXADS();
                  }
                  if (testCase.equalsIgnoreCase("peer_multicast") || testCase.equalsIgnoreCase("multicast")) {
                      testXADSMulticastByPeerXADS();
                  }
              }
            }
          }
        };
        log("Tests will be run using " + threads + " threads for " + iterations + " iterations");
        Thread[] testRunners = new Thread[threads];
        for (int i=0; i < threads; i++) {
          testRunners[i] = new Thread(task);
          testRunners[i].start();
        }
        
        for (int i=0; i<threads; i++) {
          testRunners[i].join();
        }
    }

    private static synchronized void prepareTest(boolean isLocatorTest) throws Exception {
        if (ic == null) {
            ic = getInitialContext(url);
        }

        if (isLocatorTest && gfxdlocds == null) {
            gfxdlocds = (DataSource) ic.lookup(gfxdLocatorDataSrcName);
            initTable(gfxdlocds, tableName);
            initTable(gfxdlocds, tableName2);
        }

        if (!isLocatorTest && gfxdmcds == null) {
            gfxdmcds = (DataSource) ic.lookup(gfxdMulticastDataSrcName);
            initTable(gfxdmcds, tableName);
            initTable(gfxdmcds, tableName2);
        }

        if (!commonEnvSetup) {
            otherds = (DataSource) ic.lookup(otherDataSrcName);
            initTable(otherds, tableName);
            initTable(otherds, tableName2);

            jms = new JMSQueueHelper(ic, queueConnFactoryName, queueName);
            jms.init();

            beanManagedXATest = lookupBMXABean(ic, BMXAHomeJNDIName);
            containerManagedXATest = lookupCMXABean(ic, CMXAHomeJNDIName);

            commonEnvSetup = true;
        }
    }

    private static synchronized void unprepareTest() {}

    private static void testXADSLocatorByThinXADS() {
      try {
          prepareTest(true);
      } catch(Exception e) {
        log("Failed in the test preparation for testXADSLocatorByThinXADS");
        return;
      }

        testBMCommitDSLocatorWithJMSByThinXADS();
        testBMRollbackDSLocatorWithJMSByThinXADS();
        testBMCommitDSLocatorWithOtherDBByThinXADS();
        testBMRollbackDSLocatorWithOtherDBByThinXADS();
        testBMCommitDSLocatorWithSameDBByThinXADS();
        testBMRollbackDSLocatorWithSameDBByThinXADS();

        testCMCommitDSLocatorWithJMSByThinXADS();
        testCMRollbackDSLocatorWithJMSByThinXADS();
        testCMCommitDSLocatorWithOtherDBByThinXADS();
        testCMRollbackDSLocatorWithOtherDBByThinXADS();
        testCMCommitDSLocatorWithSameDBByThinXADS();
        testCMRollbackDSLocatorWithSameDBByThinXADS();

        unprepareTest();
    }

    private static void testXADSLocatorByPeerXADS() {
      try {
        prepareTest(true);
      } catch(Exception e) {
        log("Failed in the test preparation for testXADSLocatorByPeerXADS");
        return;
      }

        testBMCommitDSLocatorWithJMSByPeerXADS();
        testBMRollbackDSLocatorWithJMSByPeerXADS();
        testBMCommitDSLocatorWithOtherDBByPeerXADS();
        testBMRollbackDSLocatorWithOtherDBByPeerXADS();
        testBMCommitDSLocatorWithSameDBByPeerXADS();
        testBMRollbackDSLocatorWithSameDBByPeerXADS();

        testCMCommitDSLocatorWithJMSByPeerXADS();
        testCMRollbackDSLocatorWithJMSByPeerXADS();
        testCMCommitDSLocatorWithOtherDBByPeerXADS();
        testCMRollbackDSLocatorWithOtherDBByPeerXADS();
        testCMCommitDSLocatorWithSameDBByPeerXADS();
        testCMRollbackDSLocatorWithSameDBByPeerXADS();

        unprepareTest();
    }

    private static void testXADSMulticastByThinXADS() {
      try {
        prepareTest(false);
      } catch(Exception e) {
        log("Failed in the test preparation for testXADSMulticastByThinXADS");
        return;
      }

        testBMCommitDSMulticastWithJMSByThinXADS();
        testBMRollbackDSMulticastWithJMSByThinXADS();
        testBMCommitDSMulticastWithOtherDBByThinXADS();
        testBMRollbackDSMulticastWithOtherDBByThinXADS();
        testBMCommitDSMulticastWithSameDBByThinXADS();
        testBMRollbackDSMulticastWithSameDBByThinXADS();

        testCMCommitDSMulticastWithJMSByThinXADS();
        testCMRollbackDSMulticastWithJMSByThinXADS();
        testCMCommitDSMulticastWithOtherDBByThinXADS();
        testCMRollbackDSMulticastWithOtherDBByThinXADS();
        testCMCommitDSMulticastWithSameDBByThinXADS();
        testCMRollbackDSMulticastWithSameDBByThinXADS();

        unprepareTest();
    }

    private static void testXADSMulticastByPeerXADS() {
      try {
        prepareTest(false);
      } catch(Exception e) {
        log("Failed in the test preparation for testXADSMulticastByPeerXADS");
        return;
      }

        testBMCommitDSMulticastWithJMSByPeerXADS();
        testBMRollbackDSMulticastWithJMSByPeerXADS();
        testBMCommitDSMulticastWithOtherDBByPeerXADS();
        testBMRollbackDSMulticastWithOtherDBByPeerXADS();
        testBMCommitDSMulticastWithSameDBByPeerXADS();
        testBMRollbackDSMulticastWithSameDBByPeerXADS();

        testCMCommitDSMulticastWithJMSByPeerXADS();
        testCMRollbackDSMulticastWithJMSByPeerXADS();
        testCMCommitDSMulticastWithOtherDBByPeerXADS();
        testCMRollbackDSMulticastWithOtherDBByPeerXADS();
        testCMCommitDSMulticastWithSameDBByPeerXADS();
        testCMRollbackDSMulticastWithSameDBByPeerXADS();

        unprepareTest();
    }

    //===Test XA for JMS and GemFireXD (with Locator or Multicast) using client or embedded datasource in WLS
    private static void testBMCommitDSLocatorWithJMSByThinXADS() {
        boolean pass = testXACommitWithJMS(THIN_XADS_TO_LOCATOR, BEAN_MANAGED_TXN);
        log("testBMCommitDSLocatorWithJMSByThinXADS: " + (pass ? "Passed" : "Failed"));
    }

    private static void testBMCommitDSLocatorWithJMSByPeerXADS() {
        boolean pass = testXACommitWithJMS(PEER_XADS_TO_LOCATOR, BEAN_MANAGED_TXN);
        log("testBMCommitDSLocatorWithJMSByPeerXADS: " + (pass ? "Passed" : "Failed"));
    }

    private static void testBMCommitDSMulticastWithJMSByThinXADS() {
        boolean pass = testXACommitWithJMS(THIN_XADS_TO_MULTICAST, BEAN_MANAGED_TXN);
        log("testBMCommitDSMulticastWithJMSByThinXADS: " + (pass ? "Passed" : "Failed"));
    }

    private static void testBMCommitDSMulticastWithJMSByPeerXADS() {
        boolean pass = testXACommitWithJMS(PEER_XADS_TO_MULTICAST, BEAN_MANAGED_TXN);
        log("testBMCommitDSMulticastWithJMSByPeerXADS: " + (pass ? "Passed" : "Failed"));
    }

    private static void testBMRollbackDSLocatorWithJMSByThinXADS() {
        boolean pass = testXARollbackWithJMS(THIN_XADS_TO_LOCATOR, BEAN_MANAGED_TXN);
        log("testBMRollbackDSLocatorWithJMSByThinXADS: " + (pass ? "Passed" : "Failed"));
    }

    private static void testBMRollbackDSLocatorWithJMSByPeerXADS() {
        boolean pass = testXARollbackWithJMS(PEER_XADS_TO_LOCATOR, BEAN_MANAGED_TXN);
        log("testBMRollbackDSLocatorWithJMSByPeerXADS: " + (pass ? "Passed" : "Failed"));
    }

    private static void testBMRollbackDSMulticastWithJMSByThinXADS() {
        boolean pass = testXARollbackWithJMS(THIN_XADS_TO_MULTICAST, BEAN_MANAGED_TXN);
        log("testBMRollbackDSMulticastWithJMSByThinXADS: " + (pass ? "Passed" : "Failed"));
    }

    private static void testBMRollbackDSMulticastWithJMSByPeerXADS() {
        boolean pass = testXARollbackWithJMS(PEER_XADS_TO_MULTICAST, BEAN_MANAGED_TXN);
        log("testBMRollbackDSMulticastWithJMSByPeerXADS: " + (pass ? "Passed" : "Failed"));
    }

    private static void testCMCommitDSLocatorWithJMSByThinXADS() {
        boolean pass = testXACommitWithJMS(THIN_XADS_TO_LOCATOR, CONTAINER_MANAGED_TXN);
        log("testCMCommitDSLocatorWithJMSByThinXADS: " + (pass ? "Passed" : "Failed"));
    }

    private static void testCMCommitDSLocatorWithJMSByPeerXADS() {
        boolean pass = testXACommitWithJMS(PEER_XADS_TO_LOCATOR, CONTAINER_MANAGED_TXN);
        log("testCMCommitDSLocatorWithJMSByPeerXADS: " + (pass ? "Passed" : "Failed"));
    }

    private static void testCMCommitDSMulticastWithJMSByThinXADS() {
        boolean pass = testXACommitWithJMS(THIN_XADS_TO_MULTICAST, CONTAINER_MANAGED_TXN);
        log("testCMCommitDSMulticastWithJMSByThinXADS: " + (pass ? "Passed" : "Failed"));
    }

    private static void testCMCommitDSMulticastWithJMSByPeerXADS() {
        boolean pass = testXACommitWithJMS(PEER_XADS_TO_MULTICAST, CONTAINER_MANAGED_TXN);
        log("testCMCommitDSMulticastWithJMSByPeerXADS: " + (pass ? "Passed" : "Failed"));
    }

    private static void testCMRollbackDSLocatorWithJMSByThinXADS() {
        boolean pass = testXARollbackWithJMS(THIN_XADS_TO_LOCATOR, CONTAINER_MANAGED_TXN);
        log("testCMRollbackDSLocatorWithJMSByThinXADS: " + (pass ? "Passed" : "Failed"));
    }

    private static void testCMRollbackDSLocatorWithJMSByPeerXADS() {
        boolean pass = testXARollbackWithJMS(PEER_XADS_TO_LOCATOR, CONTAINER_MANAGED_TXN);
        log("testCMRollbackDSLocatorWithJMSByPeerXADS: " + (pass ? "Passed" : "Failed"));
    }

    private static void testCMRollbackDSMulticastWithJMSByThinXADS() {
        boolean pass = testXARollbackWithJMS(THIN_XADS_TO_MULTICAST, CONTAINER_MANAGED_TXN);
        log("testCMRollbackDSMulticastWithJMSByThinXADS: " + (pass ? "Passed" : "Failed"));
    }

    private static void testCMRollbackDSMulticastWithJMSByPeerXADS() {
        boolean pass = testXARollbackWithJMS(PEER_XADS_TO_MULTICAST, CONTAINER_MANAGED_TXN);
        log("testCMRollbackDSMulticastWithJMSByPeerXADS: " + (pass ? "Passed" : "Failed"));
    }

    //JMS tests should be synchronized to ensure to get right result from JMS queue
    private static synchronized boolean testXACommitWithJMS(int dstype, int txn_type) {
        boolean pass = true;
        String testData = getTestData();

        try {
            jms.send(testData);
            jms.closeSender();

            if (txn_type == BEAN_MANAGED_TXN) {
                beanManagedXATest.testXACommitRollbackWithJMS(true, dstype);
            } else {
                containerManagedXATest.testXACommitRollbackWithJMS(true, dstype);
            }

            boolean dbPass = verifyDatabase(dstype, tableName, testData, testData);
            boolean jmsPass = verifyJMS(null);
            pass = dbPass & jmsPass;
        } catch (Exception ex) {
            log("testXACommitWithJMS failed: " + ex);
            pass = false;
        }
        return pass;
    }

    private static synchronized boolean testXARollbackWithJMS(int dstype, int txn_type) {
        boolean pass = true;
        String testData = getTestData();

        try {
            jms.send(testData);
            jms.closeSender();
            if (txn_type == BEAN_MANAGED_TXN) {
                beanManagedXATest.testXACommitRollbackWithJMS(false, dstype);
            } else {
                containerManagedXATest.testXACommitRollbackWithJMS(false, dstype);
            }

            boolean dbPass = verifyDatabase(dstype, tableName, testData, null);
            boolean jmsPass = verifyJMS(testData);
            pass = dbPass & jmsPass;
        } catch (Exception ex) {
            log("testXARollbackWithJMS failed: " + ex);
            pass = false;
        }
        return pass;
    }


    //===Test XA for Other database (e.g. Derby) and GemFireXD (with Locator or Multicast) using client or embedded datasource in WLS
    private static void testBMCommitDSLocatorWithOtherDBByThinXADS() {
        boolean pass = testXACommitWithOtherDB(THIN_XADS_TO_LOCATOR, BEAN_MANAGED_TXN);
        log("testBMCommitDSLocatorWithOtherDBByThinXADS: " + (pass ? "Passed" : "Failed"));
    }

    private static void testBMCommitDSLocatorWithOtherDBByPeerXADS() {
        boolean pass = testXACommitWithOtherDB(PEER_XADS_TO_LOCATOR, BEAN_MANAGED_TXN);
        log("testBMCommitDSLocatorWithOtherDBByPeerXADS: " + (pass ? "Passed" : "Failed"));
    }

    private static void testBMCommitDSMulticastWithOtherDBByThinXADS() {
        boolean pass = testXACommitWithOtherDB(THIN_XADS_TO_MULTICAST, BEAN_MANAGED_TXN);
        log("testBMCommitDSMulticastWithOtherDBByThinXADS: " + (pass ? "Passed" : "Failed"));
    }

    private static void testBMCommitDSMulticastWithOtherDBByPeerXADS() {
        boolean pass = testXACommitWithOtherDB(PEER_XADS_TO_MULTICAST, BEAN_MANAGED_TXN);
        log("testBMCommitDSMulticastWithOtherDBByPeerXADS: " + (pass ? "Passed" : "Failed"));
    }

    private static void testBMRollbackDSLocatorWithOtherDBByThinXADS() {
        boolean pass = testXARollbackWithOtherDB(THIN_XADS_TO_LOCATOR, BEAN_MANAGED_TXN);
        log("testBMRollbackDSLocatorWithOtherDBByThinXADS: " + (pass ? "Passed" : "Failed"));
    }

    private static void testBMRollbackDSLocatorWithOtherDBByPeerXADS() {
        boolean pass = testXARollbackWithOtherDB(PEER_XADS_TO_LOCATOR, BEAN_MANAGED_TXN);
        log("testBMRollbackDSLocatorWithOtherDBByPeerXADS: " + (pass ? "Passed" : "Failed"));
    }

    private static void testBMRollbackDSMulticastWithOtherDBByThinXADS() {
        boolean pass = testXARollbackWithOtherDB(THIN_XADS_TO_MULTICAST, BEAN_MANAGED_TXN);
        log("testBMRollbackDSMulticastWithOtherDBByThinXADS: " + (pass ? "Passed" : "Failed"));
    }

    private static void testBMRollbackDSMulticastWithOtherDBByPeerXADS() {
        boolean pass = testXARollbackWithOtherDB(PEER_XADS_TO_MULTICAST, BEAN_MANAGED_TXN);
        log("testBMRollbackDSMulticastWithOtherDBByPeerXADS: " + (pass ? "Passed" : "Failed"));
    }

    private static void testCMCommitDSLocatorWithOtherDBByThinXADS() {
        boolean pass = testXACommitWithOtherDB(THIN_XADS_TO_LOCATOR, CONTAINER_MANAGED_TXN);
        log("testCMCommitDSLocatorWithOtherDBByThinXADS: " + (pass ? "Passed" : "Failed"));
    }

    private static void testCMCommitDSLocatorWithOtherDBByPeerXADS() {
        boolean pass = testXACommitWithOtherDB(PEER_XADS_TO_LOCATOR, CONTAINER_MANAGED_TXN);
        log("testCMCommitDSLocatorWithOtherDBByPeerXADS: " + (pass ? "Passed" : "Failed"));
    }

    private static void testCMCommitDSMulticastWithOtherDBByThinXADS() {
        boolean pass = testXACommitWithOtherDB(THIN_XADS_TO_MULTICAST, CONTAINER_MANAGED_TXN);
        log("testCMCommitDSMulticastWithOtherDBByThinXADS: " + (pass ? "Passed" : "Failed"));
    }

    private static void testCMCommitDSMulticastWithOtherDBByPeerXADS() {
        boolean pass = testXACommitWithOtherDB(PEER_XADS_TO_MULTICAST, CONTAINER_MANAGED_TXN);
        log("testCMCommitDSMulticastWithOtherDBByPeerXADS: " + (pass ? "Passed" : "Failed"));
    }

    private static void testCMRollbackDSLocatorWithOtherDBByThinXADS() {
        boolean pass = testXARollbackWithOtherDB(THIN_XADS_TO_LOCATOR, CONTAINER_MANAGED_TXN);
        log("testCMRollbackDSLocatorWithOtherDBByThinXADS: " + (pass ? "Passed" : "Failed"));
    }

    private static void testCMRollbackDSLocatorWithOtherDBByPeerXADS() {
        boolean pass = testXARollbackWithOtherDB(PEER_XADS_TO_LOCATOR, CONTAINER_MANAGED_TXN);
        log("testCMRollbackDSLocatorWithOtherDBByPeerXADS: " + (pass ? "Passed" : "Failed"));
    }

    private static void testCMRollbackDSMulticastWithOtherDBByThinXADS() {
        boolean pass = testXARollbackWithOtherDB(THIN_XADS_TO_MULTICAST, CONTAINER_MANAGED_TXN);
        log("testCMRollbackDSMulticastWithOtherDBByThinXADS: " + (pass ? "Passed" : "Failed"));
    }

    private static void testCMRollbackDSMulticastWithOtherDBByPeerXADS() {
        boolean pass = testXARollbackWithOtherDB(PEER_XADS_TO_MULTICAST, CONTAINER_MANAGED_TXN);
        log("testCMRollbackDSMulticastWithOtherDBByPeerXADS: " + (pass ? "Passed" : "Failed"));
    }

    private static boolean testXACommitWithOtherDB(int dstype, int txn_type) {
        boolean pass = true;
        String testData = getTestData();

        try {
            if (txn_type == BEAN_MANAGED_TXN) {
                beanManagedXATest.testXACommitRollbackWithOtherDB(true, dstype, testData);
            } else {
                containerManagedXATest.testXACommitRollbackWithOtherDB(true, dstype, testData);
            }
            boolean dbPass1 = verifyDatabase(dstype, tableName, testData, testData);
            boolean dbPass2 = verifyDatabase(THIN_XADS_TO_DERBY, tableName, testData, testData);
            pass = dbPass1 & dbPass2;
        } catch (Exception ex) {
            log("testXACommitWithOtherDB failed: " + ex);
            pass = false;
        }
        return pass;
    }

    private static boolean testXARollbackWithOtherDB(int dstype, int txn_type) {
        boolean pass = true;
        String testData = getTestData();

        try {
            if (txn_type == BEAN_MANAGED_TXN) {
                beanManagedXATest.testXACommitRollbackWithOtherDB(false, dstype, testData);
            } else {
                containerManagedXATest.testXACommitRollbackWithOtherDB(false, dstype, testData);
            }
            boolean dbPass1 = verifyDatabase(dstype, tableName, testData, null);
            boolean dbPass2 = verifyDatabase(THIN_XADS_TO_DERBY, tableName, testData, null);
            pass = dbPass1 & dbPass2;
        } catch (Exception ex) {
            log("testXARollbackWithOtherDB failed: " + ex);
            pass = false;
        }
        return pass;
    }

    //===Test XA for Other database (e.g. Derby) and GemFireXD (with Locator or Multicast) using client or embedded datasource in WLS
    private static void testBMCommitDSLocatorWithSameDBByThinXADS() {
        boolean pass = testXACommitWithSameDB(THIN_XADS_TO_LOCATOR, BEAN_MANAGED_TXN);
        log("testBMCommitDSLocatorWithSameDBByThinXADS: " + (pass ? "Passed" : "Failed"));
    }

    private static void testBMCommitDSLocatorWithSameDBByPeerXADS() {
        boolean pass = testXACommitWithSameDB(PEER_XADS_TO_LOCATOR, BEAN_MANAGED_TXN);
        log("testBMCommitDSLocatorWithSameDBByPeerXADS: " + (pass ? "Passed" : "Failed"));
    }

    private static void testBMCommitDSMulticastWithSameDBByThinXADS() {
        boolean pass = testXACommitWithSameDB(THIN_XADS_TO_MULTICAST, BEAN_MANAGED_TXN);
        log("testBMCommitDSMulticastWithSameDBByThinXADS: " + (pass ? "Passed" : "Failed"));
    }

    private static void testBMCommitDSMulticastWithSameDBByPeerXADS() {
        boolean pass = testXACommitWithSameDB(PEER_XADS_TO_MULTICAST, BEAN_MANAGED_TXN);
        log("testBMCommitDSMulticastWithSameDBByPeerXADS: " + (pass ? "Passed" : "Failed"));
    }

    private static void testBMRollbackDSLocatorWithSameDBByThinXADS() {
        boolean pass = testXARollbackWithSameDB(THIN_XADS_TO_LOCATOR, BEAN_MANAGED_TXN);
        log("testBMRollbackDSLocatorWithSameDBByThinXADS: " + (pass ? "Passed" : "Failed"));
    }

    private static void testBMRollbackDSLocatorWithSameDBByPeerXADS() {
        boolean pass = testXARollbackWithSameDB(PEER_XADS_TO_LOCATOR, BEAN_MANAGED_TXN);
        log("testBMRollbackDSLocatorWithSameDBByPeerXADS: " + (pass ? "Passed" : "Failed"));
    }

    private static void testBMRollbackDSMulticastWithSameDBByThinXADS() {
        boolean pass = testXARollbackWithSameDB(THIN_XADS_TO_MULTICAST, BEAN_MANAGED_TXN);
        log("testBMRollbackDSMulticastWithSameDBByThinXADS: " + (pass ? "Passed" : "Failed"));
    }

    private static void testBMRollbackDSMulticastWithSameDBByPeerXADS() {
        boolean pass = testXARollbackWithSameDB(PEER_XADS_TO_MULTICAST, BEAN_MANAGED_TXN);
        log("testBMRollbackDSMulticastWithSameDBByPeerXADS: " + (pass ? "Passed" : "Failed"));
    }

    private static void testCMCommitDSLocatorWithSameDBByThinXADS() {
        boolean pass = testXACommitWithSameDB(THIN_XADS_TO_LOCATOR, CONTAINER_MANAGED_TXN);
        log("testCMCommitDSLocatorWithSameDBByThinXADS: " + (pass ? "Passed" : "Failed"));
    }

    private static void testCMCommitDSLocatorWithSameDBByPeerXADS() {
        boolean pass = testXACommitWithSameDB(PEER_XADS_TO_LOCATOR, CONTAINER_MANAGED_TXN);
        log("testCMCommitDSLocatorWithSameDBByPeerXADS: " + (pass ? "Passed" : "Failed"));
    }

    private static void testCMCommitDSMulticastWithSameDBByThinXADS() {
        boolean pass = testXACommitWithSameDB(THIN_XADS_TO_MULTICAST, CONTAINER_MANAGED_TXN);
        log("testCMCommitDSMulticastWithSameDBByThinXADS: " + (pass ? "Passed" : "Failed"));
    }

    private static void testCMCommitDSMulticastWithSameDBByPeerXADS() {
        boolean pass = testXACommitWithSameDB(PEER_XADS_TO_MULTICAST, CONTAINER_MANAGED_TXN);
        log("testCMCommitDSMulticastWithSameDBByPeerXADS: " + (pass ? "Passed" : "Failed"));
    }

    private static void testCMRollbackDSLocatorWithSameDBByThinXADS() {
        boolean pass = testXARollbackWithSameDB(THIN_XADS_TO_LOCATOR, CONTAINER_MANAGED_TXN);
        log("testCMRollbackDSLocatorWithSameDBByThinXADS: " + (pass ? "Passed" : "Failed"));
    }

    private static void testCMRollbackDSLocatorWithSameDBByPeerXADS() {
        boolean pass = testXARollbackWithSameDB(PEER_XADS_TO_LOCATOR, CONTAINER_MANAGED_TXN);
        log("testCMRollbackDSLocatorWithSameDBByPeerXADS: " + (pass ? "Passed" : "Failed"));
    }

    private static void testCMRollbackDSMulticastWithSameDBByThinXADS() {
        boolean pass = testXARollbackWithSameDB(THIN_XADS_TO_MULTICAST, CONTAINER_MANAGED_TXN);
        log("testCMRollbackDSMulticastWithSameDBByThinXADS: " + (pass ? "Passed" : "Failed"));
    }

    private static void testCMRollbackDSMulticastWithSameDBByPeerXADS() {
        boolean pass = testXARollbackWithSameDB(PEER_XADS_TO_MULTICAST, CONTAINER_MANAGED_TXN);
        log("testCMRollbackDSMulticastWithSameDBByPeerXADS: " + (pass ? "Passed" : "Failed"));
    }

    private static boolean testXACommitWithSameDB(int dstype, int txn_type) {
        boolean pass = true;
        String testData = getTestData();

        try {
            if (txn_type == BEAN_MANAGED_TXN) {
                beanManagedXATest.testXACommitRollbackWithSameDB(true, dstype, testData);
            } else {
                containerManagedXATest.testXACommitRollbackWithSameDB(true, dstype, testData);
            }
            boolean dbPass1 = verifyDatabase(dstype, tableName, testData, testData);
            boolean dbPass2 = verifyDatabase(dstype, tableName2, testData, testData);
            pass = dbPass1 & dbPass2;
        } catch (Exception ex) {
            log("testXACommitWithSameDB failed: " + ex);
            pass = false;
        }
        return pass;
     }

     private static boolean testXARollbackWithSameDB(int dstype, int txn_type) {
        boolean pass = true;
        String testData = getTestData();

        try {
            if (txn_type == BEAN_MANAGED_TXN) {
                beanManagedXATest.testXACommitRollbackWithSameDB(false, dstype, testData);
            } else {
                containerManagedXATest.testXACommitRollbackWithSameDB(false, dstype, testData);
            }
            boolean dbPass1 = verifyDatabase(dstype, tableName, testData, null);
            boolean dbPass2 = verifyDatabase(dstype, tableName2, testData, null);
            pass = dbPass1 & dbPass2;
        } catch (Exception ex) {
            log("testXARollbackWithSameDB failed: " + ex);
            pass = false;
        }
        return pass;
    }

    //=== other private methods
    private static String getTestData() {
        return "GemFireXDData_" +  + Thread.currentThread().getId() + "_" + (System.currentTimeMillis()%10000000);
    }

    private static void initTable(DataSource ds, String tblName) throws SQLException {
        Connection jcon = null;
        Statement stmt = null;
        try {
            jcon = ds.getConnection();
            //jcon.setAutoCommit(true);
            stmt = jcon.createStatement();
            try {
                stmt.execute("drop table " + tblName);
            } catch (SQLException ex) {
            }
            stmt.execute("create table APP." + tblName + "(data varchar(40))");
        } catch (SQLException sqle) {
          log("Failed to create the test tables");
          throw sqle;
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ex) {
                }
            }
            if (jcon != null) {
                try {
                    jcon.close();
                } catch (SQLException ex) {
                }
            }
        }
    }

    private static boolean verifyJMS(String expectedData) {
        boolean jmsPass = true;
        try {
            String actualData = jms.receive();
            if (actualData == null) {
                if(expectedData == null) {
                    jmsPass = true;
                } else {
                    jmsPass = false;
                }
            } else {
                jmsPass = actualData.equals(expectedData) == true;
            }
        } catch (JMSException ex) {
            log("Got JMSException while verifying jms: " + ex);
            jmsPass = false;
        } finally {
            jms.closeReceiver();
        }
        return jmsPass;
    }

    private static boolean verifyDatabase(int dstype, String tblName, String testData, String expectedData) {
        Connection jcon = null;
        Statement stmt = null;
        ResultSet rs = null;
        boolean dbPass = true;
        DataSource ds = null;

        switch (dstype) {
            case THIN_XADS_TO_LOCATOR:
            case PEER_XADS_TO_LOCATOR:
                ds = gfxdlocds;
                break;
            case THIN_XADS_TO_MULTICAST:
            case PEER_XADS_TO_MULTICAST:
                ds = gfxdmcds;
                break;
            case THIN_XADS_TO_DERBY:
            default:
                ds = otherds;
        }

        try {
            jcon = ds.getConnection();
            stmt = jcon.createStatement();
            stmt.execute("select data from app." + tblName + " where data = '" + testData + "'");
            rs = stmt.getResultSet();

            String actualData = null;
            int rowCount = 0;
            while (rs.next()) {
                actualData = rs.getString(1);
                rowCount++;
            }

            if (actualData == null) {
                dbPass = expectedData == null ? true : false;
            } else {
                dbPass = actualData.equals(expectedData) && rowCount == 1;
            }
        } catch (SQLException ex) {
            log("Got SQLException while verifying database: " + ex);
            dbPass = false;
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException ex) {
                }
            }

            try {
                if (expectedData != null) {
                    int count = stmt.executeUpdate("delete from app." + tblName + " where data = '" + testData + "'");
                    //log("Executed: delete from " + tableName + " where data = '" + testData + "'; Returned: " + count);
                }
            } catch (SQLException sqle) {
                log("Unable to clean the table: " + sqle);
            }

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ex) {
                }
            }
            if (jcon != null) {
                try {
                    jcon.close();
                } catch (SQLException ex) {
                }
            }
            return dbPass;
        }
    }

    private static BMXATest lookupBMXABean(InitialContext ictx, String jndiName) {
        try {
            Object home = ictx.lookup(jndiName);
            BMXATestHome rhome = (BMXATestHome) PortableRemoteObject.narrow(
                    home, BMXATestHome.class);
            BMXATest bean = (BMXATest) PortableRemoteObject.narrow(
                    rhome.create(), BMXATest.class);
            return bean;
        } catch (NamingException ne) {
            log("The client was unable to lookup the EJBHome.  Please make sure ");
            log("that you have deployed the ejb with the JNDI name " + jndiName
                    + " on the WebLogic server at " + url);
        } catch (CreateException ce) {
            log("Creation failed: " + ce);
        } catch (RemoteException ex) {
            log("Creation failed: " + ex);
        }
        return null;
    }

    private static CMXATest lookupCMXABean(InitialContext ictx, String jndiName) {
        try {
            Object home = ictx.lookup(jndiName);
            CMXATestHome rhome = (CMXATestHome) PortableRemoteObject.narrow(
                    home, CMXATestHome.class);
            CMXATest bean = (CMXATest) PortableRemoteObject.narrow(
                    rhome.create(), CMXATest.class);
            return bean;
        } catch (NamingException ne) {
            log("The client was unable to lookup the EJBHome.  Please make sure ");
            log("that you have deployed the ejb with the JNDI name " + jndiName
                    + " on the WebLogic server at " + url);
        } catch (CreateException ce) {
            log("Creation failed: " + ce);
        } catch (RemoteException ex) {
            log("Creation failed: " + ex);
        }
        return null;
    }

    private static InitialContext getInitialContext(String url)
            throws NamingException {
        Hashtable<String, String> env = new Hashtable<String, String>();
        env.put(Context.INITIAL_CONTEXT_FACTORY,
                "weblogic.jndi.WLInitialContextFactory");
        env.put(Context.PROVIDER_URL, url);
        return new InitialContext(env);
    }

    private static void log(String s) {
        System.out.println(s);
    }
}

/**
 * Utility class uses to create JMS objects and send or receive a message to a queue.
 */
class JMSQueueHelper {
    private InitialContext ictx;
    private String queueConnFactoryName;
    private String queueName;
    private QueueConnectionFactory qconFactory;
    private QueueConnection qSenderConn;
    private QueueConnection qReceiverConn;
    private QueueSession qSenderSession;
    private QueueSession qReceiverSession;
    private QueueSender qSender;
    private QueueReceiver qReceiver;
    private Queue queue;
    private TextMessage msg;

    public JMSQueueHelper(InitialContext ictx, String queueConnFactoryName, String queueName) {
        this.ictx = ictx;
        this.queueConnFactoryName = queueConnFactoryName;
        this.queueName = queueName;
    }

    public void init() throws NamingException, JMSException {
        qconFactory = (QueueConnectionFactory) ictx.lookup(queueConnFactoryName);
        queue = (Queue) ictx.lookup(queueName);
    }

    public void send(String message) throws JMSException {
        qSenderConn = qconFactory.createQueueConnection();
        qSenderSession = qSenderConn.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        qSender = qSenderSession.createSender(queue);
        msg = qSenderSession.createTextMessage();
        qSenderConn.start();
        msg.setText(message);
        qSender.send(msg);
    }

    public String receive() throws JMSException {
        String msgText = null;
        qReceiverConn = qconFactory.createQueueConnection();
        qReceiverSession = qReceiverConn.createQueueSession(false, javax.jms.Session.AUTO_ACKNOWLEDGE);
        qReceiver = qReceiverSession.createReceiver(queue);
        qReceiverConn.start();

        Message rmsg = qReceiver.receiveNoWait();
        if (rmsg != null) {
            if (rmsg instanceof TextMessage) {
                msgText = ((TextMessage) rmsg).getText();
            } else {
                msgText = rmsg.toString();
            }
        }

        return msgText;
    }

    public void closeSender() {
        if (qSender != null) {
            try {
                qSender.close();
            } catch (JMSException ex) {
            }
        }
        if (qSenderSession != null) {
            try {
                qSenderSession.close();
            } catch (JMSException ex) {
            }
        }
        if (qSenderConn != null) {
            try {
                qSenderConn.close();
            } catch (JMSException ex) {
            }
        }
    }

    public void closeReceiver() {
        if (qReceiver != null) {
            try {
                qReceiver.close();
            } catch (JMSException ex) {
            }
        }
        if (qReceiverSession != null) {
            try {
                qReceiverSession.close();
            } catch (JMSException ex) {
            }
        }
        if (qReceiverConn != null) {
            try {
                qReceiverConn.close();
            } catch (JMSException ex) {
            }
        }
    }
}
