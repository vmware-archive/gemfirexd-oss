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
package hyperictest;

import hydra.HostHelper;
import hydra.HypericServerInstallDescription;
import hydra.HypericServerInstallHelper;
import hydra.Log;
import hydra.ProcessMgr;

import java.io.File;

public class HypericInstall {

    private static String sep = File.separator;
    private static String result;
    private static String antCmd;
    private static String antHome;
    private static String buildXmlFile;
    private static String buildVersion;
    private static String releaseNum;
    private static String installDir;
    private static String hostOS;
    private static String buildCopyDir;
    private static String hqServerDb;
    private static String cmdProperty;
    private static String oracleDbUser;
    private static String oracleDbPwd;
    private static String oracleDbUrl;
    private static String oracleSID;
    private static String oracleAdminUser;
    private static String oracleAdminPwd;
    private static String mysqlDbUser;
    private static String mysqlDbPwd;
    private static String mysqlDbUrl;
    private static String mysqlPort;
    private static String mysqlAdminUser;
    private static String mysqlAdminPwd;
    private static String agentHost;
    private static String agentServerHost;
    private static String agentPort;

    private static String getCmdPropertyString() {
        String agentHostName = HostHelper.getLocalHost();
        HypericServerInstallDescription hqd = HypericServerInstallHelper
                        .getHypericServerInstallDescription(agentHostName);
        antHome = hqd.hostDescription.getAntHome();
        buildXmlFile = hqd.buildXmlFile;
        buildVersion = hqd.buildVersion;
        releaseNum = hqd.releaseNum;
        installDir = hqd.installDir;
        hostOS = hqd.hostOS;
        buildCopyDir = hqd.buildCopyDir;
        hqServerDb = hqd.hqServerDb;
        agentHost = hqd.agentHost;
        agentServerHost = hqd.agentServerHost;
        agentPort = hqd.agentPort;
        
        if (hostOS.equals("win32")) {
            antCmd = antHome + sep + "bin" + sep + "ant.bat" + " ";
        } else {
            antCmd = antHome + sep + "bin" + sep + "ant --noconfig" + " ";
        }

        cmdProperty = antCmd + "-Drelease.num=" + releaseNum + " ";
        
        cmdProperty += "-Dagent.host.ip=" + agentHost + " ";
        cmdProperty += "-Dagent.cam.ip=" + agentServerHost + " ";
        cmdProperty += "-Dagent.port=" + agentPort + " "; 
        cmdProperty += "-Dserver.host=" + agentServerHost + " ";

        if (hqServerDb != null && hqServerDb.equalsIgnoreCase("mysql")) {
            mysqlDbUser = hqd.mysqlDbUser;
            mysqlDbPwd = hqd.mysqlDbPwd;
            mysqlAdminUser = hqd.mysqlAdminUser;
            mysqlAdminPwd = hqd.mysqlAdminPwd;
            mysqlDbUrl = hqd.mysqlDbUrl;
            mysqlPort = hqd.mysqlPort;
            cmdProperty += "-Dmysql.db.user=" + mysqlDbUser + " ";
            cmdProperty += "-Dmysql.db.password=" + mysqlDbPwd + " ";
            cmdProperty += "-Dmysql.db.url=" + mysqlDbUrl + " ";
            cmdProperty += "-Dmysql.db.admin.user=" + mysqlAdminUser + " ";
            cmdProperty += "-Dmysql.db.admin.password=" + mysqlAdminPwd + " ";
            cmdProperty += "-Dmysql.db.port=" + mysqlPort + " ";
        } else if (hqServerDb != null && hqServerDb.equalsIgnoreCase("oracle")) {
            oracleDbUser = hqd.oracleDbUser;
            oracleDbPwd = hqd.oracleDbPwd;
            oracleAdminUser = hqd.oracleAdminUser;
            oracleAdminPwd = hqd.oracleAdminPwd;
            oracleDbUrl = hqd.oracleDbUrl;
            oracleSID = hqd.oracleSID;
            cmdProperty += "-Doracle.db.user=" + oracleDbUser + " ";
            cmdProperty += "-Doracle.db.password=" + oracleDbPwd + " ";
            cmdProperty += "-Doracle.db.url=" + oracleDbUrl + " ";
            cmdProperty += "-Doracle.db.admin.user=" + oracleAdminUser + " ";
            cmdProperty += "-Doracle.db.admin.password=" + oracleAdminPwd + " ";
            cmdProperty += "-Doracle.db.sid=" + oracleSID + " ";
        }

        cmdProperty += "-Dbuild.version=" + buildVersion + " ";
        cmdProperty += "-Dinstall.dir=" + installDir + " ";
        cmdProperty += "-Dbuild.os=" + hostOS + " ";
        cmdProperty += "-Dbuild.copy.dir=" + buildCopyDir + " ";
        cmdProperty += "-Dhq.server.db=" + hqServerDb + " ";
        return cmdProperty;
    }

    public static void recreateHQServerDBSchema() {
        // String cmd = antHome + sep + "bin" + sep + antCmd + " ";
        String cmdProperty = getCmdPropertyString();
        // String cmd = antHome + " ";
        // String cmd = "ant" + " ";
        String cmd = cmdProperty + "-f " + buildXmlFile + " recreateHQServerDBSchema " + " ";
        result = ProcessMgr.fgexec(cmd, 1800);
        Log.getLogWriter().info(result);
    }

    public static void downloadHQAgentBuild() {
        // String cmd = antHome + sep + "bin" + sep + antCmd + " ";
        String cmdProperty = getCmdPropertyString();
        // String cmd = antHome + " ";
        // String cmd = "ant" + " ";
        String cmd = cmdProperty + "-f " + buildXmlFile + " copyAndExtractHQAgentBuild " + " ";
        // cmd += cmdProperty;
        result = ProcessMgr.fgexec(cmd, 1800);
        Log.getLogWriter().info(result);
    }

    public static void setupHQAgent() {
        // String cmd = antHome + sep + "bin" + sep + antCmd + " ";
        String cmdProperty = getCmdPropertyString();
        // String cmd = antHome + " ";
        // String cmd = "ant" + " ";
        String cmd = cmdProperty + "-f " + buildXmlFile + " setupHQAgent " + " ";
        // cmd += cmdProperty;
        result = ProcessMgr.fgexec(cmd, 1800);
        Log.getLogWriter().info(result);
    }

    public static void downloadHQServerBuild() {
        // String cmd = antHome + sep + "bin" + sep + antCmd + " ";
        String cmdProperty = getCmdPropertyString();
        // String cmd = antHome + " ";
        // String cmd = "ant" + " ";

        String cmd = cmdProperty + "-f " + buildXmlFile + " copyAndExtractHQServerBuild " + " ";
        // cmd += cmdProperty;
        result = ProcessMgr.fgexec(cmd, 1800);
        Log.getLogWriter().info(result);
    }

    public static void configureHQServer() {
        // String cmd = antHome + sep + "bin" + sep + antCmd + " ";
        String cmdProperty = getCmdPropertyString();
        // String cmd = antHome + " ";
        // String cmd = "ant" + " ";
        String cmd = cmdProperty + "-f " + buildXmlFile + " configureHQServerBuild " + " ";
        // cmd += cmdProperty;
        result = ProcessMgr.fgexec(cmd, 1800);
        Log.getLogWriter().info(result);
    }

    public static void setupHQServer() {
        // String cmd = antHome + sep + "bin" + sep + antCmd + " ";
        String cmdProperty = getCmdPropertyString();
        // String cmd = antHome + " ";
        // String cmd = "ant" + " ";
        String cmd = cmdProperty + "-f " + buildXmlFile + " setupHQServer " + " ";
        // cmd += cmdProperty;
        result = ProcessMgr.fgexec(cmd, 1800);
        Log.getLogWriter().info(result);
    }

    public static void startHQServer() {
        // String cmd = antHome + sep + "bin" + sep + antCmd + " ";
        String cmdProperty = getCmdPropertyString();
        // String cmd = "ant" + " ";
        String cmd = cmdProperty + "-f " + buildXmlFile + " startHQServer " + " ";
        // cmd += cmdProperty;
        result = ProcessMgr.fgexec(cmd, 1800);
        Log.getLogWriter().info(result);
    }

    public static void stopHQServer() {
        // String cmd = antHome + sep + "bin" + sep + antCmd + " ";
        String cmdProperty = getCmdPropertyString();
        // String cmd = "ant" + " ";
        String cmd = cmdProperty + "-f " + buildXmlFile + " stopHQServer " + " ";
        // cmd += cmdProperty;
        result = ProcessMgr.fgexec(cmd, 1800);
        Log.getLogWriter().info(result);
    }

}
