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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Vector;

public class HypericServerInstallDescription
  extends AbstractDescription implements Serializable {

  private static List<String> hosts = new ArrayList(); // tracks physical hosts that have been used

  /** The logical name for the HypericServerInstall configuration */
  public String name;

  public String hostDescriptionName; // logical host name from HostPrms.names

  public HostDescription hostDescription; // from hostDescriptionName

  public String buildXmlFile;

  public String releaseNum;

  public String buildVersion;

  public String buildCopyDir;

  public String installDir;

  public String hostOS;

  public String hqServerDb;

  // Oracle DB Settings
  public String oracleDbUrl;
  
  public String oracleSID;

  public String oracleDbUser;

  public String oracleDbPwd;

  public String oracleAdminUser;

  public String oracleAdminPwd;
  
  // MySQL DB Settings
  public String mysqlDbUrl;

  public String mysqlDbUser;

  public String mysqlDbPwd;

  public String mysqlAdminUser;

  public String mysqlAdminPwd;
  
  public String mysqlPort;
  
  //Agent specific properties
  public String agentHost;
  
  public String agentServerHost;
  
  public String agentPort;
  
  public SortedMap<String, String> toSortedMap() {
    SortedMap<String, String> map = new TreeMap<String, String>();
    String header = this.getClass().getName() + "." + this.getName() + ".";
    map.put(header + "buildXmlFile", this.buildXmlFile);
    map.put(header + "hostName", this.hostDescriptionName);
    map.put(header + "releaseNum", this.releaseNum);
    map.put(header + "buildVersion", this.buildVersion);
    map.put(header + "buildCopyDir", this.buildCopyDir);
    map.put(header + "installDir", this.installDir);
    map.put(header + "hostOS", this.hostOS);
    map.put(header + "hqServerDb", this.hqServerDb);
    map.put(header + "oracleDbUrl", this.oracleDbUrl);
    map.put(header + "oracleDbUser", this.oracleDbUser);
    map.put(header + "oracleDbPwd", this.oracleDbPwd);
    map.put(header + "oracleAdminUser", this.oracleAdminUser);
    map.put(header + "oracleAdminPwd", this.oracleAdminPwd);
    map.put(header + "agentHost", this.agentHost);
    map.put(header + "agentServerHost", this.agentServerHost);
    map.put(header + "agentPort", this.agentPort);
    return map;
  }

  protected static void configure(TestConfig config) {

    ConfigHashtable tab = config.getParameters();

    // create a description for each logical name
    Vector names = tab.vecAt(HypericServerInstallPrms.names, new HydraVector());

    for (int i = 0; i < names.size(); i++) {

      String name = (String)names.elementAt(i);

      // create description from test configuration parameters
      HypericServerInstallDescription hqd =
        createHypericServerInstallDescription(name, config, i, hosts);

      // save configuration
      config.addHypericServerInstallDescription(hqd);
    }
  }

  /**
   * Creates the description using test configuration parameters.
   */
  private static HypericServerInstallDescription createHypericServerInstallDescription(String name, TestConfig config, int index, List<String> hosts) {

    ConfigHashtable tab = config.getParameters();

    HypericServerInstallDescription hqd = new HypericServerInstallDescription();
    hqd.setName(name);

    {
      Long key = HypericServerInstallPrms.hostName;
      String hostName = tab.stringAtWild(key, index, null);
      if (hostName == null) {
        String s = "Missing " + BasePrms.nameForKey(key);
        throw new HydraConfigException(s);
      }
      hqd.setHostDescriptionName(hostName);

      HostDescription hd = config.getHostDescription(hostName);
      if (hd == null) {
        String s = "Undefined value in " + BasePrms.nameForKey(key) + ": "
                 + hostName;
        throw new HydraConfigException(s);
      }
      String host = hd.getHostName();
      /*
      if (hosts.contains(host)) {
        String s = "Duplicate physical host name in " + BasePrms.nameForKey(key)
                 + ": " + hostName + " (" + host + ")";
        throw new HydraConfigException(s);
      }
      */
      hqd.setHostDescription(hd);
      hosts.add(host);
    }
    {
      Long key = HypericServerInstallPrms.buildXmlFile;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null) {
        hqd.setBuildXmlFile(str);
      }
    }
    {
      Long key = HypericServerInstallPrms.releaseNum;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null) {
        hqd.setReleaseNum(str);
      }
    }
    {
      Long key = HypericServerInstallPrms.buildVersion;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null) {
        hqd.setBuildVersion(str);
      }
    }
    {
      Long key = HypericServerInstallPrms.buildCopyDir;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null) {
        hqd.setTmpDir(str);
      }
    }
    {
      Long key = HypericServerInstallPrms.installDir;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null) {
        hqd.setInstallDir(str);
      }
    }
    {
      Long key = HypericServerInstallPrms.hostOS;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null) {
        hqd.setHostOS(str);
      }
    }
    {
      Long key = HypericServerInstallPrms.hqServerDb;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null) {
        hqd.setHQServerDb(str);
      }
    }
    {
      Long key = HypericServerInstallPrms.oracleDbUrl;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null) {
        hqd.setOracleDbUrl(str);
      }
    }
    {
        Long key = HypericServerInstallPrms.oracleSID;
        String str = tab.getString(key, tab.getWild(key, index, null));
        if (str != null) {
          hqd.setOracleDbUrl(str);
        }
     }
    {
      Long key = HypericServerInstallPrms.oracleDbUser;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null) {
        hqd.setOracleDbUser(str);
      }
    }
    {
      Long key = HypericServerInstallPrms.oracleDbPwd;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null) {
        hqd.setOracleDbPwd(str);
      }
    }
    {
      Long key = HypericServerInstallPrms.oracleAdminUser;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null) {
        hqd.setOracleAdminUser(str);
      }
    }
    {
      Long key = HypericServerInstallPrms.oracleAdminPwd;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null) {
        hqd.setOracleAdminPwd(str);
      }
    }
    {
        Long key = HypericServerInstallPrms.mysqlDbUrl;
        String str = tab.getString(key, tab.getWild(key, index, null));
        if (str != null) {
          hqd.setOracleDbUrl(str);
        }
      }
      {
          Long key = HypericServerInstallPrms.mysqlPort;
          String str = tab.getString(key, tab.getWild(key, index, null));
          if (str != null) {
            hqd.setOracleDbUrl(str);
          }
       }
      {
        Long key = HypericServerInstallPrms.mysqlDbUser;
        String str = tab.getString(key, tab.getWild(key, index, null));
        if (str != null) {
          hqd.setOracleDbUser(str);
        }
      }
      {
        Long key = HypericServerInstallPrms.mysqlDbPwd;
        String str = tab.getString(key, tab.getWild(key, index, null));
        if (str != null) {
          hqd.setOracleDbPwd(str);
        }
      }
      {
        Long key = HypericServerInstallPrms.mysqlAdminUser;
        String str = tab.getString(key, tab.getWild(key, index, null));
        if (str != null) {
          hqd.setOracleAdminUser(str);
        }
      }
      {
        Long key = HypericServerInstallPrms.mysqlAdminPwd;
        String str = tab.getString(key, tab.getWild(key, index, null));
        if (str != null) {
          hqd.setOracleAdminPwd(str);
        }
      }
    {
      Long key = HypericServerInstallPrms.agentHost;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null) {
        hqd.setAgentHost(str);
      }
    }
    {
      Long key = HypericServerInstallPrms.agentServerHost;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null) {
        hqd.setAgentServerHost(str);
      }
    }
    {
      Long key = HypericServerInstallPrms.agentPort;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null) {
        hqd.setAgentPort(str);
      }
    }
    return hqd;
  }

  /**
   * Returns the logical name for the HypericServerInstall description.
   */
  public String getName() {
    return this.name;
  }

  private void setName(String name) {
    this.name = name;
  }

  private void setHostDescriptionName(String str) {
    this.hostDescriptionName = str;
  }

  private void setHostDescription(HostDescription hd) {
    this.hostDescription = hd;
  }

  private void setBuildXmlFile(String buildXmlFile) {
    this.buildXmlFile = buildXmlFile;
  }
  
  private void setReleaseNum(String releaseNum) {
    this.releaseNum = releaseNum;
  }
  
  private void setBuildVersion(String buildVersion) {
    this.buildVersion = buildVersion;
  }
  
  private void setTmpDir(String buildCopyDir) {
    this.buildCopyDir = buildCopyDir;
  }
  
  
  private void setInstallDir(String installDir) {
    this.installDir = installDir;
  }
  
  private void setHostOS(String hostOS) {
    this.hostOS = hostOS;
  }
  
  private void setHQServerDb(String hqServerDb) {
    this.hqServerDb = hqServerDb;
  }
  
  private void setOracleDbUrl(String oracleDbUrl) {
    this.oracleDbUrl = oracleDbUrl;
  }
  
  private void setOracleDbUser(String oracleDbUser) {
    this.oracleDbUser = oracleDbUser;
  }
  
  private void setOracleDbPwd(String oracleDbPwd) {
    this.oracleDbPwd = oracleDbPwd;
  }
  
  private void setOracleAdminUser(String oracleAdminUser) {
    this.oracleAdminUser = oracleAdminUser;
  }
  
  private void setOracleAdminPwd(String oracleAdminPwd) {
    this.oracleAdminPwd = oracleAdminPwd;
  }
  
  private void setAgentHost(String agentHost) {
    this.agentHost = agentHost;
  }
  
  private void setAgentServerHost(String agentServerHost) {
    this.agentServerHost = agentServerHost;
  }
  
  private void setAgentPort(String agentPort) {
    this.agentPort = agentPort;
  }
}
