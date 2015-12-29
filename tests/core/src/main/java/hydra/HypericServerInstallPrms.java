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

public class HypericServerInstallPrms extends BasePrms {

	static {
		setValues(HypericServerInstallPrms.class);
	}
	
	public static Long names;
	
	public static Long hostName;
	
	public static Long buildXmlFile;
	
	public static Long releaseNum;
	
	public static Long buildVersion;
	
	public static Long buildCopyDir;
	
	public static Long installDir;
	
	public static Long hostOS;
	
	public static Long hqServerDb;
	
	// Oracle DB Settings
	public static Long oracleDbUrl;
	
	public static Long oracleSID;
	
	public static Long oracleDbUser;
	
	public static Long oracleDbPwd;
	
	public static Long oracleAdminUser;
	
	public static Long oracleAdminPwd;
	
	// MySQL DB Settings
    public static Long mysqlDbUrl;
    
    public static Long mysqlPort;
    
    public static Long mysqlDbUser;
    
    public static Long mysqlDbPwd;
    
    public static Long mysqlAdminUser;
    
    public static Long mysqlAdminPwd;
	
	public static Long agentHost;
	
	public static Long agentServerHost;
	
	public static Long agentPort;
	
	
}
