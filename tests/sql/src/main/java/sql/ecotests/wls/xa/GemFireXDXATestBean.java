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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import javax.ejb.CreateException;
import javax.ejb.SessionContext;
import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.sql.DataSource;
import javax.transaction.UserTransaction;
import weblogic.ejb.GenericSessionBean;
//import weblogic.ejbgen.*;
//import weblogic.ejbgen.Session;

public class GemFireXDXATestBean extends GenericSessionBean {

    public static final int THIN_XADS_TO_LOCATOR = 0;
	public static final int PEER_XADS_TO_LOCATOR = 1;
	public static final int THIN_XADS_TO_MULTICAST = 2;
	public static final int PEER_XADS_TO_MULTICAST = 3;


    protected static final boolean VERBOSE = true;

    protected void log(String s) {
        if (VERBOSE)
            System.out.println(s);
    }

    public void ejbCreate() throws CreateException {
    }

    public void ejbPostCreate() throws CreateException {
    }

    protected void updateDatabase(Connection conn, String tableName, String data) {
        PreparedStatement stmt = null;
        try {
            String sql = "insert into app." + tableName + " (data) values (?)";
            stmt = conn.prepareStatement(sql);
            stmt.setString(1, data);
            stmt.executeUpdate();
        } catch (SQLException ex) {
            log("Cannot update database with " + data + ": " + ex);
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ex) {
                }
            }
        }
    }

    protected String getGemFireXDDSName(int dstype) {
		String dsName = "gfxdThinLocXADSName";
		switch (dstype) {
			case THIN_XADS_TO_LOCATOR:
				dsName = "gfxdThinLocXADSName";
				break;
			case PEER_XADS_TO_LOCATOR:
				dsName = "gfxdPeerLocXADSName";
				break;
			case THIN_XADS_TO_MULTICAST:
				dsName = "gfxdThinMcXADSName";
				break;
			case PEER_XADS_TO_MULTICAST:
				dsName = "gfxdPeerMcXADSName";
				break;
			default:
		}
		return dsName;
	}
}
