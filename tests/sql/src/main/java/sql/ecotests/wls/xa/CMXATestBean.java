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
import javax.naming.NamingException;
import javax.sql.DataSource;
//import weblogic.ejb.GenericSessionBean;
import weblogic.ejbgen.*;
import weblogic.ejbgen.Session;


@EnvEntries({
        @EnvEntry(name = "queueName", type = "java.lang.String", value = "weblogic.gfxdtest.jms.gfxdQueue"),
        @EnvEntry(name = "queueConnFactoryName", type = "java.lang.String", value = "weblogic.gfxdtest.jms.QueueConnectionFactory"),
        @EnvEntry(name = "tableName", type = "java.lang.String", value = "gfxdXAtest"),
        @EnvEntry(name = "tableName2", type = "java.lang.String", value = "gfxdXAtest2"),
        @EnvEntry(name = "gfxdThinLocXADSName", type = "java.lang.String", value = "gfxdtest-thin-loc-xads"),
        @EnvEntry(name = "gfxdPeerLocXADSName", type = "java.lang.String", value = "gfxdtest-peer-loc-xads"),
        @EnvEntry(name = "gfxdThinMcXADSName", type = "java.lang.String", value = "gfxdtest-thin-mc-xads"),
        @EnvEntry(name = "gfxdPeerMcXADSName", type = "java.lang.String", value = "gfxdtest-peer-mc-xads"),
        @EnvEntry(name = "otherXADataSrcName", type = "java.lang.String", value = "derby-thin-loc-xads") })

@FileGeneration(remoteClass = Constants.Bool.TRUE, localHome = Constants.Bool.FALSE, remoteHome = Constants.Bool.TRUE, remoteClassName = "CMXATest", remoteHomeName = "CMXATestHome", localClass = Constants.Bool.FALSE)
@JndiName(remote = "weblogic-gfxdtest-CMXATestHome")
@ResourceRefs({
        @ResourceRef(name = "gfxdThinLocXADataSource", jndiName = "gfxdtest-thin-loc-xads", type = "javax.sql.DataSource", auth = ResourceRef.Auth.CONTAINER, sharingScope = ResourceRef.SharingScope.UNSHAREABLE),
        @ResourceRef(name = "gfxdPeerLocXADataSource", jndiName = "gfxdtest-peer-loc-xads", type = "javax.sql.DataSource", auth = ResourceRef.Auth.CONTAINER, sharingScope = ResourceRef.SharingScope.UNSHAREABLE),
        @ResourceRef(name = "gfxdThinMcXADataSource", jndiName = "gfxdtest-thin-mc-xads", type = "javax.sql.DataSource", auth = ResourceRef.Auth.CONTAINER, sharingScope = ResourceRef.SharingScope.UNSHAREABLE),
        @ResourceRef(name = "gfxdPeerMcXADataSource", jndiName = "gfxdtest-peer-mc-xads", type = "javax.sql.DataSource", auth = ResourceRef.Auth.CONTAINER, sharingScope = ResourceRef.SharingScope.UNSHAREABLE),
        @ResourceRef(name = "otherXADataSource", jndiName = "derby-thin-loc-xads", type = "javax.sql.DataSource", auth = ResourceRef.Auth.CONTAINER, sharingScope = ResourceRef.SharingScope.UNSHAREABLE) })

@Session(maxBeansInFreePool = "100", initialBeansInFreePool = "0", transTimeoutSeconds = "0", type = Session.SessionType.STATELESS, transactionType = Session.SessionTransactionType.CONTAINER, ejbName = "CMXATestEJB")
public class CMXATestBean extends GemFireXDXATestBean {
    private SessionContext sc = null;

    public void setSessionContext(SessionContext sc) {
		this.sc = sc;
	}

    @RemoteMethod(transactionAttribute = Constants.TransactionAttribute.REQUIRED)
    public void testXACommitRollbackWithJMS(boolean toCommit, int dstype) throws GemFireXDXATestException {
		JMSQueueHelper jms = null;
        Connection gfxdConn = null;

        try {
			log("CM TRANSACTION BEGUN");
            Context ictx = new InitialContext();
            Context env = (Context) ictx.lookup("java:comp/env");

            String queueConnFactoryName = (String) env.lookup("queueConnFactoryName");
            String queueName = (String) env.lookup("queueName");
            jms = new JMSQueueHelper(ictx, queueConnFactoryName, queueName);
            jms.init();

            String tableName = (String) env.lookup("tableName");
            String gfxdXADSName = (String) env.lookup(getGemFireXDDSName(dstype));
            DataSource gfxdxads = (DataSource) ictx.lookup(gfxdXADSName);
            gfxdConn = gfxdxads.getConnection();

            String msgText = jms.receive();
            if (msgText == null) {
				log("***WARN!! Message is null**** ");
				throw new GemFireXDXATestException("Failed in test setting: Message is null");
			} else {
				updateDatabase(gfxdConn, tableName, msgText);
				if (toCommit) {
					log("CM TRANSACTION COMMIT");
				} else {
					log("CM TRANSACTION ROLLBACK");
					sc.setRollbackOnly();
				}
			}
        } catch (NamingException nex) {
            log("Naming exception: " + nex);
            sc.setRollbackOnly();
            throw new GemFireXDXATestException("Failed in test setting: ", nex);
        } catch (JMSException jex) {
            log("JMS exception: " + jex);
            sc.setRollbackOnly();
            throw new GemFireXDXATestException("Failed in test setting: ", jex);
        } catch (SQLException sqle) {
            log("SQL exception: " + sqle);
            sc.setRollbackOnly();
            throw new GemFireXDXATestException("Failed in test setting: ", sqle);
        } catch (Exception e) {
			log("Exception: " + e);
			sc.setRollbackOnly();
			throw new GemFireXDXATestException("Failed in test setting: ", e);
        } finally {
			jms.closeReceiver();
            if (gfxdConn != null) {
                try {
                    gfxdConn.close();
                } catch (SQLException ex) {
                }
			}
        }
    }


   	@RemoteMethod(transactionAttribute = Constants.TransactionAttribute.REQUIRED)
   	public void testXACommitRollbackWithOtherDB(boolean toCommit, int dstype, String testData) throws GemFireXDXATestException {
        Connection gfxdConn = null;
        Connection dbConn = null;

        try {
			log("CM TRANSACTION BEGUN");
            Context ictx = new InitialContext();
            Context env = (Context) ictx.lookup("java:comp/env");

            String gfxdXADSName = (String) env.lookup(getGemFireXDDSName(dstype));
            DataSource gfxdxads = (DataSource) ictx.lookup(gfxdXADSName);
            gfxdConn = gfxdxads.getConnection();

            String otherXADataSrcName = (String) env.lookup("otherXADataSrcName");
            DataSource otherxads = (DataSource) ictx.lookup(otherXADataSrcName);
            dbConn = otherxads.getConnection();

            String tableName = (String) env.lookup("tableName");

			updateDatabase(gfxdConn, tableName, testData);
			updateDatabase(dbConn, tableName,testData);
			if (toCommit) {
				log("CM TRANSACTION COMMIT");
			} else {
				log("CM TRANSACTION ROLLBACK");
				sc.setRollbackOnly();
			}
        } catch (NamingException nex) {
            log("Naming exception: " + nex);
            sc.setRollbackOnly();
            throw new GemFireXDXATestException("Failed in test setting: ", nex);
        } catch (SQLException sqle) {
            log("SQL exception: " + sqle);
            sc.setRollbackOnly();
            throw new GemFireXDXATestException("Failed in test setting: ", sqle);
        } catch (Exception e) {
			log("Exception: " + e);
			sc.setRollbackOnly();
			throw new GemFireXDXATestException("Failed in test setting: ", e);
        } finally {
            if (gfxdConn != null) {
                try {
                    gfxdConn.close();
                } catch (SQLException ex) {
                }
            }
            if (dbConn != null) {
                try {
                    dbConn.close();
                } catch (SQLException ex) {
                }
            }
        }
	}


   	@RemoteMethod(transactionAttribute = Constants.TransactionAttribute.REQUIRED)
   	public void testXACommitRollbackWithSameDB(boolean toCommit, int dstype, String testData) throws GemFireXDXATestException {
        Connection gfxdConn = null;
        Connection dbConn = null;

        try {
			log("CM TRANSACTION BEGUN");
            Context ictx = new InitialContext();

            Context env = (Context) ictx.lookup("java:comp/env");
            String tableName = (String) env.lookup("tableName");
            String tableName2 = (String) env.lookup("tableName2");

            String gfxdXADSName = (String) env.lookup(getGemFireXDDSName(dstype));
            DataSource gfxdxads = (DataSource) ictx.lookup(gfxdXADSName);
            gfxdConn = gfxdxads.getConnection();
            dbConn = gfxdxads.getConnection();

			updateDatabase(gfxdConn, tableName, testData);
			updateDatabase(dbConn, tableName2, testData);
			if (toCommit) {
				log("CM TRANSACTION COMMIT");
			} else {
				log("CM TRANSACTION ROLLBACK");
				sc.setRollbackOnly();
			}

        } catch (NamingException nex) {
            log("Naming exception: " + nex);
            sc.setRollbackOnly();
            throw new GemFireXDXATestException("Failed in test setting: ", nex);
        } catch (SQLException sqle) {
            log("SQL exception: " + sqle);
            sc.setRollbackOnly();
            throw new GemFireXDXATestException("Failed in test setting: ", sqle);
        } catch (Exception e) {
			log("Exception: " + e);
			sc.setRollbackOnly();
			throw new GemFireXDXATestException("Failed in test setting: ", e);
        } finally {
            if (gfxdConn != null) {
                try {
                    gfxdConn.close();
                } catch (SQLException ex) {
                }
            }
            if (dbConn != null) {
                try {
                    dbConn.close();
                } catch (SQLException ex) {
                }
            }
        }
	}
}
