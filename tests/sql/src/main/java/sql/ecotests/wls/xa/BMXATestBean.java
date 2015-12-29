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
import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import javax.transaction.UserTransaction;
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

@FileGeneration(remoteClass = Constants.Bool.TRUE, localHome = Constants.Bool.FALSE, remoteHome = Constants.Bool.TRUE, remoteClassName = "BMXATest", remoteHomeName = "BMXATestHome", localClass = Constants.Bool.FALSE)
@JarSettings(ejbClientJar = "test_client.jar")
@JndiName(remote = "weblogic-gfxdtest-BMXATestHome")
@ResourceRefs({
        @ResourceRef(name = "gfxdThinLocXADataSource", jndiName = "gfxdtest-thin-loc-xads", type = "javax.sql.DataSource", auth = ResourceRef.Auth.CONTAINER, sharingScope = ResourceRef.SharingScope.UNSHAREABLE),
        @ResourceRef(name = "gfxdPeerLocXADataSource", jndiName = "gfxdtest-peer-loc-xads", type = "javax.sql.DataSource", auth = ResourceRef.Auth.CONTAINER, sharingScope = ResourceRef.SharingScope.UNSHAREABLE),
        @ResourceRef(name = "gfxdThinMcXADataSource", jndiName = "gfxdtest-thin-mc-xads", type = "javax.sql.DataSource", auth = ResourceRef.Auth.CONTAINER, sharingScope = ResourceRef.SharingScope.UNSHAREABLE),
        @ResourceRef(name = "gfxdPeerMcXADataSource", jndiName = "gfxdtest-peer-mc-xads", type = "javax.sql.DataSource", auth = ResourceRef.Auth.CONTAINER, sharingScope = ResourceRef.SharingScope.UNSHAREABLE),
        @ResourceRef(name = "otherXADataSource", jndiName = "derby-thin-loc-xads", type = "javax.sql.DataSource", auth = ResourceRef.Auth.CONTAINER, sharingScope = ResourceRef.SharingScope.UNSHAREABLE) })

@Session(maxBeansInFreePool = "100", initialBeansInFreePool = "0", transTimeoutSeconds = "0", type = Session.SessionType.STATELESS, transactionType = Session.SessionTransactionType.BEAN, ejbName = "BMXATestEJB")
public class BMXATestBean extends GemFireXDXATestBean {
    @RemoteMethod
    public void testXACommitRollbackWithJMS(boolean toCommit, int dstype) throws GemFireXDXATestException {
		JMSQueueHelper jms = null;
        Connection gfxdConn = null;

        try {
            Context ictx = new InitialContext();
            Context env = (Context) ictx.lookup("java:comp/env");

            String queueConnFactoryName = (String) env.lookup("queueConnFactoryName");
            String queueName = (String) env.lookup("queueName");
			jms = new JMSQueueHelper(ictx, queueConnFactoryName, queueName);
			jms.init();

            String gfxdXADSName = (String) env.lookup(getGemFireXDDSName(dstype));
            DataSource gfxdxads = (DataSource) ictx.lookup(gfxdXADSName);
            gfxdConn = gfxdxads.getConnection();

            String tableName = (String) env.lookup("tableName");

            UserTransaction utx = getSessionContext().getUserTransaction();
            utx.begin();
            log("BM TRANSACTION BEGUN");
            String msgText = jms.receive();
			if (msgText == null) {
				log("***WARN!! Message is null**** ");
				return;
			} else {
				updateDatabase(gfxdConn, tableName, msgText);
			}
			if (toCommit) {
				utx.commit();
				log("BM TRANSACTION COMMIT");
			} else {
				utx.rollback();
				log("BM TRANSACTION ROLLBACK");
			}

        } catch (NamingException nex) {
            log("Naming exception: " + nex);
            throw new GemFireXDXATestException("Failed in test setting: ", nex);
        } catch (JMSException jex) {
            log("JMS exception: " + jex);
            throw new GemFireXDXATestException("Failed in test setting: ", jex);
        } catch (SQLException sqle) {
            log("SQL exception: " + sqle);
            throw new GemFireXDXATestException("Failed in test setting: ", sqle);
        } catch (Exception e) {
            log("EXCEPTION: " + e);
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


   	@RemoteMethod
   	public void testXACommitRollbackWithOtherDB(boolean toCommit, int dstype, String testData) throws GemFireXDXATestException {
        Connection gfxdConn = null;
        Connection dbConn = null;

        try {
            Context ictx = new InitialContext();

            Context env = (Context) ictx.lookup("java:comp/env");
            String tableName = (String) env.lookup("tableName");

            String gfxdXADSName = (String) env.lookup(getGemFireXDDSName(dstype));
            DataSource gfxdxads = (DataSource) ictx.lookup(gfxdXADSName);
            gfxdConn = gfxdxads.getConnection();

            String otherXADataSrcName = (String) env.lookup("otherXADataSrcName");
            DataSource otherxads = (DataSource) ictx.lookup(otherXADataSrcName);
            dbConn = otherxads.getConnection();

            UserTransaction utx = getSessionContext().getUserTransaction();
            utx.begin();
            log("BM TRANSACTION BEGUN");

			updateDatabase(gfxdConn, tableName, testData);
			updateDatabase(dbConn, tableName,testData);
			if (toCommit) {
				utx.commit();
				log("BM TRANSACTION COMMIT");
			} else {
				utx.rollback();
				log("BM TRANSACTION ROLLBACK");
			}

        } catch (NamingException nex) {
            log("Naming exception: " + nex);
            throw new GemFireXDXATestException("Failed in test setting: ", nex);
        } catch (SQLException sqle) {
            log("SQL exception: " + sqle);
            throw new GemFireXDXATestException("Failed in test setting: ", sqle);
        } catch (Exception e) {
            log("Exception: " + e);
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


   	@RemoteMethod
   	public void testXACommitRollbackWithSameDB(boolean toCommit, int dstype, String testData) throws GemFireXDXATestException {
        Connection gfxdConn = null;
        Connection dbConn = null;

        try {
            Context ictx = new InitialContext();

            Context env = (Context) ictx.lookup("java:comp/env");
            String tableName = (String) env.lookup("tableName");
            String tableName2 = (String) env.lookup("tableName2");

            String gfxdXADSName = (String) env.lookup(getGemFireXDDSName(dstype));
            DataSource gfxdxads = (DataSource) ictx.lookup(gfxdXADSName);
            gfxdConn = gfxdxads.getConnection();
            dbConn = gfxdxads.getConnection();

            UserTransaction utx = getSessionContext().getUserTransaction();
            utx.begin();
            log("BM TRANSACTION BEGUN");

			updateDatabase(gfxdConn, tableName, testData);
			updateDatabase(dbConn, tableName2, testData);
			if (toCommit) {
				utx.commit();
				log("BM TRANSACTION COMMIT");
			} else {
				utx.rollback();
				log("BM TRANSACTION ROLLBACK");
			}

        } catch (NamingException nex) {
            log("Naming exception: " + nex);
            throw new GemFireXDXATestException("Failed in test setting: ", nex);
        } catch (SQLException sqle) {
            log("SQL exception: " + sqle);
            throw new GemFireXDXATestException("Failed in test setting: ", sqle);
        } catch (Exception e) {
            log("Exception: " + e);
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
