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
package com.gemstone.gemfire.internal.jta;

import java.util.Properties;

import junit.framework.TestCase;

import javax.transaction.*;

import com.gemstone.gemfire.distributed.DistributedSystem;

public class UserTransactionImplTest extends TestCase {

	private static TransactionManagerImpl tm = TransactionManagerImpl.getTransactionManager();
	private UserTransaction utx = null;
	static {
	    try {
	      Properties props = new Properties ();      
//	      props.setProperty("mcast-port", "10340");
	      DistributedSystem.connect(props);
	    } catch (Exception e) {
	      fail("Unable to create Distributed System. Exception=" + e);
	    }
	  }
	public UserTransactionImplTest(String name){
		super(name);		
	}
	
	protected void setUp(){
		try{
		utx = new UserTransactionImpl();
		}
		catch(Exception e){
			fail("Exception in setup due to "+e);
			e.printStackTrace();
		}
	}
	
	protected void tearDown(){
	}
	
	public void testBegin() {
		try{
		utx.begin();
		Transaction txn = tm.getTransaction();
		if (txn==null)
			fail("transaction not registered in the transaction map");
		GlobalTransaction gtx = tm.getGlobalTransaction();
		if (gtx==null)
			fail("Global transaction not registered with the transaction manager");
		if(!gtx.getTransactions().contains(txn))
			fail("Transaction not added to the list");
		int status = gtx.getStatus();
		if (status != Status.STATUS_ACTIVE)
			fail("Transaction status not set to be active");
		utx.commit();
		}		
		catch(Exception e){
			fail("Exception occured in testBegin due to "+e);
			e.printStackTrace();			
		}
	}
	
	public void testCommit() {	
	try{
		utx.begin();	
		utx.commit();				
		Transaction txn = tm.getTransaction();
		if (txn != null)
			fail("transaction not removed from map after commit");
	}
	catch(Exception e){
		fail("Exception occured in testCommit due to "+e);
		e.printStackTrace();
	}
	}
	
	
	public void testRollback() {
		try{
			utx.begin();
			utx.rollback();
			
			Transaction txn = tm.getTransaction();
			if (txn != null)
				fail("transaction not removed from map after rollback");
		}
		catch(Exception e){
			fail("Exception occured in testRollback due to "+e);
			e.printStackTrace();
		}

	}
	
	
	public void testSetRollbackOnly() {
		try{
			utx.begin();
			utx.setRollbackOnly();
			GlobalTransaction gtx = tm.getGlobalTransaction();
			if (gtx.getStatus()!= Status.STATUS_MARKED_ROLLBACK)
				fail("Status not marked for rollback");
			utx.rollback();
		}
		catch(Exception e){
			fail("Exception occured in testSetRollbackOnly due to "+e);
			e.printStackTrace();
		}
	}
	
	
	public void testGetStatus() {
		try{
			utx.begin();
			tm.setRollbackOnly();
			int status = utx.getStatus();
			if (status != Status.STATUS_MARKED_ROLLBACK)
				fail("Get status failed to get correct status");
			utx.rollback();
		}
		catch(Exception e){
			fail("Exception occured in testGetStatus due to "+e);
			e.printStackTrace();
		}
	}
	
	public void testThread(){
		try {
			utx.begin();
				utx.commit();			 
			utx.begin();
			utx.commit();
		} catch (Exception e) {
			fail("test Thread failed due to "+e);
			e.printStackTrace();
		} 	
	}
	
}
