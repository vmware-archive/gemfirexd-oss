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
package com.pivotal.gemfirexd.app.tpce.springjpa;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import com.pivotal.gemfirexd.app.tpce.TPCETxnInput;
import com.pivotal.gemfirexd.app.tpce.TPCETxnOutput;
import com.pivotal.gemfirexd.app.tpce.jpa.txn.JpaTradeOrder;

@Repository

public class SpringTradeOrder extends JpaTradeOrder {
	public SpringTradeOrder() {
    	jpaTxnManager = new SpringTxnManager();
	}
	
	@PersistenceContext
	public void setEntityManager (EntityManager entityManager) {
		this.entityManager = entityManager;
	}	
	/*
	 * Seen two Exceptions during the developement:
	 * LazyInitializationException -- no session: it was because the method is not under the transaction
	 * InvalidIsolationLevelException: Standard JPA does not support custom isolation levels
	 * use a special JpaDialect for your JPA implementation: it was because trying to change the
	 * isolation level from Spring. Use default.
	 */
	//@Transactional(rollbackFor= com.pivotal.gemfirexd.app.tpce.springjpa.RollbackIndicatorException.class)
	@Transactional
	public TPCETxnOutput runTxn(TPCETxnInput txnInput) {
		return super.runTxn(txnInput);
	}
}
