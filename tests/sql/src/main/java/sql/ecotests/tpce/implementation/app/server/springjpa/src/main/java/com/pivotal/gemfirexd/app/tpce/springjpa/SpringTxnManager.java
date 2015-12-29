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

import org.springframework.transaction.interceptor.TransactionAspectSupport;

import com.pivotal.gemfirexd.app.tpce.jpa.txn.JpaTxnManager;

public class SpringTxnManager implements JpaTxnManager {

	//Spring container will do the commit
	public void commit() {}

	public void rollback() {
		//Programmatic way to roll back current transaction
		TransactionAspectSupport.currentTransactionStatus( ).setRollbackOnly();
		
		//Declarative way to roll back current transaction
		//throw new RollbackIndicatorException();
	}

	//Spring container will start a transaction
	public void beginTransaction() {}

	public void close() {}

}
