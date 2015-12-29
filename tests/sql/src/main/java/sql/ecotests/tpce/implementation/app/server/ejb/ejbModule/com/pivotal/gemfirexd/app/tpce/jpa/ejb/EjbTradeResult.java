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
package com.pivotal.gemfirexd.app.tpce.jpa.ejb;



import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.ejb.SessionContext;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import com.pivotal.gemfirexd.app.tpce.jpa.txn.JpaTradeResult;

/**
 * Session Bean implementation class EJBTradeResult
 */
@Stateless
@LocalBean
@TransactionAttribute(TransactionAttributeType.REQUIRED)
public class EjbTradeResult extends JpaTradeResult implements EjbTradeResultRemote, EjbTradeResultLocal {
	@Resource
	public SessionContext context;
	
    public EjbTradeResult() {}
    
    @PostConstruct
    public void setJpaTxnManager() {
    	jpaTxnManager = new EjbJpaTxnManager(context);
    }
    
	@PersistenceContext
	public void setEntityManager (EntityManager entityManager) {
		this.entityManager = entityManager;
	}	
}
