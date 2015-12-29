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
package com.pivotal.gemfirexd.app.tpce.jpa.jse;
import javax.persistence.*;

import com.pivotal.gemfirexd.app.tpce.jpa.txn.JpaTxnManager;

/**
 * The Java utility class to bootstrap JPA.
 */

public class JseJpaTxnManager implements JpaTxnManager {
	private static final String PERSISTENCE_UNIT = "tpce-persistence";
	private static final EntityManagerFactory entityManagerFactory = Persistence.createEntityManagerFactory(PERSISTENCE_UNIT);
    	
    private EntityManager entityManager = null; 
    private EntityTransaction entityTransaction = null;
    
    public JseJpaTxnManager() {
    	entityManager = entityManagerFactory.createEntityManager();
    	entityTransaction = entityManager.getTransaction();
    }
    
    public void beginTransaction() {
    	entityTransaction.begin();
    }

    public void commit() {
    	entityTransaction.commit();
    }
    
    public void rollback() {
    	entityTransaction.rollback();
    }
    
    public void close() {
    	if (entityManager != null && entityManager.isOpen()) {
    		entityManager.close();
    	}
    }
    
    public EntityManager getEntityManager() {
    	return entityManager;
    }
}