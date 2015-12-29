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
package diskRecovery;

import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.TransactionEvent;
import com.gemstone.gemfire.cache.TransactionListener;

import java.util.Properties;

import util.AbstractListener;
import util.TestException;

/**
 * @author lynn
 *
 */
public class ConverterTxListener extends AbstractListener implements TransactionListener, Declarable {
  
  /** Constructor. This allows or disallows instantiation of this class
   *  depending on the value of a boolean serialized in a well-known file.
   *  If the file does not exist, allow instantiation. If the file does
   *  exist, allow instantiation if the Boolean in the file is true, disallow
   *  instantiation by throwing an exception if the boolean in the file is
   *  false.
   * 
   */
  public ConverterTxListener() {
    super();
    if (!InstantiationHelper.allowInstantiation()) {
       throw new TestException(this.getClass().getName() + " should not be instantiated");
    }
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.CacheCallback#close()
   */
  public void close() {
    logCall("close", null);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.TransactionListener#afterCommit(com.gemstone.gemfire.cache.TransactionEvent)
   */
  public void afterCommit(TransactionEvent event) {
    logTxEvent("afterCommit", event);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.TransactionListener#afterFailedCommit(com.gemstone.gemfire.cache.TransactionEvent)
   */
  public void afterFailedCommit(TransactionEvent event) {
    logTxEvent("afterFailedCommit", event);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.TransactionListener#afterRollback(com.gemstone.gemfire.cache.TransactionEvent)
   */
  public void afterRollback(TransactionEvent event) {
    logTxEvent("afterRollback", event);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.Declarable#init(java.util.Properties)
   */
  public void init(Properties props) {
    
  }

}
