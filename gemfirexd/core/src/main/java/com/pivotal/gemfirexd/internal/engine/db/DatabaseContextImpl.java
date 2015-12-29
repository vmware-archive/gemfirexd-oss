
/*

 Derived from source files from the Derby project.

 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to you under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.

 */

/*
 * Changes for GemFireXD distributed data platform (some marked by "GemStone changes")
 *
 * Portions Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
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

package com.pivotal.gemfirexd.internal.engine.db;

import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.iapi.db.Database;
import com.pivotal.gemfirexd.internal.iapi.db.DatabaseContext;
import com.pivotal.gemfirexd.internal.iapi.error.ExceptionSeverity;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.context.ContextImpl;
import com.pivotal.gemfirexd.internal.iapi.services.context.ContextManager;
import com.pivotal.gemfirexd.internal.iapi.services.context.ContextService;
import com.pivotal.gemfirexd.internal.iapi.services.monitor.Monitor;

/**
 * A context that shutdowns down the database on a database exception.
 * 
 * @author Eric Zoerner
 */
final class DatabaseContextImpl extends ContextImpl implements DatabaseContext {

  /** DOCUMENT ME! */
  private final Database db;

  /**
   * Creates a new DatabaseContextImpl object.
   * 
   * @param cm
   *          DOCUMENT ME!
   * @param db
   *          DOCUMENT ME!
   */
  DatabaseContextImpl(ContextManager cm, Database db) {
    super(cm, DatabaseContextImpl.CONTEXT_ID);
    this.db = db;
  }

  /**
   * DOCUMENT ME!
   * 
   * @param t
   *          DOCUMENT ME!
   */
  public void cleanupOnError(Throwable t) {

    if (!(t instanceof StandardException)) {
      return;
    }

    StandardException se = (StandardException)t;

    // Ensure the context is popped if the session is
    // going away.
    if (se.getSeverity() < ExceptionSeverity.SESSION_SEVERITY) {
      return;
    }

    popMe();

    // Asif:The bug is that the CacheClosedException may have been thrown by
    // remote cache but it would cause local cache shutdown, so check if the
    // current cache is genuinely closed or was it remote
    final GemFireCacheImpl cache = Misc.getGemFireCacheNoThrow();
    if (se.getSeverity() == ExceptionSeverity.DATABASE_SEVERITY
        && (cache == null
            || cache.getCancelCriterion().cancelInProgress() != null)) {
      ContextService.getFactory().notifyAllActiveThreads(this);
      Monitor.getMonitor().shutdown(db);
    }
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof DatabaseContext) {
      return ((DatabaseContextImpl)other).db == db;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return db.hashCode();
  }

  public Database getDatabase() {
    return db;
  }
}
