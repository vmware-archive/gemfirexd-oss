package org.apache.ddlutils.task;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.ddlutils.Platform;
import org.apache.tools.ant.BuildException;

/**
 * Base type for commands that have the database info embedded.
 * 
 * @version $Revision: 289996 $
 * @ant.type ignore="true"
 */
public abstract class DatabaseCommand extends Command
{
    /** The platform configuration. */
// GemStone changes BEGIN
    protected PlatformConfiguration _platformConf = new PlatformConfiguration();
    /* (original code)
    private PlatformConfiguration _platformConf = new PlatformConfiguration();
    */
// GemStone changes END

    /**
     * Returns the database type.
     * 
     * @return The database type
     */
    protected String getDatabaseType()
    {
        return _platformConf.getDatabaseType();
    }

    /**
     * Returns the data source to use for accessing the database.
     * 
     * @return The data source
     */
    protected BasicDataSource getDataSource()
    {
        return _platformConf.getDataSource();
    }

    /**
     * Returns the catalog pattern if any.
     * 
     * @return The catalog pattern
     */
    public String getCatalogPattern()
    {
        return _platformConf.getCatalogPattern();
    }

    /**
     * Returns the schema pattern if any.
     * 
     * @return The schema pattern
     */
    public String getSchemaPattern()
    {
        return _platformConf.getSchemaPattern();
    }

    /**
     * Sets the platform configuration.
     * 
     * @param platformConf The platform configuration
     */
    protected void setPlatformConfiguration(PlatformConfiguration platformConf)
    {
        _platformConf = platformConf;
    }

    /**
     * Creates the platform for the configured database.
     * 
     * @return The platform
     */
    protected Platform getPlatform() throws BuildException
    {
// GemStone changes BEGIN
        final Platform platform = _platformConf.getPlatform();
        platform.setIsolationLevel(_isolationLevel);
        return platform;
        /* (original code)
        return _platformConf.getPlatform();
        */
// GemStone changes END
    }

    /**
     * {@inheritDoc}
     */
    public boolean isRequiringModel()
    {
        return true;
    }
// GemStone changes BEGIN

    // the isolation level to use for DB operations
    protected int _isolationLevel = -1;

    // flag to indicate that identity column has to be added at the end
    // of a load using ALTER TABLE (only supported by GemFireXD for now)
    protected boolean _addIdentityUsingAlterTable = false;

    /**
     * Get the current isolation level set for this platform. If none is set
     * then a value < 0 is returned.
     */
    public final int getIsolationLevel() {
      return _isolationLevel;
    }

    /**
     * Set the transaction isolation level to use for all DB operations.
     */
    public void setIsolationLevel(int level) {
      _isolationLevel = level;
    }

    /**
     * Returns true if identity column has to be added at the end of a load
     * using ALTER TABLE (only supported by GemFireXD for now)
     */
    public final boolean isAddIdentityUsingAlterTable() {
      return _addIdentityUsingAlterTable;
    }

    /**
     * Set the flag for {@link #isAddIdentityUsingAlterTable()}.
     */
    public final void setAddIdentityUsingAlterTable(boolean v) {
      _addIdentityUsingAlterTable = v;
    }
// GemStone changes END
}
