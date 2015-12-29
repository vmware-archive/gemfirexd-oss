package org.apache.ddlutils.platform.oracle;

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

import java.io.IOException;

import org.apache.ddlutils.Platform;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Table;

/**
 * The SQL builder for Oracle 10.
 * 
 * @version $Revision: $
 */
// GemStone changes BEGIN
public class Oracle10Builder extends Oracle9Builder
/* (original code)
public class Oracle10Builder extends Oracle8Builder
*/
// GemStone changes END
{
    /**
     * Creates a new builder instance.
     * 
     * @param platform The plaftform this builder belongs to
     */
    public Oracle10Builder(Platform platform)
    {
        super(platform);
    }

    /**
     * {@inheritDoc}
     */
    public void dropTable(Table table) throws IOException
    {
    	// The only difference to the Oracle 8/9 variant is the purge which prevents the
    	// table from being moved to the recycle bin (which is new in Oracle 10)
        Column[] columns = table.getAutoIncrementColumns();

        for (int idx = 0; idx < columns.length; idx++)
        {
            dropAutoIncrementTrigger(table, columns[idx]);
            dropAutoIncrementSequence(table, columns[idx]);
        }

        print("DROP TABLE ");
        printIdentifier(getTableName(table));
        print(" CASCADE CONSTRAINTS PURGE");
        printEndOfStatement();
    }
}
