
package org.apache.ddlutils.platform.gemfirexd;

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
 * Adapted from DerbyModelReader for GemFireXD distributed data platform.
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

import org.apache.ddlutils.Platform;
import org.apache.ddlutils.model.ForeignKey;
import org.apache.ddlutils.model.Index;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.platform.DatabaseMetaDataWrapper;
import org.apache.ddlutils.platform.derby.DerbyModelReader;

/**
 * Reads a database model from a GemFireXD database.
 * 
 * @version $Revision: $
 */
public class GemFireXDModelReader extends DerbyModelReader {

  /**
   * Creates a new model reader for GemFireXD databases.
   * 
   * @param platform
   *          The platform that this model reader belongs to
   */
  public GemFireXDModelReader(Platform platform) {
    super(platform);
  }

  /**
   * {@inheritDoc}
   */
  protected boolean isInternalForeignKeyIndex(DatabaseMetaDataWrapper metaData,
      Table table, ForeignKey fk, Index index) {
    return true;
  }

  /**
   * {@inheritDoc}
   */
  protected boolean isInternalPrimaryKeyIndex(DatabaseMetaDataWrapper metaData,
      Table table, Index index) {
    return true;
  }
}
