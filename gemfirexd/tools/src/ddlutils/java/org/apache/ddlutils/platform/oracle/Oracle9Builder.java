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

package org.apache.ddlutils.platform.oracle;

import java.sql.Types;

import org.apache.ddlutils.Platform;
import org.apache.ddlutils.model.Column;

/**
 * SqlBuilder for Oracle9 platform. Currently for remapping native
 * DATE/TIMESTAMP types to Oracle's DATE/TIMESTAMP.
 */
public class Oracle9Builder extends Oracle8Builder {

  /**
   * Creates a new builder instance.
   * 
   * @param platform
   *          The plaftform this builder belongs to
   */
  public Oracle9Builder(Platform platform) {
    super(platform);
  }

  /**
   * {@inheritDoc}
   */
  protected String getNativeType(Column column) {
    // convert the type to DATE or TIMESTAMP depending on precision
    // the JDBC type was converted to TIMESTAMP for both by Oracle8ModelReader
    // getColumn method, but the precision will still help us determine the
    // actual type to use (see comments in #45096)
    final int typeCode = column.getTypeCode();
    if (typeCode == Types.TIMESTAMP) {
      if (column.getSizeAsInt() <= 7) {
        return "DATE";
      }
    }
    return super.getNativeType(column);
  }
}
