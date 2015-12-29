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

package com.pivotal.gemfirexd.internal.engine;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Iterator;
import java.util.Map;

import com.pivotal.gemfirexd.internal.iapi.sql.ResultColumnDescriptor;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedResultSetMetaData;
import com.pivotal.gemfirexd.internal.impl.store.raw.data.GfxdJarResource;

public final class GfxdJarHandler extends GfxdVTITemplate implements
    GfxdVTITemplateNoAllNodesRoute {

  private Iterator<Map.Entry<String, Long>> jarNameMapIterator;
  private Object[] currentEntry;

  @Override
  public boolean next() throws SQLException {
    if (this.jarNameMapIterator == null) {
      GfxdJarResource jarResource = Misc.getMemStoreBooting()
          .getJarFileHandler();
      if (jarResource == null) {
        return false;
      }
      this.jarNameMapIterator = jarResource.getNameToIDMap().entrySet()
          .iterator();
    }
    if (this.jarNameMapIterator != null) {
      if (this.jarNameMapIterator.hasNext()) {
        Map.Entry<String, Long> e = this.jarNameMapIterator.next();
        String schema = "";
        String alias = e.getKey();
        int dotIndex = alias.indexOf('.');
        if (dotIndex != -1) {
          schema = alias.substring(0, dotIndex);
          alias = alias.substring(dotIndex + 1);
        }
        this.currentEntry = new Object[] { schema, alias, e.getValue() };
        return true;
      }
    }
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected Object getObjectForColumn(int columnNumber) throws SQLException {
    return this.currentEntry[columnNumber - 1];
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    return metadata;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() throws SQLException {
    this.jarNameMapIterator = null;
    this.currentEntry = null;
  }

  //to enable this class to be included in gemfirexd.jar
  public static void dummy() {
  }
  
  /** Metadata */

  public static final String SCHEMA = "SCHEMA";
  public static final String ALIAS = "ALIAS";
  public static final String ID = "ID";

  private static final ResultColumnDescriptor[] columnInfo = {
      EmbedResultSetMetaData.getResultColumnDescriptor(SCHEMA, Types.VARCHAR,
          false, 256),
      EmbedResultSetMetaData.getResultColumnDescriptor(ALIAS, Types.VARCHAR,
          false, 256),
      EmbedResultSetMetaData.getResultColumnDescriptor(ID, Types.BIGINT, false),
  };

  private static final ResultSetMetaData metadata = new EmbedResultSetMetaData(
      columnInfo);
}
