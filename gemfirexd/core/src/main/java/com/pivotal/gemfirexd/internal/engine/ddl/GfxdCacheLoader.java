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

package com.pivotal.gemfirexd.internal.engine.ddl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.CacheLoader;
import com.gemstone.gemfire.cache.CacheLoaderException;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.LoaderHelper;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.internal.cache.KeyWithRegionContext;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.execute.InternalFunctionInvocationTargetException;
import com.pivotal.gemfirexd.callbacks.RowLoader;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.GfxdDataSerializable;
import com.pivotal.gemfirexd.internal.engine.GfxdSerializable;
import com.pivotal.gemfirexd.internal.engine.distributed.FunctionExecutionException;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.engine.store.RegionKey;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.ColumnDescriptorList;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.TableDescriptor;

/**
 * This is a kind of helper implementation for CacheLoader which will load the
 * gemfire cache as DVDArray when Region's cacheloader's load method will be
 * invoked. It will also call the INSERT on the conn which it has with it, so
 * that the derby artifacts, like indexes etc get updated with this INSERT
 * statement but the INSERT here would not be actually inserted into the table
 * as the load function on returning will load the region with the dvdarray.
 * 
 * @author Kumar Neeraj
 * @since 6.0
 */
public class GfxdCacheLoader implements CacheLoader<Object, Object> {

  private int[] pkIndexes = null;

  private RowLoader gfxdLdr = null;

  private String schemaName = null;

  private String tableName = null;

  private ColumnDescriptorList columnDescriptorList = null;

  private int numberOfColumns = -1;

  /**
   * This constructor will be called at the time of parsing of the createTable
   * statement itself.
   * 
   * @param schemaName
   *          The schemaName of the table
   * @param tableName
   *          The tableName
   * @param columnNames
   *          The list of names of the columns of the table
   * @param gfxdLdr
   * 
   * @throws StandardException
   */
  public GfxdCacheLoader(String schemaName, String tableName,
      final RowLoader gfxdLdr) {
    /*
    final LanguageConnectionContext insertlcc = GemFireXDUtils
        .setupContextManager(null, null, false, false, false);
    insertlcc.setInsertEnabled(false);
    */
    this.schemaName = schemaName;
    this.tableName = tableName;
    this.gfxdLdr = gfxdLdr;
  }

  public RowLoader getRowLoader() {
    return this.gfxdLdr;
  }

  public Object load(LoaderHelper<Object, Object> helper)
      throws CacheLoaderException {
    RegionKey primaryKey = null;
    try {
      primaryKey = (RegionKey)helper.getKey();
      Object[] columnValues = new Object[this.numberOfColumns];
      Object[] primaryKeyAsJavaObjectArray = new Object[primaryKey.nCols()];
      primaryKey.getKeyColumns(primaryKeyAsJavaObjectArray);
      for (int index = 0; index < primaryKeyAsJavaObjectArray.length; ++index) {
        columnValues[pkIndexes[index] - 1] = primaryKeyAsJavaObjectArray[index];
      }

      Object row = this.gfxdLdr.getRow(this.schemaName, this.tableName,
          primaryKeyAsJavaObjectArray);
      if (row != null) {
        GemFireContainer gfcontainer = (GemFireContainer)helper.getRegion()
            .getUserAttribute();
        final Object value = gfcontainer.makeValueAsPerStorage(row);
        if (value != null) {
          return value;
        }
      }
    } catch (Throwable t) {
      if (GemFireXDUtils.retryToBeDone(t)) {
        throw new InternalFunctionInvocationTargetException(t);
      }
      throw new CacheLoaderException("exception while loading a row", t);
    }
    throw new EntryNotFoundException("no value obtained for key: " + primaryKey
        + " from row loader");
  }

  public void close() {
  }

  /**
   * Keeping this method as public as the ALTER TABLE thread can directly call
   * this function on this object and all the details pertaining to the table
   * can be reset.
   * 
   * @throws StandardException
   */
  public void setTableDetails(TableDescriptor td) throws StandardException {
    if (td == null) {
      return;
    }
    this.columnDescriptorList = td.getColumnDescriptorList();
    this.numberOfColumns = this.columnDescriptorList.size();
    this.pkIndexes = GemFireXDUtils.getPrimaryKeyColumns(td);
  }

  public int getNumberOfColumns() {
    return this.numberOfColumns;
  }

  @Override
  public String toString() {
    // return the name of the underlying implementation (#44125)
    return String.valueOf(this.gfxdLdr);
  }

  @SuppressWarnings("serial")
  public static final class GetRowFunction implements Function, Declarable {

    public final static String ID = "gfxd-GetRowFunction";

    /**
     * Added for tests that use XML for comparison of region attributes.
     * 
     * @see Declarable#init(Properties)
     */
    @Override
    public void init(Properties props) {
      // nothing required for this function
    }

    private GfxdCacheLoader getRowLoaderFromRegion(Region<?, ?> region) {
      Object ldr = region.getAttributes().getCacheLoader();
      if (ldr instanceof GfxdCacheLoader) {
        return (GfxdCacheLoader)ldr;
      }
      throw new IllegalStateException("Expected a GfxdCacheLoader in region: "
          + region.getName());
    }

    public void execute(FunctionContext context) {
      GetRowFunctionArgs args = (GetRowFunctionArgs)context.getArguments();
      String tableName = args.getTableName();
      String schemaName = args.getSchemaName();
      final LocalRegion region = (LocalRegion)Misc.getRegion(Misc
          .getRegionPath(schemaName, tableName, null), true, false);
      final GemFireContainer gfcontainer = (GemFireContainer)region
          .getUserAttribute();
      final GfxdCacheLoader ldr = getRowLoaderFromRegion(region);
      final RowLoader rowLdr = ldr.getRowLoader();
      final RegionKey key = (RegionKey)args.getKey();
      if (region.keyRequiresRegionContext()) {
        ((KeyWithRegionContext)key).setRegionContext(region);
      }
      Object[] pkdarr = new Object[key.nCols()];
      try {
        key.getKeyColumns(pkdarr);
        Object row = rowLdr.getRow(schemaName, tableName, pkdarr);
        Object value = gfcontainer.makeValueAsPerStorage(row);
        /*
        try {
          gfcontainer.insert(key, value);
        }
        catch (StandardException ex) {
          boolean ignorableException = ex.getSQLState().equals(
              SQLState.LANG_DUPLICATE_KEY_CONSTRAINT);
          if (ignorableException) {
            DataValueDescriptor[] dvdarr = valueAsStored.getDVDArray();
            GfxdPartitionResolver gfxdpr = (GfxdPartitionResolver)region
                .getAttributes().getPartitionAttributes()
                .getPartitionResolver();
            Object callbackarg = gfxdpr.getRoutingObjectFromDvdArray(dvdarr);
            value = region.get(key, callbackarg);
          }
          else {
            throw ex;
          }
        }
        */
        context.getResultSender().lastResult((Serializable)value);
      } catch (Exception e) {
        throw new FunctionExecutionException(e);
      }
    }

    public String getId() {
      return ID;
    }

    public boolean hasResult() {
      return true;
    }

    public boolean optimizeForWrite() {
      return true;
    }
  
    public boolean isHA() {
      // don't let GFE layer retry since the list of members can change and will
      // be determined by ServerGroupUtils.GetServerGroupMembers
      return false;
    }
  }

  public static final class GetRowFunctionArgs extends GfxdDataSerializable {

    private String schema;

    private String table;

    private Object key;

    public GetRowFunctionArgs() {
    }

    public GetRowFunctionArgs(String schemaName, String tableName, Object gfKey) {
      this.schema = schemaName;
      this.table = tableName;
      this.key = gfKey;
    }

    @Override
    public byte getGfxdID() {
      return GfxdSerializable.GETROW_ARGS;
    }

    public String getTableName() {
      return this.table;
    }

    public String getSchemaName() {
      return this.schema;
    }

    public Object getKey() {
      return this.key;
    }

    @Override
    public void toData(final DataOutput out) throws IOException {
      super.toData(out);
      DataSerializer.writeString(this.schema, out);
      DataSerializer.writeString(this.table, out);
      DataSerializer.writeObject(this.key, out);
    }

    @Override
    public void fromData(DataInput in) throws IOException,
            ClassNotFoundException {
      super.fromData(in);
      this.schema = DataSerializer.readString(in);
      this.table = DataSerializer.readString(in);
      this.key = DataSerializer.readObject(in);
    }
  }
}
