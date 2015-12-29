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

package com.pivotal.gemfirexd.internal.engine.access;

import java.util.Properties;

import com.pivotal.gemfirexd.internal.catalog.UUID;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.monitor.ModuleControl;
import com.pivotal.gemfirexd.internal.iapi.services.monitor.Monitor;
import com.pivotal.gemfirexd.internal.iapi.services.uuid.UUIDFactory;
import com.pivotal.gemfirexd.internal.iapi.store.access.ColumnOrdering;
import com.pivotal.gemfirexd.internal.iapi.store.access.conglomerate.ConglomerateFactory;
import com.pivotal.gemfirexd.internal.iapi.store.access.conglomerate.TransactionManager;
import com.pivotal.gemfirexd.internal.iapi.store.raw.ContainerKey;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;

/**
 * Base class for common functionality of GemFireXD conglomerate factories.
 * 
 * @author swale
 */
public abstract class MemConglomerateFactory implements ConglomerateFactory,
    ModuleControl {

  private final String formatUUIDString;

  private UUID formatUUID;

  private final String primaryId;

  private final String secondaryId;

  private final int factoryId;

  protected MemConglomerateFactory(String formatUUIDString, String primaryId,
      String secondaryId, int factoryId) {
    this.formatUUIDString = formatUUIDString;
    this.primaryId = primaryId;
    this.secondaryId = secondaryId;
    this.factoryId = factoryId;
  }

  /*
   ** Methods of MethodFactory (via ConglomerateFactory)
   */

  public final Properties defaultProperties() {
    return new Properties();
  }

  /**
   * Return whether this access method implements the implementation type given
   * in the argument string.
   **/
  public final boolean supportsImplementation(String implementationId) {
    return (implementationId.equals(this.primaryId) || (this.secondaryId !=
        null && implementationId.equals(this.secondaryId)));
  }

  /**
   * Return the primary implementation type for this access method.
   **/
  public final String primaryImplementationType() {
    return this.primaryId;
  }

  /**
   * Return whether this access method supports the format supplied in the
   * argument.
   **/
  public final boolean supportsFormat(UUID formatId) {
    return formatId.equals(this.formatUUID);
  }

  /**
   * Return the primary format that this access method supports.
   **/
  public final UUID primaryFormat() {
    return this.formatUUID;
  }

  /*
   ** Methods of ConglomerateFactory
   */

  /**
   * Return the conglomerate factory id.
   * <p>
   * Return a number in the range of 0-15 which identifies this factory. Code
   * which names conglomerates depends on this range currently, but could be
   * easily changed to handle larger ranges. One hex digit seemed reasonable for
   * the number of conglomerate types being currently considered (heap, btree,
   * gist, gist btree, gist rtree, hash, others? ).
   * <p>
   * 
   * @see ConglomerateFactory#getConglomerateFactoryId
   * 
   * @return an unique identifier used to the factory into the conglomid.
   **/
  public final int getConglomerateFactoryId() {
    return this.factoryId;
  }

  /*
   ** Methods of ModuleControl.
   */

  public final boolean canSupport(String identifier, Properties startParams) {
    String impl = startParams.getProperty("gemfirexd.access.Conglomerate.type");
    if (impl == null) {
      return false;
    }
    return supportsImplementation(impl);
  }

  public final void boot(boolean create, Properties startParams)
      throws StandardException {
    // Find the UUID factory.
    final UUIDFactory uuidFactory = Monitor.getMonitor().getUUIDFactory();

    // Make a UUID that identifies this conglomerate's format.
    this.formatUUID = uuidFactory.recreateUUID(this.formatUUIDString);
  }

  public final void stop() {
  }

  /*
   ** Methods of ConglomerateFactory
   */

  /**
   * Create the conglomerate and return a conglomerate object for it.
   * 
   * @see ConglomerateFactory#createConglomerate
   * 
   * @exception StandardException
   *              Standard exception policy.
   */
  public abstract MemConglomerate createConglomerate(
      TransactionManager xact_mgr, int segment, long containerId,
      DataValueDescriptor[] template, ColumnOrdering[] columnOrder,
      int[] collationIds, Properties properties, int temporaryFlag)
      throws StandardException;

  /**
   * Return Conglomerate object for conglomerate with conglomid.
   * 
   * @return An instance of the conglomerate.
   * 
   * @exception StandardException
   *              Standard exception policy.
   */
  public final MemConglomerate readConglomerate(
      TransactionManager xact_manager, ContainerKey containerKey)
      throws StandardException {
    return Misc.getMemStore().findConglomerate(containerKey);
  }
}
