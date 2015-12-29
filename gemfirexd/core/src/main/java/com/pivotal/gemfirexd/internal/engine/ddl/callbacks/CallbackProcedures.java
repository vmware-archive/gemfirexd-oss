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

package com.pivotal.gemfirexd.internal.engine.ddl.callbacks;

import java.util.Set;

import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.AttributesMutator;
import com.gemstone.gemfire.cache.CacheLoader;
import com.gemstone.gemfire.cache.CacheWriter;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.ClassPathLoader;
import com.gemstone.gemfire.internal.cache.EventErrorHandler;
import com.pivotal.gemfirexd.callbacks.EventCallback;
import com.pivotal.gemfirexd.callbacks.impl.GatewayConflictResolver;
import com.pivotal.gemfirexd.callbacks.GatewayEventErrorHandler;
import com.pivotal.gemfirexd.callbacks.RowLoader;
import com.pivotal.gemfirexd.callbacks.impl.GatewayConflictResolverWrapper;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.ddl.GfxdCacheListener;
import com.pivotal.gemfirexd.internal.engine.ddl.GfxdCacheLoader;
import com.pivotal.gemfirexd.internal.engine.ddl.GfxdCacheWriter;
import com.pivotal.gemfirexd.internal.engine.ddl.GfxdDDLRegionQueue;
import com.pivotal.gemfirexd.internal.engine.ddl.callbacks.messages.GfxdAddListenerMessage;
import com.pivotal.gemfirexd.internal.engine.ddl.callbacks.messages.GfxdRemoveGatewayConflictResolverMessage;
import com.pivotal.gemfirexd.internal.engine.ddl.callbacks.messages.GfxdRemoveGatewayEventErrorHandlerMessage;
import com.pivotal.gemfirexd.internal.engine.ddl.callbacks.messages.GfxdRemoveListenerMessage;
import com.pivotal.gemfirexd.internal.engine.ddl.callbacks.messages.GfxdRemoveLoaderMessage;
import com.pivotal.gemfirexd.internal.engine.ddl.callbacks.messages.GfxdRemoveWriterMessage;
import com.pivotal.gemfirexd.internal.engine.ddl.callbacks.messages.GfxdSetGatewayConflictResolverMessage;
import com.pivotal.gemfirexd.internal.engine.ddl.callbacks.messages.GfxdSetGatewayEventErrorHandlerMessage;
import com.pivotal.gemfirexd.internal.engine.ddl.callbacks.messages.GfxdSetLoaderMessage;
import com.pivotal.gemfirexd.internal.engine.ddl.callbacks.messages.GfxdSetWriterMessage;
import com.pivotal.gemfirexd.internal.engine.distributed.GfxdMessage;
import com.pivotal.gemfirexd.internal.engine.store.EventErrorHandlerWrapper;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.engine.store.ServerGroupUtils;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.TableDescriptor;
import com.pivotal.gemfirexd.internal.iapi.util.StringUtil;
import com.pivotal.gemfirexd.internal.impl.jdbc.Util;
import com.pivotal.gemfirexd.internal.shared.common.SharedUtils;

/**
 * Contains the body of the procedures which are used to create/remove
 * GfxdCacheListeners and GfxdCacheWriter
 * 
 * @author kneeraj
 * 
 */
public class CallbackProcedures {

  /** static map to ensure uniqueness of schema + listener name */
  private static final ConcurrentHashMap<String, GfxdCacheListener> idListenerMap
    = new ConcurrentHashMap<String, GfxdCacheListener>();

  /**
   * 
   * @param schemaName
   * @param tableName
   * @param implementation
   */
  public static void addGfxdCacheListener(final String id,
      final String schemaName, final String tableName, String implementation,
      String initInfo, String serverGroups) throws StandardException {
    // NULL table name, id or implementation is illegal
    // schema name can be null, and initInfo is nullable
    if ((tableName == null) || (id == null) || (implementation == null)) {
      throw StandardException.newException(SQLState.ENTITY_NAME_MISSING);
    }

    final GemFireContainer container = getContainerForTable(schemaName,
        tableName);

    final Set<DistributedMember> members = GfxdMessage.getAllGfxdMembers();
    final DistributedMember myId = Misc.getGemFireCache().getMyId();
    boolean self = members.remove(myId);
    if (self) {
      addGfxdCacheListenerLocally(id, container, implementation, initInfo,
          serverGroups);
    }
    GfxdAddListenerMessage listenerMsg = new GfxdAddListenerMessage(id,
        container.getSchemaName(), container.getTableName(), implementation,
        initInfo, serverGroups);
    try {
      final GfxdDDLRegionQueue queue = Misc.getMemStore().getDDLStmtQueue();
      final long replayKey = queue.newUUID();
      listenerMsg.setReplayKey(replayKey);
      queue.put(Long.valueOf(replayKey), listenerMsg);
      listenerMsg.send(Misc.getDistributedSystem(), members);
    } catch (StandardException se) {
      throw se;
    } catch (Exception ex) {
      throw StandardException.newException(
          SQLState.UNEXPECTED_EXCEPTION_FOR_LISTENER, ex, id, ex.toString());
    }
  }

  public static void addGfxdCacheListenerLocally(String id,
      final GemFireContainer container, String implementation,
      String initInfo, String serverGroups) throws StandardException {

    // skip listener for nodes not in the provided server groups or not in those
    // of the table
    if (skipExecutionForGroupsAndTable(serverGroups, container)) {
      return;
    }

    @SuppressWarnings("unchecked")
    final Region<Object, Object> region = container.getRegion();
    final AttributesMutator<Object, Object> mutator = region
        .getAttributesMutator();
    try {
      final EventCallback ecb = (EventCallback)ClassPathLoader.getLatest()
          .forName(implementation).newInstance();
      ecb.init(initInfo);
      final GfxdCacheListener aListener = new GfxdCacheListener(id, ecb);
      id = Misc.getFullTableName(container.getSchemaName(), id, null);
      final GfxdCacheListener prevListener = idListenerMap.putIfAbsent(id,
          aListener);
      if (prevListener != null) {
        throw Util.newEmbedSQLException(
            SQLState.LANG_OBJECT_ALREADY_EXISTS_IN_OBJECT, new Object[] {
                "listener", id, "schema", container.getSchemaName() }, null);
      }
      mutator.addCacheListener(aListener);
    } catch (Exception ex) {
      throw StandardException.newException(
          SQLState.UNEXPECTED_EXCEPTION_FOR_LISTENER, ex, id, ex.toString());
    }
  }

  /**
   * 
   * @param schemaName
   * @param tableName
   * @param implementation
   */
  public static void addGfxdCacheWriter(final String schemaName,
      final String tableName, String implementation, String initInfo,
      String serverGroups) throws StandardException {
    // NULL table name or implementation is illegal
    // schema name can be null, and initInfo is nullable
    if ((tableName == null) || (implementation == null)) {
      throw StandardException.newException(SQLState.ENTITY_NAME_MISSING);
    }

    final GemFireContainer container = getContainerForTable(schemaName,
        tableName);
    GfxdSetWriterMessage writerMsg = new GfxdSetWriterMessage(
        container.getSchemaName(), container.getTableName(), implementation,
        initInfo, serverGroups);
    final Set<DistributedMember> members = GfxdMessage.getAllGfxdMembers();
    final DistributedMember myId = Misc.getGemFireCache().getMyId();
    boolean self = members.remove(myId);
    if (self) {
      addGfxdCacheWriterLocally(container, implementation, initInfo,
          serverGroups);
    }
    try {
      final GfxdDDLRegionQueue queue = Misc.getMemStore().getDDLStmtQueue();
      final long replayKey = queue.newUUID();
      writerMsg.setReplayKey(replayKey);
      queue.put(Long.valueOf(replayKey), writerMsg);
      writerMsg.send(Misc.getDistributedSystem(), members);
    } catch (StandardException se) {
      throw se;
    } catch (Exception ex) {
      throw StandardException.newException(
          SQLState.UNEXPECTED_EXCEPTION_FOR_WRITER, ex,
          container.getQualifiedTableName(), ex.toString());
    }
  }

  public static void addGfxdCacheWriterLocally(
      final GemFireContainer container, String implementation, String initInfo,
      String serverGroups) throws StandardException {

    // skip writer for nodes not in the provided server groups or not in those
    // of the table
    if (skipExecutionForGroupsAndTable(serverGroups, container)) {
      return;
    }

    @SuppressWarnings("unchecked")
    final Region<Object, Object> reg = container.getRegion();
    final AttributesMutator<Object, Object> mutator = reg
        .getAttributesMutator();
    try {
      final EventCallback ecb = (EventCallback)ClassPathLoader.getLatest()
          .forName(implementation).newInstance();
      ecb.init(initInfo);
      final GfxdCacheWriter writer = new GfxdCacheWriter(ecb);
      mutator.setCacheWriter(writer);
    } catch (Exception ex) {
      throw StandardException.newException(
          SQLState.UNEXPECTED_EXCEPTION_FOR_WRITER, ex,
          container.getQualifiedTableName(), ex.toString());
    }
  }

  public static void addGfxdGatewayConflictResolver(
      final String implementation, String initInfo) throws StandardException{
    // implementation is non null, initInfo can be null
    if(implementation == null) {
      throw StandardException.newException(SQLState.ENTITY_NAME_MISSING);
    }
    try {
      // send message to all nodes to set the "loader-installed" flag
      final Set<DistributedMember> members = GfxdMessage.getAllGfxdMembers();
      final DistributedMember myId = Misc.getGemFireCache().getMyId();
      boolean self = members.remove(myId);
      if (self) {
        addGfxdGatewayConflictResolverLocally(implementation, initInfo);
      }

      GfxdSetGatewayConflictResolverMessage resolverMsg = new GfxdSetGatewayConflictResolverMessage(
          implementation, initInfo);
      
      final GfxdDDLRegionQueue queue = Misc.getMemStore().getDDLStmtQueue();
      final long replayKey = queue.newUUID();
      resolverMsg.setReplayKey(replayKey);
      queue.put(Long.valueOf(replayKey), resolverMsg);
      resolverMsg.send(Misc.getDistributedSystem(), members);
    } catch (StandardException se) {
      throw se;
    } catch (Exception ex) {
      throw StandardException.newException(
          SQLState.UNEXPECTED_EXCEPTION_FOR_CONFLICT_RESOLVER, ex, ex.toString());
    }    
    
  }
  
  public static void addGfxdGatewayConflictResolverLocally(String implementation, String initInfo) throws StandardException{
    try {
      final GatewayConflictResolver resolver = (GatewayConflictResolver)ClassPathLoader
          .getLatest().forName(implementation).newInstance();
      resolver.init(initInfo);

      GatewayConflictResolverWrapper wrapper = new GatewayConflictResolverWrapper(
          resolver);
      Misc.getGemFireCache().setGatewayConflictResolver(wrapper);

    } catch (Exception ex) {
      throw StandardException.newException(
          SQLState.UNEXPECTED_EXCEPTION_FOR_CONFLICT_RESOLVER, ex, ex.toString());
    }
  }
  
  public static void addGfxdGatewayEventErrorHandler(
      final String implementation, String initInfo) throws StandardException{
    // implementation is non null, initInfo can be null
    if(implementation == null) {
      throw StandardException.newException(SQLState.ENTITY_NAME_MISSING);
    }
    try {
      // send message to all nodes to set the "loader-installed" flag
      final Set<DistributedMember> members = GfxdMessage.getAllGfxdMembers();
      final DistributedMember myId = Misc.getGemFireCache().getMyId();
      boolean self = members.remove(myId);
      if (self) {
        addGfxdGatewayEventErrorHandlerLocally(implementation, initInfo);
      }

      GfxdSetGatewayEventErrorHandlerMessage handlerMsg = new GfxdSetGatewayEventErrorHandlerMessage(
          implementation, initInfo);
      
      final GfxdDDLRegionQueue queue = Misc.getMemStore().getDDLStmtQueue();
      final long replayKey = queue.newUUID();
      handlerMsg.setReplayKey(replayKey);
      queue.put(Long.valueOf(replayKey), handlerMsg);
      handlerMsg.send(Misc.getDistributedSystem(), members);
    } catch (StandardException se) {
      throw se;
    } catch (Exception ex) {
      throw StandardException.newException(
          SQLState.UNEXPECTED_EXCEPTION_FOR_EVENT_ERROR_HANDLER, ex, ex.toString());
    }    
  }
  public static void addGfxdGatewayEventErrorHandlerLocally(String implementation, String initInfo) throws StandardException{
    try {
      final GatewayEventErrorHandler handler = (GatewayEventErrorHandler)ClassPathLoader
          .getLatest().forName(implementation).newInstance();
      handler.init(initInfo);

      EventErrorHandler internalHandler = Misc.getGemFireCache()
          .getEventErrorHandler();
      if (internalHandler instanceof EventErrorHandlerWrapper) {
        EventErrorHandlerWrapper wrapper = (EventErrorHandlerWrapper)internalHandler;
        wrapper.setCustomGatewayEventErrorHandler(handler);
      }
    } catch (Exception ex) {
      throw StandardException.newException(
          SQLState.UNEXPECTED_EXCEPTION_FOR_EVENT_ERROR_HANDLER, ex,
          ex.toString());
    }
  }

  public static void removeGfxdGatewayConflictResolver() throws StandardException{
    try {
      // send message to all nodes to set the "loader-installed" flag
      final Set<DistributedMember> members = GfxdMessage.getAllGfxdMembers();
      final DistributedMember myId = Misc.getGemFireCache().getMyId();
      boolean self = members.remove(myId);
      if (self) {
        removeGfxdGatewayConflictResolverLocally();
      }

      GfxdRemoveGatewayConflictResolverMessage resolverMsg = new GfxdRemoveGatewayConflictResolverMessage();
      
      final GfxdDDLRegionQueue queue = Misc.getMemStore().getDDLStmtQueue();
      final long replayKey = queue.newUUID();
      resolverMsg.setReplayKey(replayKey);
      queue.put(Long.valueOf(replayKey), resolverMsg);
      resolverMsg.send(Misc.getDistributedSystem(), members);
    } catch (StandardException se) {
      throw se;
    } catch (Exception ex) {
      throw StandardException.newException(
          SQLState.UNEXPECTED_EXCEPTION_FOR_CONFLICT_RESOLVER, ex, ex.toString());
    }    
  }
  
  
  public static void removeGfxdGatewayConflictResolverLocally() throws StandardException{
    Misc.getGemFireCache().setGatewayConflictResolver(null);
  }


  public static void removeGfxdGatewayEventErrorHandler() throws StandardException{
    try {
      final Set<DistributedMember> members = GfxdMessage.getAllGfxdMembers();
      final DistributedMember myId = Misc.getGemFireCache().getMyId();
      boolean self = members.remove(myId);
      if (self) {
        removeGfxdGatewayEventErrorHandlerLocally();
      }

      GfxdRemoveGatewayEventErrorHandlerMessage handlerMsg = new GfxdRemoveGatewayEventErrorHandlerMessage();
      
      final GfxdDDLRegionQueue queue = Misc.getMemStore().getDDLStmtQueue();
      final long replayKey = queue.newUUID();
      handlerMsg.setReplayKey(replayKey);
      queue.put(Long.valueOf(replayKey), handlerMsg);
      handlerMsg.send(Misc.getDistributedSystem(), members);
    } catch (StandardException se) {
      throw se;
    } catch (Exception ex) {
      throw StandardException.newException(
          SQLState.UNEXPECTED_EXCEPTION_FOR_EVENT_ERROR_HANDLER, ex, ex.toString());
    }    
  }
  
  
  public static void removeGfxdGatewayEventErrorHandlerLocally() throws StandardException{
    EventErrorHandler internalHandler = Misc.getGemFireCache().getEventErrorHandler();
    if(internalHandler instanceof EventErrorHandlerWrapper) {
      EventErrorHandlerWrapper wrapper = (EventErrorHandlerWrapper)internalHandler;
      wrapper.setCustomGatewayEventErrorHandler(null);
    }
  }
  
  public static void addGfxdCacheLoader(final String schemaName,
      final String tableName, String implementation, String initInfo)
      throws StandardException {
    // NULL table name, id or implementation is illegal
    // schema name can be null, and initInfo is nullable
    if ((tableName == null) || (implementation == null)) {
      throw StandardException.newException(SQLState.ENTITY_NAME_MISSING);
    }

    final GemFireContainer container = getContainerForTable(schemaName,
        tableName);
    try {
      // send message to all nodes to set the "loader-installed" flag
      final Set<DistributedMember> members = GfxdMessage.getAllGfxdMembers();
      final DistributedMember myId = Misc.getGemFireCache().getMyId();
      boolean self = members.remove(myId);
      if (self) {
        addGfxdCacheLoaderLocally(container, implementation, initInfo);
      }

      GfxdSetLoaderMessage loaderMsg = new GfxdSetLoaderMessage(
          container.getSchemaName(), container.getTableName(), implementation,
          initInfo);
      final GfxdDDLRegionQueue queue = Misc.getMemStore().getDDLStmtQueue();
      final long replayKey = queue.newUUID();
      loaderMsg.setReplayKey(replayKey);
      queue.put(Long.valueOf(replayKey), loaderMsg);
      loaderMsg.send(Misc.getDistributedSystem(), members);
    } catch (StandardException se) {
      throw se;
    } catch (Exception ex) {
      throw StandardException.newException(
          SQLState.UNEXPECTED_EXCEPTION_FOR_LOADER, ex,
          container.getQualifiedTableName(), ex.toString());
    }
  }

  public static void addGfxdCacheLoaderLocally(
      final GemFireContainer container, String implementation, String initInfo)
      throws StandardException {

    @SuppressWarnings("unchecked")
    final Region<Object, Object> reg = container.getRegion();
    LogWriter logger = Misc.getCacheLogWriter();
    if (logger.fineEnabled()) {
      logger.fine("CallbackProcedures::addGfxdCacheLoaderLocally: "
          + "setting loader info as true for container=" + container);
    }
    container.setLoaderInstalled(true);

    // skip loader for nodes not those of the table
    if (skipExecutionForGroupsAndTable(null, container)) {
      return;
    }

    try {
      final AttributesMutator<Object, Object> mutator = reg
          .getAttributesMutator();
      final RowLoader rldr = (RowLoader)ClassPathLoader.getLatest()
          .forName(implementation).newInstance();
      rldr.init(initInfo);
      final GfxdCacheLoader loader = new GfxdCacheLoader(
          container.getSchemaName(), container.getTableName(), rldr);
      TableDescriptor td = container.getTableDescriptor();
      assert td != null;
      loader.setTableDetails(td);
      mutator.setCacheLoader(loader);
    } catch (StandardException se) {
      throw se;
    } catch (Exception ex) {
      throw StandardException.newException(
          SQLState.UNEXPECTED_EXCEPTION_FOR_LOADER, ex,
          container.getQualifiedTableName(), ex.toString());
    }
  }

  /**
   * 
   * @param id
   * @param schemaName
   * @param tableName
   */
  public static void removeGfxdCacheListener(final String id,
      final String schemaName, final String tableName) throws StandardException {
    // NULL id or tablename is illegal
    // schema name can be null
    if ((tableName == null) || (id == null)) {
      throw StandardException.newException(SQLState.ENTITY_NAME_MISSING);
    }

    final GemFireContainer container = getContainerForTable(schemaName,
        tableName);
    final Set<DistributedMember> members = GfxdMessage.getOtherMembers();
    removeGfxdCacheListenerLocally(id, container);
    GfxdRemoveListenerMessage removeMsg = new GfxdRemoveListenerMessage(id,
        container.getSchemaName(), container.getTableName());
    try {
      final GfxdDDLRegionQueue queue = Misc.getMemStore().getDDLStmtQueue();
      final long replayKey = queue.newUUID();
      removeMsg.setReplayKey(replayKey);
      queue.put(Long.valueOf(replayKey), removeMsg);
      removeMsg.send(Misc.getDistributedSystem(), members);
    } catch (StandardException se) {
      throw se;
    } catch (Exception ex) {
      throw StandardException.newException(
          SQLState.UNEXPECTED_EXCEPTION_FOR_LISTENER, ex, id, ex.toString());
    }
  }

  public static void removeGfxdCacheListenerLocally(String id,
      final GemFireContainer container) throws StandardException {

    id = Misc.getFullTableName(container.getSchemaName(), id, null);
    final GfxdCacheListener aListener = idListenerMap.remove(id);
    if (aListener == null) {
      return;
    }
    @SuppressWarnings("unchecked")
    final Region<Object, Object> reg = container.getRegion();
    try {
      final AttributesMutator<Object, Object> mutator = reg
          .getAttributesMutator();
      mutator.removeCacheListener(aListener);
      aListener.close();
    } catch (Exception ex) {
      throw StandardException.newException(
          SQLState.UNEXPECTED_EXCEPTION_FOR_LISTENER, ex, id, ex.toString());
    }
  }

  /**
   * 
   * @param schemaName
   * @param tableName
   */
  public static void removeGfxdCacheWriter(final String schemaName,
      final String tableName) throws StandardException {
    // NULL table name is illegal
    // schema name can be null
    if (tableName == null) {
      throw StandardException.newException(SQLState.ENTITY_NAME_MISSING);
    }

    final GemFireContainer container = getContainerForTable(schemaName,
        tableName);
    final Set<DistributedMember> members = GfxdMessage.getOtherMembers();
    removeGfxdCacheWriterLocally(container);
    GfxdRemoveWriterMessage removeMsg = new GfxdRemoveWriterMessage(
        container.getSchemaName(), container.getTableName());
    try {
      final GfxdDDLRegionQueue queue = Misc.getMemStore().getDDLStmtQueue();
      final long replayKey = queue.newUUID();
      removeMsg.setReplayKey(replayKey);
      queue.put(Long.valueOf(replayKey), removeMsg);
      removeMsg.send(Misc.getDistributedSystem(), members);
    } catch (StandardException se) {
      throw se;
    } catch (Exception ex) {
      throw StandardException.newException(
          SQLState.UNEXPECTED_EXCEPTION_FOR_WRITER, ex,
          container.getQualifiedTableName(), ex.toString());
    }
  }

  public static void removeGfxdCacheWriterLocally(
      final GemFireContainer container) throws StandardException {

    @SuppressWarnings("unchecked")
    final Region<Object, Object> reg = container.getRegion();
    final CacheWriter<Object, Object> prevWriter = reg.getAttributes()
        .getCacheWriter();
    if (prevWriter == null) {
      return;
    }
    try {
      final AttributesMutator<Object, Object> mutator = reg
          .getAttributesMutator();
      mutator.setCacheWriter(null);
    } catch (Exception ex) {
      throw StandardException.newException(
          SQLState.UNEXPECTED_EXCEPTION_FOR_WRITER, ex,
          container.getQualifiedTableName(), ex.toString());
    }
  }

  public static void removeGfxdCacheLoader(final String schemaName,
      final String tableName) throws StandardException {
    // NULL table name is illegal
    // schema name can be null
    if (tableName == null) {
      throw StandardException.newException(SQLState.ENTITY_NAME_MISSING);
    }

    final GemFireContainer container = getContainerForTable(schemaName,
        tableName);
    final Set<DistributedMember> members = GfxdMessage.getOtherMembers();
    removeGfxdCacheLoaderLocally(container);
    GfxdRemoveLoaderMessage removeMsg = new GfxdRemoveLoaderMessage(
        container.getSchemaName(), container.getTableName());
    try {
      final GfxdDDLRegionQueue queue = Misc.getMemStore().getDDLStmtQueue();
      final long replayKey = queue.newUUID();
      removeMsg.setReplayKey(replayKey);
      queue.put(Long.valueOf(replayKey), removeMsg);
      removeMsg.send(Misc.getDistributedSystem(), members);
    } catch (StandardException se) {
      throw se;
    } catch (Exception ex) {
      throw StandardException.newException(
          SQLState.UNEXPECTED_EXCEPTION_FOR_LOADER, ex,
          container.getQualifiedTableName(), ex.toString());
    }
  }

  public static void removeGfxdCacheLoaderLocally(
      final GemFireContainer container) throws StandardException {

    @SuppressWarnings("unchecked")
    final Region<Object, Object> reg = container.getRegion();
    final CacheLoader<Object, Object> prevLoader = reg.getAttributes()
        .getCacheLoader();
    if (prevLoader == null) {
      return;
    }
    try {
      final AttributesMutator<Object, Object> mutator = reg
          .getAttributesMutator();
      mutator.setCacheLoader(null);
      container.setLoaderInstalled(false);
    } catch (Exception ex) {
      throw StandardException.newException(
          SQLState.UNEXPECTED_EXCEPTION_FOR_LOADER, ex,
          container.getQualifiedTableName(), ex.toString());
    }
  }

  // kneeraj: Just so that the CallbackProcedures class comes in the
  // gemfirexd.jar
  public static void dummy() {
  }

  public static void clearStatics() {
    idListenerMap.clear();
  }

  /**
   * Get the {@link GemFireContainer} for given schema.table. The arguments can
   * be case-sensitive or case-insensitive since this method will try for both.
   * If the schema is null, then the current schema is tried.
   */
  public static GemFireContainer getContainerForTable(String schemaName,
      String tableName) throws StandardException {
    GemFireContainer container;
    if (schemaName == null || schemaName.length() == 0) {
      schemaName = Misc.getDefaultSchemaName(null);
    }
    Region<Object, Object> reg = Misc.getRegionByPath(
        Misc.getRegionPath(schemaName, tableName, null), false);
    if (reg == null
        || (container = (GemFireContainer)reg.getUserAttribute()) == null) {
      // assume case-insensitive
      if (schemaName != null && schemaName.length() > 0) {
        schemaName = StringUtil.SQLToUpperCase(schemaName);
      }
      tableName = StringUtil.SQLToUpperCase(tableName);
      reg = Misc.getRegionByPath(
          Misc.getRegionPath(schemaName, tableName, null), false);
      if (reg == null
          || (container = (GemFireContainer)reg.getUserAttribute()) == null) {
        throw StandardException.newException(SQLState.LANG_TABLE_NOT_FOUND,
            Misc.getFullTableName(schemaName, tableName, null));
      }
    }
    return container;
  }

  /**
   * Returns true if execution of a message should be skipped on this node if
   * not in the provided server groups or not in those of the given table.
   */
  @SuppressWarnings("unchecked")
  public static boolean skipExecutionForGroupsAndTable(final String groups,
      final GemFireContainer container) throws StandardException {

    SortedSet<String> serverGroups;
    if (groups != null && groups.length() > 0) {
      serverGroups = SharedUtils.toSortedSet(groups, false);
      if (!ServerGroupUtils.isGroupMember(serverGroups)) {
        return true;
      }
    }
    final TableDescriptor td;
    if (container != null) {
      if ((td = container.getTableDescriptor()) == null
          || !container.isDataStore()) {
        return true;
      }
      serverGroups = td.getDistributionDescriptor().getServerGroups();
      if (!ServerGroupUtils.isGroupMember(serverGroups)) {
        return true;
      }
    }
    return false;
  }
}
