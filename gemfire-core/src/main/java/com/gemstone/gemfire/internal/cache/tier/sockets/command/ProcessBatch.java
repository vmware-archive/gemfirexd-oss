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
/**
 * 
 */
package com.gemstone.gemfire.internal.cache.tier.sockets.command;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.KeyWithRegionContext;
import com.gemstone.gemfire.internal.cache.GatewayEventImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.tier.BatchException;
import com.gemstone.gemfire.internal.cache.tier.CachedRegionHelper;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.*;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.security.AuthorizeRequest;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.operations.*;
import com.gemstone.gemfire.distributed.internal.DistributionStats;
import com.gemstone.org.jgroups.util.StringId;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * @author ashahid
 * 
 */
// Gateway Backward Compatibilty.
// If new gemfire version needs to be added because of GatewayEventImpl versioning 
// then this class needs to extend to support new version of GatewayEventImpl.

public class ProcessBatch extends BaseCommand {

  private final static ProcessBatch singleton = new ProcessBatch();

  private final   BaseCommand gfxdBulkDMLSubcommand;

  private final static Set<String> notFoundRegions = new HashSet<String>();

  private final static Object notFoundRegionsSync = new Object();

  public static Command getCommand() {
    return singleton;
  }

  private ProcessBatch() {
    BaseCommand temp = null;
    if (GemFireCacheImpl.gfxdSystem()) {
      try {
        final Class<?> clazz = Class.forName("com.pivotal.gemfirexd"
            + ".internal.engine.ddl.wan.GfxdBulkDMLCommand");
        temp = (BaseCommand)clazz.newInstance();
      } catch (Exception e) {
      }
    }
    gfxdBulkDMLSubcommand = temp;
  }

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long startp)
      throws IOException, ClassNotFoundException, InterruptedException {
    long start = startp;
    Part regionNamePart = null, keyPart = null, valuePart = null, callbackArgPart = null;
    String regionName = null;
    Object callbackArg = null, key = null;
    int partNumber = 0;
    CachedRegionHelper crHelper = servConn.getCachedRegionHelper();
    CacheServerStats stats = servConn.getCacheServerStats();
//    boolean versioningEnabled = servConn.getClientVersion().ordinal >= Version.GFE_70_ORDINAL && GatewayBatchOp.VERSION_WITH_OLD_WAN;
    EventID eventId = null;
    // requiresResponse = true; let PROCESS_BATCH deal with this itself
    {
      long oldStart = start;
      start = DistributionStats.getStatTime();
      stats.incReadProcessBatchRequestTime(start - oldStart);
    }
    Part callbackArgExistsPart;
    // Get early ack flag. This test should eventually be moved up above this
    // switch
    // statement so that all messages can take advantage of it.
    boolean earlyAck = msg.getEarlyAck();

    stats.incBatchSize(msg.getPayloadLength());

    // Retrieve the number of events
    Part numberOfEventsPart = msg.getPart(0);
    int numberOfEvents = numberOfEventsPart.getInt();

    // Retrieve the batch id
    Part batchIdPart = msg.getPart(1);
    int batchId = batchIdPart.getInt();

    // logger.warning("Received process batch request " + batchId);
    if (batchId == -1) {
      // This batch was sent during failover processing by the new
      // primary. The events in this batch may have already been sent
      // in a previous batch.
      if (logger.infoEnabled()) {
        logger.info(
          LocalizedStrings.ProcessBatch_RECEIVED_PROCESS_BATCH_REQUEST_FROM_A_NEW_PRIMARY_GATEWAY_CONTAINING_EVENTS_THAT_MAY_HAVE_ALREADY_BEEN_PROCESSED_THIS_PROCESS_BATCH_REQUEST_WILL_BE_PROCESSED);
      }
    }
    else {
      // If this batch has already been seen, do not reply.
      // Instead, drop the batch and continue.
      if (batchId == servConn.getLatestBatchIdReplied()) {

        if (APPLY_RETRIES) {
          // Do nothing!!!
          if (logger.warningEnabled()) {
            logger.warning(
              LocalizedStrings.ProcessBatch_RECEIVED_PROCESS_BATCH_REQUEST_0_THAT_HAS_ALREADY_BEEN_OR_IS_BEING_PROCESSED_GEMFIRE_GATEWAY_APPLYRETRIES_IS_SET_SO_THIS_BATCH_WILL_BE_PROCESSED_ANYWAY,
              batchId);
          }
        }
        else {
          if (logger.warningEnabled()) {
            logger.warning(
                  LocalizedStrings.ProcessBatch_RECEIVED_PROCESS_BATCH_REQUEST_0_THAT_HAS_ALREADY_BEEN_OR_IS_BEING_PROCESSED__THIS_PROCESS_BATCH_REQUEST_IS_BEING_IGNORED,
                  batchId);
          }
          return;
        }
      }

      // Verify the batches arrive in order
      if (batchId != servConn.getLatestBatchIdReplied() + 1) {
        if (logger.warningEnabled()) {
          logger.warning(
            LocalizedStrings.ProcessBatch_RECEIVED_PROCESS_BATCH_REQUEST_0_OUT_OF_ORDER_THE_ID_OF_THE_LAST_BATCH_PROCESSED_WAS_1_THIS_BATCH_REQUEST_WILL_BE_PROCESSED_BUT_SOME_MESSAGES_MAY_HAVE_BEEN_LOST,
            new Object[] {Integer.valueOf(batchId), Integer.valueOf(servConn.getLatestBatchIdReplied())});
        }
      }
    }

    if (logger.fineEnabled()) {
      logger.fine("Received process batch request " + batchId
          + " that will be processed.");
    }
    // System.out.println("Received process batch request " + batchId + " that
    // will be processed.");

    // Thread.sleep(12000);
    // If early ack mode, acknowledge right away
    if (earlyAck) {
      // Increment the batch id unless the received batch id is -1 (a failover
      // batch)
      if (batchId != -1) {
        servConn.incrementLatestBatchIdReplied(batchId);
      }
      writeReply(msg, servConn);
      servConn.setAsTrue(RESPONDED);
      {
        long oldStart = start;
        start = DistributionStats.getStatTime();
        stats.incWriteProcessBatchResponseTime(start - oldStart);
      }
    }

    // Retrieve the events from the message parts. The '2' below
    // represents the number of events (part0) and the batchId (part1)
    partNumber = 2;

    if (logger.fineEnabled()) {
      logger.fine(servConn.getName() + ": Received process batch request "
          + batchId + " containing " + numberOfEvents + " events ("
          + msg.getPayloadLength() + " bytes) with "
          + (earlyAck ? "early" : "normal") + " acknowledgement on "
          + servConn.getSocketString());
      if (earlyAck) {
        logger.fine(servConn.getName()
            + ": Sent process batch early response for batch " + batchId
            + " containing " + numberOfEvents + " events ("
            + msg.getPayloadLength() + " bytes) with "
            + (earlyAck ? "early" : "normal") + " acknowledgement on "
            + servConn.getSocketString());
      }
    }
    // logger.warning("Received process batch request " + batchId + " containing
    // " + numberOfEvents + " events (" + msg.getPayloadLength() + " bytes) with
    // " + (earlyAck ? "early" : "normal") + " acknowledgement on " +
    // getSocketString());
    // if (earlyAck) {
    // logger.warning("Sent process batch early response for batch " + batchId +
    // " containing " + numberOfEvents + " events (" + msg.getPayloadLength() +
    // " bytes) with " + (earlyAck ? "early" : "normal") + " acknowledgement on
    // " + getSocketString());
    // }

    // Keep track of whether a response has been written for
    // exceptions
    
    boolean wroteResponse = earlyAck;
    for (int i = 0; i < numberOfEvents; i++) {
      int extraParts = 0;

      // System.out.println("Processing event " + i + " in batch " + batchId + "
      // starting with part number " + partNumber);
      Part actionTypePart = msg.getPart(partNumber);
      int actionType = actionTypePart.getInt();

      //Part possibleDuplicatePart = msg.getPart(partNumber + 1);
      //byte[] possibleDuplicatePartBytes = (byte[])possibleDuplicatePart
      //    .getObject();
      //boolean possibleDuplicate = possibleDuplicatePartBytes[0] == 0x01;

      boolean callbackArgExists = false;

      EntryEventImpl clientEvent = null;

      try {
        // Make sure instance variables are null before each iteration
        regionName = null;
        key = null;
        callbackArg = null;

        switch (actionType) {
        case GatewayEventImpl.CREATE_ACTION: // Create
          // Retrieve the region name from the message parts
          regionNamePart = msg.getPart(partNumber + 2);
          regionName = regionNamePart.getString();

          // Retrieve the event id from the message parts
          // This was going to be used to determine possible
          // duplication of events, but it is unused now. In
          // fact the event id is overridden by the FROM_GATEWAY
          // token.
          Part eventIdPart = msg.getPart(partNumber + 3);
          // String eventId = eventIdPart.getString();
          try {
            eventId = (EventID)eventIdPart.getObject();
          }
          catch (Exception e) {
            if (logger.warningEnabled()) {
              logger.warning(
                LocalizedStrings.ProcessBatch_0_CAUGHT_EXCEPTION_PROCESSING_BATCH_REQUEST_1_CONTAINING_2_EVENTS,
                new Object[] {servConn.getName(), Integer.valueOf(batchId), Integer.valueOf(numberOfEvents)}, e);
            }
            throw e;
          }

          // Retrieve the key from the message parts
          keyPart = msg.getPart(partNumber + 4);
          try {
            key = keyPart.getStringOrObject();            
          }
          catch (Exception e) {
            if (logger.warningEnabled()) {
              logger.warning(
                LocalizedStrings.ProcessBatch_0_CAUGHT_EXCEPTION_PROCESSING_BATCH_REQUEST_1_CONTAINING_2_EVENTS,
                new Object[] {servConn.getName(), Integer.valueOf(batchId), Integer.valueOf(numberOfEvents)}, e);
            }
            throw e;
          }

          /*
           * CLIENT EXCEPTION HANDLING TESTING CODE String keySt = (String) key;
           * System.out.println("Processing new key: " + key); if
           * (keySt.startsWith("failure")) { throw new Exception(LocalizedStrings.ProcessBatch_THIS_EXCEPTION_REPRESENTS_A_FAILURE_ON_THE_SERVER.toLocalizedString()); }
           */

          // Retrieve the value from the message parts (do not deserialize it)
          valuePart = msg.getPart(partNumber + 5);
          // try {
          // logger.warning(getName() + ": Creating key " + key + " value " +
          // valuePart.getObject());
          // } catch (Exception e) {}

          if (logger.fineEnabled()) {
            logger.fine(servConn.getName()
                + ": Processing batch create request " + batchId + " on "
                + servConn.getSocketString() + " for region " + regionName
                + " key " + key + " value " + valuePart + " callbackArg "
                + callbackArg);
          }

          // Retrieve the callbackArg from the message parts if necessary
          int index = partNumber+6;
          callbackArgExistsPart = msg.getPart(index++);
          {
            byte[] partBytes = (byte[])callbackArgExistsPart.getObject();
            callbackArgExists = partBytes[0] == 0x01;
          }
          if (callbackArgExists) {
            extraParts++;
            callbackArgPart = msg.getPart(index++);
            try {
              callbackArg = callbackArgPart.getObject();
            }
            catch (Exception e) {
              if (logger.warningEnabled()) {
                logger.warning(
                  LocalizedStrings.ProcessBatch_0_CAUGHT_EXCEPTION_PROCESSING_BATCH_CREATE_REQUEST_1_FOR_2_EVENTS,
                  new Object[] {servConn.getName(), Integer.valueOf(batchId), Integer.valueOf(numberOfEvents)}, e);
              }
              throw e;
            }
          }
          
//          if (versioningEnabled) {
//            versionTimeStamp = msg.getPart(index++).getLong();
//            extraParts++;
//          }
          

          // Process the create request
          if (key == null || regionName == null) {
            StringId message = null;
            Object[] messageArgs = new Object[] { servConn.getName(), Integer.valueOf(batchId)};
            if (key == null) {
              message = LocalizedStrings.ProcessBatch_0_THE_INPUT_REGION_NAME_FOR_THE_BATCH_CREATE_REQUEST_1_IS_NULL;
            }
            if (regionName == null) {
              message = LocalizedStrings.ProcessBatch_0_THE_INPUT_REGION_NAME_FOR_THE_BATCH_CREATE_REQUEST_1_IS_NULL;
            }
            if (logger.warningEnabled()) {
              logger.warning(message, messageArgs);
            }
            throw new Exception(message.toLocalizedString(messageArgs));
          }
          else {
            LocalRegion region = (LocalRegion)crHelper.getRegion(regionName);            
            
            if (region == null) {
              String reason = LocalizedStrings.ProcessBatch_WAS_NOT_FOUND_DURING_BATCH_CREATE_REQUEST_0.toLocalizedString(new Object[] {regionName, Integer.valueOf(batchId)});
              throw new RegionDestroyedException(reason, regionName);
            }
            else {
              // In case of gemfirexd system, if check if table is non PK based
              // and accordingly regenerate primary key
              if (this.gfxdBulkDMLSubcommand != null) {
                key = this.gfxdBulkDMLSubcommand.regenerateKeyConditionally(
                    key, region);
                callbackArg = this.gfxdBulkDMLSubcommand
                    .regenerateCallbackConditionally(key, callbackArg, region);
              }
              clientEvent = new EntryEventImpl(eventId);
//              if (versioningEnabled) {
//                VersionTag<?> tag = VersionTag.create(region.getVersionMember());
//                tag.setIsGatewayTag(true);
//                tag.setVersionTimeStamp(versionTimeStamp);
//                tag.setDistributedSystemId(dsid);
//                clientEvent.setVersionTag(tag);
//              }

              try {
                byte[] value = valuePart.getSerializedForm();
                boolean isObject = valuePart.isObject();
                // bug #44941 - receiving a CREATE with a null creates an invalidated entry
                if (isObject && InternalDataSerializer.isSerializedNull(value)) {
                  value = null;
                }
                if (region.keyRequiresRegionContext()) {
                  ((KeyWithRegionContext)key).setRegionContext(region);
                }
                // [sumedh] This should be done on client while sending
                // since that is the WAN gateway
                AuthorizeRequest authzRequest = servConn.getAuthzRequest();
                if (authzRequest != null) {
                  PutOperationContext putContext = authzRequest.putAuthorize(
                      regionName, key, value, isObject, callbackArg);
                  value = putContext.getSerializedValue();
                  isObject = putContext.isObject();
                }
                if (this.gfxdBulkDMLSubcommand != null
                    /*PDXTypes is a meta region (not a user table) so there will not be callbacks for it*/
                    && !region.isPdxTypesRegion()) {
                  this.gfxdBulkDMLSubcommand.beforeOperationCallback(region,
                      actionType);
                }
                boolean result  = false;
                try {
                // Attempt to create the entry
                result = region.basicBridgeCreate(key, value,
                    isObject, callbackArg, servConn.getProxyID(), false,
                    clientEvent, false);

                // If the create fails (presumably because it already exists),
                // attempt to update the entry
                if (!result) {
                  result = region.basicBridgePut(key, value, null, isObject,
                      callbackArg, servConn.getProxyID(), false,
                      false /* in this case we know that its not a
                      SerializableDelta */, clientEvent);
                }
                }finally {
                  if(this.gfxdBulkDMLSubcommand != null && 
                      !region.isPdxTypesRegion()) {/*PDXTypes is a meta region (not a user table) so there will not be callbacks for it*/
                    this.gfxdBulkDMLSubcommand.afterOperationCallback(region, actionType);
                  }  
                }

                if (result || clientEvent.isConcurrencyConflict()) { // concurrency conflict problems are not sent back
                  servConn.setModificationInfo(true, regionName, key);
                }
                else {
                  // This exception will be logged in the catch block below
                  throw new Exception(LocalizedStrings.ProcessBatch_0_FAILED_TO_CREATE_OR_UPDATE_ENTRY_FOR_REGION_1_KEY_2_VALUE_3_CALLBACKARG_4
                      .toLocalizedString(new Object[] {servConn.getName(), regionName, key, valuePart, callbackArg}));
                }
              }
              catch (Exception e) {
                if (logger.warningEnabled()) {
                  logger.warning(
                    LocalizedStrings.ProcessBatch_0_CAUGHT_EXCEPTION_PROCESSING_BATCH_CREATE_REQUEST_1_FOR_2_EVENTS,
                    new Object[] {servConn.getName(), Integer.valueOf(batchId), Integer.valueOf(numberOfEvents)}, e);
                }
                throw e;
              }
            }
          }
          break;
        case GatewayEventImpl.UPDATE_ACTION: // Update
          // Retrieve the region name from the message parts
          regionNamePart = msg.getPart(partNumber + 2);
          regionName = regionNamePart.getString();

          // Retrieve the event id from the message parts
          // This was going to be used to determine possible
          // duplication of events, but it is unused now. In
          // fact the event id is overridden by the FROM_GATEWAY
          // token.
          eventIdPart = msg.getPart(partNumber + 3);
          // eventId = eventIdPart.getString();

          try {
            eventId = (EventID)eventIdPart.getObject();
          }
          catch (Exception e) {
            if (logger.warningEnabled()) {
              logger.warning(
                LocalizedStrings.ProcessBatch_0_CAUGHT_EXCEPTION_PROCESSING_BATCH_REQUEST_1_CONTAINING_2_EVENTS,
                new Object[] {servConn.getName(), Integer.valueOf(batchId), Integer.valueOf(numberOfEvents)}, e);
            }
            throw e;
          }

          // Retrieve the key from the message parts
          keyPart = msg.getPart(partNumber + 4);
          try {
            key = keyPart.getStringOrObject();
          }
          catch (Exception e) {
            if (logger.warningEnabled()) {
              logger.warning(
                LocalizedStrings.ProcessBatch_0_CAUGHT_EXCEPTION_PROCESSING_BATCH_UPDATE_REQUEST_1_CONTAINING_2_EVENTS,
                new Object[] {servConn.getName(), Integer.valueOf(batchId), Integer.valueOf(numberOfEvents)}, e);
            }
            throw e;
          }

          /*
           * CLIENT EXCEPTION HANDLING TESTING CODE keySt = (String) key;
           * System.out.println("Processing updated key: " + key); if
           * (keySt.startsWith("failure")) { throw new Exception(LocalizedStrings.ProcessBatch_THIS_EXCEPTION_REPRESENTS_A_FAILURE_ON_THE_SERVER.toLocalizedString()); }
           */

          // Retrieve the value from the message parts (do not deserialize it)
          valuePart = msg.getPart(partNumber + 5);
          // try {
          // logger.warning(getName() + ": Updating key " + key + " value " +
          // valuePart.getObject());
          // } catch (Exception e) {}

          // Retrieve the callbackArg from the message parts if necessary
          index = partNumber + 6;
          callbackArgExistsPart = msg.getPart(index++);
          {
            byte[] partBytes = (byte[])callbackArgExistsPart.getObject();
            callbackArgExists = partBytes[0] == 0x01;
          }
          if (callbackArgExists) {
            callbackArgPart = msg.getPart(index++);
            extraParts++;
            try {
              callbackArg = callbackArgPart.getObject();
            }
            catch (Exception e) {
              if (logger.warningEnabled()) {
                logger.warning(
                  LocalizedStrings.ProcessBatch_0_CAUGHT_EXCEPTION_PROCESSING_BATCH_UPDATE_REQUEST_1_CONTAINING_2_EVENTS,
                  new Object[] {servConn.getName(), Integer.valueOf(batchId), Integer.valueOf(numberOfEvents)}, e);
              }
              throw e;
            }
          }

//          if (versioningEnabled) {
//            versionTimeStamp = msg.getPart(index++).getLong();
//            extraParts++;
//          }

          if (logger.fineEnabled()) {
            logger.fine(servConn.getName()
                + ": Processing batch update request " + batchId + " on "
                + servConn.getSocketString() + " for region " + regionName
                + " key " + key + " value " + valuePart + " callbackArg "
                + callbackArg /*+ (versioningEnabled? " version timestamp " + versionTimeStamp : "")*/);
          }

          // Process the update request
          if (key == null || regionName == null) {
            StringId message = null;
            Object[] messageArgs = new Object[] {servConn.getName(), Integer.valueOf(batchId)};
            if (key == null) {
              message = LocalizedStrings.ProcessBatch_0_THE_INPUT_KEY_FOR_THE_BATCH_UPDATE_REQUEST_1_IS_NULL;
            }
            if (regionName == null) {
              message = LocalizedStrings.ProcessBatch_0_THE_INPUT_REGION_NAME_FOR_THE_BATCH_UPDATE_REQUEST_1_IS_NULL;
            }
            if (logger.warningEnabled()) {
              logger.warning(message, messageArgs);
            }
            throw new Exception(message.toLocalizedString(messageArgs));
          }
          else {
            LocalRegion region = (LocalRegion)crHelper.getRegion(regionName);
            if (region == null) {
              String reason = LocalizedStrings.ProcessBatch_WAS_NOT_FOUND_DURING_BATCH_CREATE_REQUEST_0.toLocalizedString(new Object[] {regionName, Integer.valueOf(batchId)});
              throw new RegionDestroyedException(reason, regionName);
            }
            else {
              clientEvent = new EntryEventImpl(eventId);
//              if (versioningEnabled) {
//                VersionTag<?> tag = VersionTag.create(region.getVersionMember());
//                tag.setIsGatewayTag(true);
//                tag.setVersionTimeStamp(versionTimeStamp);
//                tag.setDistributedSystemId(dsid);
//                clientEvent.setVersionTag(tag);
//              }
              try {
                byte[] value = valuePart.getSerializedForm();
                boolean isObject = valuePart.isObject();
                if (region.keyRequiresRegionContext()) {
                  ((KeyWithRegionContext)key).setRegionContext(region);
                }
                AuthorizeRequest authzRequest = servConn.getAuthzRequest();
                if (authzRequest != null) {
                  PutOperationContext putContext = authzRequest.putAuthorize(
                      regionName, key, value, isObject, callbackArg,
                      PutOperationContext.UPDATE);
                  value = putContext.getSerializedValue();
                  isObject = putContext.isObject();
                }
                if (this.gfxdBulkDMLSubcommand != null && 
                    !region.isPdxTypesRegion()) {/*PDXTypes is a meta region (not a user table) so there will not be callbacks for it*/
                  this.gfxdBulkDMLSubcommand.beforeOperationCallback(region,
                      actionType);
                }
                boolean result = false;
                try {
                  result = region.basicBridgePut(key, value, null, isObject,
                      callbackArg, servConn.getProxyID(), false,
                      servConn.isGemFireXDSystem(), clientEvent);
                } finally {
                  if (this.gfxdBulkDMLSubcommand != null &&
                      !region.isPdxTypesRegion()) {/*PDXTypes is a meta region (not a user table) so there will not be callbacks for it*/
                    this.gfxdBulkDMLSubcommand.afterOperationCallback(region,
                        actionType);
                  }
                }

                if (result || clientEvent.isConcurrencyConflict()) { // concurrency conflict problems are not sent back
                  servConn.setModificationInfo(true, regionName, key);
                }
                else {
                  final Object[] msgArgs = new Object[] {servConn.getName(), regionName, key, valuePart, callbackArg };
                  final StringId message = 
                    LocalizedStrings.ProcessBatch_0_FAILED_TO_UPDATE_ENTRY_FOR_REGION_1_KEY_2_VALUE_3_AND_CALLBACKARG_4;                
                  if (logger.infoEnabled()) {
                    logger.info(message, msgArgs);
                  }
                  throw new Exception(message.toLocalizedString(msgArgs));
                }
              }
              catch (CancelException e) {
                // FIXME better exception hierarchy would avoid this check
                if (servConn.getCachedRegionHelper().getCache().getCancelCriterion().cancelInProgress() != null) {
                  if (logger != null && logger.fineEnabled()) {
                    logger
                        .fine(servConn.getName()
                            + " ignoring message of type "
                            + MessageType.getString(msg.getMessageType())
                            + " from client "
                            + servConn.getProxyID()
                            + " because shutdown occurred during message processing.");
                  }
                  servConn.setFlagProcessMessagesAsFalse();
                }
                else {
                  throw e;
                }
                return;
              }
              catch (Exception e) {
                // Preserve the connection under all circumstances
                if (logger.warningEnabled()) {
                  logger.warning(
                    LocalizedStrings.ProcessBatch_0_CAUGHT_EXCEPTION_PROCESSING_BATCH_UPDATE_REQUEST_1_CONTAINING_2_EVENTS,
                    new Object[] {servConn.getName(), Integer.valueOf(batchId), Integer.valueOf(numberOfEvents)}, e);
                }
                throw e;
              }
            }
          }
          break;
        case GatewayEventImpl.DESTROY_ACTION: // Destroy
          // Retrieve the region name from the message parts
          regionNamePart = msg.getPart(partNumber + 2);
          regionName = regionNamePart.getString();

          // Retrieve the event id from the message parts
          // This was going to be used to determine possible
          // duplication of events, but it is unused now. In
          // fact the event id is overridden by the FROM_GATEWAY
          // token.
          eventIdPart = msg.getPart(partNumber + 3);
          // eventId = eventIdPart.getString();

          try {
            eventId = (EventID)eventIdPart.getObject();
          }
          catch (Exception e) {
            if (logger.warningEnabled()) {
              logger.warning(
                LocalizedStrings.ProcessBatch_0_CAUGHT_EXCEPTION_PROCESSING_BATCH_UPDATE_REQUEST_1_CONTAINING_2_EVENTS,
                new Object[] {servConn.getName(), Integer.valueOf(batchId), Integer.valueOf(numberOfEvents)}, e);            
            }
            throw e;
          }

          // Retrieve the key from the message parts
          keyPart = msg.getPart(partNumber + 4);
          try {
            key = keyPart.getStringOrObject();
          }
          catch (Exception e) {
            if (logger.warningEnabled()) {
              logger.warning(
                LocalizedStrings.ProcessBatch_0_CAUGHT_EXCEPTION_PROCESSING_BATCH_UPDATE_REQUEST_1_CONTAINING_2_EVENTS,
                new Object[] {servConn.getName(), Integer.valueOf(batchId), Integer.valueOf(numberOfEvents)}, e);
            }
            throw e;
          }

          // Retrieve the callbackArg from the message parts if necessary
          index = partNumber+5;
          callbackArgExistsPart = msg.getPart(index++);
          {
            byte[] partBytes = (byte[])callbackArgExistsPart.getObject();
            callbackArgExists = partBytes[0] == 0x01;
          }
          if (callbackArgExists) {
            callbackArgPart = msg.getPart(index++);
            extraParts++;
            try {
              callbackArg = callbackArgPart.getObject();
            }
            catch (Exception e) {
              if (logger.warningEnabled()) {
                logger.warning(
                  LocalizedStrings.ProcessBatch_0_CAUGHT_EXCEPTION_PROCESSING_BATCH_DESTROY_REQUEST_1_CONTAINING_2_EVENTS,
                  new Object[] {servConn.getName(), Integer.valueOf(batchId), Integer.valueOf(numberOfEvents)}, e);
              }
              throw e;
            }
          }

//          if (versioningEnabled) {
//            versionTimeStamp = msg.getPart(index++).getLong();
//            extraParts++;
//          }

          if (logger.fineEnabled()) {
            logger.fine(servConn.getName()
                + ": Processing batch destroy request " + batchId + " on "
                + servConn.getSocketString() + " for region " + regionName
                + " key " + key /*+ (versioningEnabled? " versionStamp=" + versionTimeStamp : "")*/);
          }

          // Process the destroy request
          if (key == null || regionName == null) {
            StringId message = null;
            if (key == null) {
              message = LocalizedStrings.ProcessBatch_0_THE_INPUT_KEY_FOR_THE_BATCH_DESTROY_REQUEST_1_IS_NULL;
            }
            if (regionName == null) {
              message = LocalizedStrings.ProcessBatch_0_THE_INPUT_REGION_NAME_FOR_THE_BATCH_DESTROY_REQUEST_1_IS_NULL; 
            }
            Object[] messageArgs = new Object[] {servConn.getName(), Integer.valueOf(batchId)};
            if (logger.warningEnabled()) {
              logger.warning(message, messageArgs);
            }
            throw new Exception(message.toLocalizedString(messageArgs));
          }
          else {
            LocalRegion region = (LocalRegion)crHelper.getRegion(regionName);
            if (region == null) {
              String reason = LocalizedStrings.ProcessBatch_WAS_NOT_FOUND_DURING_BATCH_CREATE_REQUEST_0.toLocalizedString(new Object[] {regionName, Integer.valueOf(batchId)});
              throw new RegionDestroyedException(reason, regionName);
            }
            else {
              clientEvent = new EntryEventImpl(eventId);
//              if (versioningEnabled) {
//                VersionTag<?> tag = VersionTag.create(region.getVersionMember());
//                tag.setIsGatewayTag(true);
//                tag.setVersionTimeStamp(versionTimeStamp);
//                tag.setDistributedSystemId(dsid);
//                clientEvent.setVersionTag(tag);
//              }
              // Destroy the entry
              if (region.keyRequiresRegionContext()) {
                ((KeyWithRegionContext)key).setRegionContext(region);
              }
              try {
                AuthorizeRequest authzRequest = servConn.getAuthzRequest();
                if (authzRequest != null) {
                  DestroyOperationContext destroyContext = authzRequest
                      .destroyAuthorize(regionName, key, callbackArg);
                  callbackArg = destroyContext.getCallbackArg();
                }
                if(this.gfxdBulkDMLSubcommand != null &&
                    /*PDXTypes is a meta region (not a user table) so there will not be callbacks for it*/
                    !region.isPdxTypesRegion()) {
                  this.gfxdBulkDMLSubcommand.beforeOperationCallback(region, actionType);
                }
                try {
                region.basicBridgeDestroy(key, callbackArg, servConn
                    .getProxyID(), false,
                    clientEvent);
                }finally {
                  if(this.gfxdBulkDMLSubcommand != null && 
                      /*PDXTypes is a meta region (not a user table) so there will not be callbacks for it*/
                      !region.isPdxTypesRegion()) {
                    this.gfxdBulkDMLSubcommand.afterOperationCallback(region, actionType);
                    
                  }
                }
                servConn.setModificationInfo(true, regionName, key);
              }
              catch (EntryNotFoundException e) {
                if (logger.infoEnabled()) {
                  logger.info(
                    LocalizedStrings.ProcessBatch_0_DURING_BATCH_DESTROY_NO_ENTRY_WAS_FOUND_FOR_KEY_1,
                    new Object[] {servConn.getName(), key});
                }
                // throw new Exception(e);
              }
            }
          }
          break;
        case GatewayEventImpl.BULK_DML_OP_ACTION:
          Part statementPart = msg.getPart(partNumber + 2);
          if (logger.fineEnabled()) {
            logger.convertToLogWriter().fine(
                "GfxdBulkDMLOp: Part =" + statementPart+"; event ID ="+eventId);
          }
          //this.gfxdBulkDMLSubcommand.processBulkDML(statementPart);
          break;
        default:
          if (logger.severeEnabled()) {
            logger.severe(
              LocalizedStrings.Processbatch_0_UNKNOWN_ACTION_TYPE_1_FOR_BATCH_FROM_2, 
              new Object[] {servConn.getName(), Integer.valueOf(actionType), servConn.getSocketString()});
          }
        }
      }
      catch (CancelException e) {
        if (logger != null && logger.fineEnabled()) {
          logger.fine(servConn.getName() + " ignoring message of type "
              + MessageType.getString(msg.getMessageType()) + " from client "
              + servConn.getProxyID()
              + " because shutdown occurred during message processing.");
        }
        servConn.setFlagProcessMessagesAsFalse();
        return;
      }
      catch (Exception e) {
        // If an interrupted exception is thrown , rethrow it
        checkForInterrupt(servConn, e);

        // logger.warning("Caught exception for batch " + batchId + " containing
        // " + numberOfEvents + " events (" + msg.getPayloadLength() + " bytes)
        // with " + (earlyAck ? "early" : "normal") + " acknowledgement on " +
        // getSocketString());
        // If the response has not already been written (it is not
        // early ack mode), increment the latest batch id replied,
        // write the batch exception to the caller and break
        if (!wroteResponse) {
          // Increment the batch id unless the received batch id is -1 (a
          // failover batch)
          if (batchId != -1) {
            servConn.incrementLatestBatchIdReplied(batchId);
          }
          writeBatchException(msg, e, i, servConn);
          servConn.setAsTrue(RESPONDED);
          wroteResponse = true;
          break;
        }
        else {
          // If it is early ack mode, attempt to process the remaining messages
          // in the batch.
          // This could be problematic depending on where the exception
          // occurred.
          return;
        }
      }
      finally {
        // Increment the partNumber
        if (actionType == GatewayEventImpl.CREATE_ACTION ||
            actionType == GatewayEventImpl.UPDATE_ACTION) {
          partNumber += 7 + extraParts;
        }
        else if (actionType == GatewayEventImpl.DESTROY_ACTION) {
          partNumber += 6 + extraParts;
        } else if(actionType == GatewayEventImpl.BULK_DML_OP_ACTION) {
          partNumber += 3;
        }
          
      }
    }

    {
      long oldStart = start;
      start = DistributionStats.getStatTime();
      stats.incProcessBatchTime(start - oldStart);
    }

    if (!wroteResponse) {
      // Increment the batch id unless the received batch id is -1 (a failover
      // batch)
      if (batchId != -1) {
        servConn.incrementLatestBatchIdReplied(batchId);
      }
      writeReply(msg, servConn);
      servConn.setAsTrue(RESPONDED);
      stats.incWriteProcessBatchResponseTime(DistributionStats.getStatTime()
          - start);
      if (logger.fineEnabled()) {
        logger.fine(servConn.getName()
            + ": Sent process batch normal response for batch " + batchId
            + " containing " + numberOfEvents + " events ("
            + msg.getPayloadLength() + " bytes) with "
            + (earlyAck ? "early" : "normal") + " acknowledgement on "
            + servConn.getSocketString());
      }
      // logger.warning("Sent process batch normal response for batch " +
      // batchId + " containing " + numberOfEvents + " events (" +
      // msg.getPayloadLength() + " bytes) with " + (earlyAck ? "early" :
      // "normal") + " acknowledgement on " + getSocketString());
    }

  }

  private static void writeBatchException(Message origMsg, Throwable e,
      int index, ServerConnection servConn) throws IOException {
    LogWriterI18n logger = servConn.getLogger();
    Exception be = new BatchException(e, index);
    Message errorMsg = servConn.getErrorResponseMessage();
    errorMsg.setMessageType(MessageType.EXCEPTION);
    errorMsg.setNumberOfParts(2);
    errorMsg.setTransactionId(origMsg.getTransactionId());
    errorMsg.addObjPart(be);
    errorMsg.addStringPart(be.toString());
    errorMsg.send(servConn);//TODO:hitesh is this user initiative
    if (logger.warningEnabled()) {
      // Log a warning if necessary. Only log RegionDestroyedExceptions once per region.
      boolean logWarning = true;
      if (e instanceof RegionDestroyedException) {
        RegionDestroyedException rde = (RegionDestroyedException) e;
        synchronized (notFoundRegionsSync) {
          if (notFoundRegions.contains(rde.getRegionFullPath())) {
            logWarning = false;
          } else {
            notFoundRegions.add(rde.getRegionFullPath());
          }
        }
      }
      if (logWarning) {
        logger.warning(
          LocalizedStrings.ProcessBatch_0_WROTE_BATCH_EXCEPTION, servConn.getName(), be);
      }
    }
  }

}
