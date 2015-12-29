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

package com.gemstone.gemfire.internal.cache.tier.sockets.command;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.operations.GetOperationContext;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.tier.CachedRegionHelper;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.BaseCommand;
import com.gemstone.gemfire.internal.cache.tier.sockets.ChunkedMessage;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.ObjectPartList;
import com.gemstone.gemfire.internal.cache.tier.sockets.Part;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection;
import com.gemstone.gemfire.internal.cache.tier.sockets.VersionedObjectList;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.security.AuthorizeRequest;
import com.gemstone.gemfire.internal.security.AuthorizeRequestPP;
import com.gemstone.gemfire.internal.shared.Version;
import com.gemstone.gemfire.security.NotAuthorizedException;

import java.io.IOException  ;
import java.util.Iterator;
import java.util.Set;

public class GetAll70 extends BaseCommand {

  private final static GetAll70 singleton = new GetAll70();

  public static Command getCommand() {
    return singleton;
  }

  /**
   * client wants values to be serialized as byte arrays, not objects
   */
  private boolean requestSerializedValues;

  protected GetAll70() {
  }

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start)
          throws IOException, InterruptedException {
    Part regionNamePart = null, keysPart = null;
    String regionName = null;
    Object[] keys = null;
    CachedRegionHelper crHelper = servConn.getCachedRegionHelper();
    servConn.setAsTrue(REQUIRES_RESPONSE);
    servConn.setAsTrue(REQUIRES_CHUNKED_RESPONSE);
    int partIdx = 0;

    // Retrieve the region name from the message parts
    regionNamePart = msg.getPart(partIdx++);
    regionName = regionNamePart.getString();

    // Retrieve the keys array from the message parts
    keysPart = msg.getPart(partIdx++);
    try {
      keys = (Object[]) keysPart.getObject();
    } catch (Exception e) {
      writeChunkedException(msg, e, false, servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    }
    this.requestSerializedValues = msg.getPart(partIdx++).getInt() == 1;

    if (logger.fineEnabled()) {
      StringBuffer buffer = new StringBuffer();
      buffer
              .append(servConn.getName())
              .append(": Received getAll request (")
              .append(msg.getPayloadLength())
              .append(" bytes) from ")
              .append(servConn.getSocketString())
              .append(" for region ")
              .append(regionName)
              .append(" keys ");
      if (keys != null) {
        for (int i = 0; i < keys.length; i++) {
          buffer.append(keys[i]).append(" ");
        }
      } else {
        buffer.append("NULL");
      }
      logger.fine(buffer.toString());
    }

    // Process the getAll request
    if (regionName == null) {
      String message = null;
      //      if (regionName == null) (can only be null)
      {
        message = LocalizedStrings.GetAll_THE_INPUT_REGION_NAME_FOR_THE_GETALL_REQUEST_IS_NULL.toLocalizedString();
      }
      logger.warning(LocalizedStrings.TWO_ARG_COLON, new Object[]{servConn.getName(), message});
      writeChunkedErrorResponse(msg, MessageType.GET_ALL_DATA_ERROR, message,
              servConn);
      servConn.setAsTrue(RESPONDED);
    } else {
      LocalRegion region = (LocalRegion) crHelper.getRegion(regionName);
      if (region == null) {
        String reason = " was not found during getAll request";
        writeRegionDestroyedEx(msg, regionName, reason, servConn);
        servConn.setAsTrue(RESPONDED);
      } else {
        // Send header
        ChunkedMessage chunkedResponseMsg = servConn.getChunkedResponseMessage();
        chunkedResponseMsg.setMessageType(MessageType.RESPONSE);
        chunkedResponseMsg.setTransactionId(msg.getTransactionId());
        chunkedResponseMsg.sendHeader();

        // Send chunk response
        try {
          fillAndSendGetAllResponseChunks(region, regionName, keys, servConn);
          servConn.setAsTrue(RESPONDED);
        } catch (Exception e) {
          // If an interrupted exception is thrown , rethrow it
          checkForInterrupt(servConn, e);

          // Otherwise, write an exception message and continue
          writeChunkedException(msg, e, false, servConn);
          servConn.setAsTrue(RESPONDED);
          return;
        }
      }
    }
  }

  private void fillAndSendGetAllResponseChunks(Region region,
                                               String regionName, Object[] keys, ServerConnection servConn)
          throws IOException {

    // Interpret null keys object as a request to get all key,value entry pairs
    // of the region; otherwise iterate each key and perform the get behavior.
    Iterator allKeysIter;
    int numKeys;
    if (keys != null) {
      allKeysIter = null;
      numKeys = keys.length;
    } else {
      Set allKeys = region.keySet();
      allKeysIter = allKeys.iterator();
      numKeys = allKeys.size();
    }
    VersionedObjectList values = new VersionedObjectList(maximumChunkSize, keys == null,
            region.getAttributes().getConcurrencyChecksEnabled(), this.requestSerializedValues);
    AuthorizeRequest authzRequest = servConn.getAuthzRequest();
    AuthorizeRequestPP postAuthzRequest = servConn.getPostAuthzRequest();
    Get70 request = (Get70) Get70.getCommand();
    for (int i = 0; i < numKeys; i++) {
      Object key;
      boolean keyNotPresent = false;
      if (keys != null) {
        key = keys[i];
      } else {
        key = allKeysIter.next();
      }
      if (logger.fineEnabled()) {
        logger.fine(servConn.getName() + ": Getting value for key=" + key);
      }
      // Determine if the user authorized to get this key
      GetOperationContext getContext = null;
      if (authzRequest != null) {
        try {
          getContext = authzRequest.getAuthorize(regionName, key, null);
          if (logger.fineEnabled()) {
            logger.fine(servConn.getName() + ": Passed GET pre-authorization for key=" + key);
          }
        } catch (NotAuthorizedException ex) {
          logger.warning(LocalizedStrings.GetAll_0_CAUGHT_THE_FOLLOWING_EXCEPTION_ATTEMPTING_TO_GET_VALUE_FOR_KEY_1,
                  new Object[]{servConn.getName(), key}, ex);
          values.addExceptionPart(key, ex);
          continue;
        }
      }

      // Get the value and update the statistics. Do not deserialize
      // the value if it is a byte[].
      // Getting a value in serialized form is pretty nasty. I split this out
      // so the logic can be re-used by the CacheClientProxy.
      Get70.Entry entry = request.getValueAndIsObject(region, key,
              null, logger, servConn);
      keyNotPresent = entry.keyNotPresent;
      if (logger.fineEnabled()) {
        logger.fine("retrieved key=" + key + " " + entry);
      }

      if (postAuthzRequest != null) {
        try {
          getContext = postAuthzRequest.getAuthorize(regionName, key, entry.value,
                  entry.isObject, getContext);
          byte[] serializedValue = getContext.getSerializedValue();
          if (serializedValue == null) {
            entry.value = getContext.getObject();
          } else {
            entry.value = serializedValue;
          }
          entry.isObject = getContext.isObject();
          if (logger.fineEnabled()) {
            logger.fine(servConn.getName() + ": Passed GET post-authorization for key=" + key + ": " + entry.value);
          }
        } catch (NotAuthorizedException ex) {
          logger.warning(LocalizedStrings.GetAll_0_CAUGHT_THE_FOLLOWING_EXCEPTION_ATTEMPTING_TO_GET_VALUE_FOR_KEY_1,
                  new Object[]{servConn.getName(), key}, ex);
          values.addExceptionPart(key, ex);
          continue;
        }
      }


      // Add the entry to the list that will be returned to the client

      if (keyNotPresent) {
        values.addObjectPartForAbsentKey(key, entry.value, entry.versionTag);
      } else {
        values.addObjectPart(key, entry.value, entry.isObject, entry.versionTag);
      }

      // Send the intermediate chunk if necessary
      if (values.size() == maximumChunkSize) {
        // Send the chunk and clear the list
        values.setKeys(null);
        sendGetAllResponseChunk(region, values, false, servConn);
        values.clear();
      }
    }

    // Send the last chunk even if the list is of zero size.
    if (Version.GFE_701.compareTo(servConn.getClientVersion()) <= 0) {
      // 7.0.1 clients do not expect the keys in the response
      values.setKeys(null);
    }
    sendGetAllResponseChunk(region, values, true, servConn);
    servConn.setAsTrue(RESPONDED);
  }


  private static void sendGetAllResponseChunk(Region region, ObjectPartList list,
                                              boolean lastChunk, ServerConnection servConn) throws IOException {
    LogWriterI18n logger = servConn.getLogger();
    ChunkedMessage chunkedResponseMsg = servConn.getChunkedResponseMessage();
    chunkedResponseMsg.setNumberOfParts(1);
    chunkedResponseMsg.setLastChunk(lastChunk);
    chunkedResponseMsg.addObjPart(list, zipValues);

    if (logger.fineEnabled()) {
      String str = servConn.getName() + ": Sending" +
              (lastChunk ? " last " : " ") + "getAll response chunk for region=" +
              region.getFullPath() + (logger.finerEnabled()? " values=" + list + " chunk=<" +
              chunkedResponseMsg + ">" : "");
      logger.fine(str);
    }

    chunkedResponseMsg.sendChunk(servConn);
  }

}
