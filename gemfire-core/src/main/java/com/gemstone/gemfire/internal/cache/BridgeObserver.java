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
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;

/**
 * This interface is used by testing/debugging code to be notified of different
 * events. See the documentation for class BridgeObserverHolder for details.
 * 
 * @author Yogesh Mahajan
 * @since 5.1
 *  
 */
public interface BridgeObserver
{
  /**
   * This callback is called when now primary Ep is identified.
   */
  public void afterPrimaryIdentificationFromBackup(ServerLocation location);

  /**
   * This callback is called just before interest registartion
   */
  public void beforeInterestRegistration();

  /**
   * This callback is called just after interest registartion
   */
  public void afterInterestRegistration();

  /**
   * This callback is called just before primary identification
   */
  public void beforePrimaryIdentificationFromBackup();

  /**
   * This callback is called just before Interest Recovery by DSM thread happens
   */
  public void beforeInterestRecovery();
  
  /**
   * Invoked by CacheClientUpdater just before invoking endpointDied for
   * fail over
   * @param location ServerLocation which has failed
   */
  public void beforeFailoverByCacheClientUpdater(ServerLocation location);
  /**
   * Invoked before sending an instantiator message to server
   * 
   * @param eventId
   */
  public void beforeSendingToServer(EventID eventId);
  /**
   * Invoked after sending an instantiator message to server 
   * 
   * @param eventId
   */
  public void afterReceivingFromServer(EventID eventId);

  /**
   * This callback is called just before sending client ack to the primary servrer.
   */
   public void beforeSendingClientAck();  

   /**
    * Invoked after Message is created
    *
    * @param msg
    */
   public void afterMessageCreation(Message msg);
   
   /**
    * Invoked after Queue Destroy Message has been sent
    */
   public void afterQueueDestroyMessage();
   
   /**
    * Invoked after a primary is recovered from a backup or new connection. 
    */
   public void afterPrimaryRecovered(ServerLocation location);
   
}
