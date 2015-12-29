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
package com.gemstone.gemfire.management.internal.beans;



/**
 * A resource wide notification to be emitted when a resource is created or
 * destroyed.
 * 
 * @author rishim
 * 
 */
public class ResourceNotification {

  /**
   * Notification type which indicates that a region has been created in the
   * cache <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.cache.region.created</CODE>.
   */
  public static final String REGION_CREATED = "gemfire.distributedsystem.cache.region.created";

  /**
   * Notification type which indicates that a region has been closed/destroyed
   * in the cache <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.cache.region.closed</CODE>.
   */
  public static final String REGION_CLOSED = "gemfire.distributedsystem.cache.region.closed";

  /**
   * Notification type which indicates that a disk store has been created <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.cache.disk.created</CODE>.
   */
  public static final String DISK_STORE_CREATED = "gemfire.distributedsystem.cache.disk.created";

  /**
   * Notification type which indicates that a disk store has been closed. <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.cache.disk.closed</CODE>.
   */
  public static final String DISK_STORE_CLOSED = "gemfire.distributedsystem.cache.disk.closed";

  /**
   * Notification type which indicates that a lock service has been crated. <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.cache.lockservice.created</CODE>.
   */
  public static final String LOCK_SERVICE_CREATED = "gemfire.distributedsystem.cache.lockservice.created";

  /**
   * Notification type which indicates that a lock service has been closed. <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.cache.lockservice.closed</CODE>.
   */
  public static final String LOCK_SERVICE_CLOSED = "gemfire.distributedsystem.cache.lockservice.closed";
  
  
  // Distributed System level Notifications
  
  /**
   * Notification type which indicates that a member has been added to the system. <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.cache.created</CODE>.
   */
  public static final String CACHE_MEMBER_JOINED = "gemfire.distributedsystem.cache.member.joined";
  
  
  /**
   * Notification type which indicates that a member has departed from  the system. <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.cache.created</CODE>.
   */
  public static final String CACHE_MEMBER_DEPARTED = "gemfire.distributedsystem.cache.member.departed";
  
  /**
   * Notification type which indicates that a member is supected. <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.cache.created</CODE>.
   */
  public static final String CACHE_MEMBER_SUSPECT = "gemfire.distributedsystem.cache.member.suspect";
  
  /**
   * Notification type which indicates that a client has joined <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.cacheserver.client.joined</CODE>.
   */
  public static final String CLIENT_JOINED = "gemfire.distributedsystem.cacheserver.client.joined";
  
  /**
   * Notification type which indicates that a client has left <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.cacheserver.client.left</CODE>.
   */
  public static final String CLIENT_LEFT = "gemfire.distributedsystem.cacheserver.client.left";
  
  
  /**
   * Notification type which indicates that a client has crashed <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.cacheserver.client.crashed</CODE>.
   */
  public static final String CLIENT_CRASHED = "gemfire.distributedsystem.cacheserver.client.crashed";
  
  
  /**
   * Notification type which indicates that a gateway receiver is created <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.gateway.receiver.created</CODE>.
   */
  public static final String GATEWAY_RECEIVER_CREATED = "gemfire.distributedsystem.gateway.receiver.created";
  
  
  /**
   * Notification type which indicates that a gateway sender is created <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.gateway.sender.created</CODE>.
   */
  public static final String GATEWAY_SENDER_CREATED = "gemfire.distributedsystem.gateway.sender.created";
  
  /**
   * Notification type which indicates that a gateway sender is started <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.gateway.sender.started</CODE>.
   */
  public static final String GATEWAY_SENDER_STARTED = "gemfire.distributedsystem.gateway.sender.started";
  
  
  /**
   * Notification type which indicates that a gateway sender is stopped <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.gateway.sender.stopped</CODE>.
   */
  public static final String GATEWAY_SENDER_STOPPED = "gemfire.distributedsystem.gateway.sender.stopped";
  
  /**
   * Notification type which indicates that a gateway sender is paused <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.gateway.sender.paused</CODE>.
   */
  public static final String GATEWAY_SENDER_PAUSED = "gemfire.distributedsystem.gateway.sender.paused";
  
  
  /**
   * Notification type which indicates that a gateway sender is resumed <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.gateway.sender.resumed</CODE>.
   */
  public static final String GATEWAY_SENDER_RESUMED = "gemfire.distributedsystem.gateway.sender.resumed";
  
  
  /**
   * Notification type which indicates that an async queue is created <BR>
   * The value of this type string is
   * <CODE>ggemfire.distributedsystem.asycn.event.queue.created</CODE>.
   */
  public static final String ASYNC_EVENT_QUEUE_CREATED = "gemfire.distributedsystem.asycn.event.queue.created";
  
  /**
   * Notification type which indicates that an async queue is created <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.asycn.event.queue.created</CODE>.
   */
  public static final String SYSTEM_ALERT = "system.alert";
  
  /**
   * Notification type which indicates that cache server is started <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.cache.server.started</CODE>.
   */
  public static final String CACHE_SERVER_STARTED = "gemfire.distributedsystem.cache.server.started";

  /**
   * Notification type which indicates that cache server is stopped <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.cache.server.stopped</CODE>.
   */
  public static final String CACHE_SERVER_STOPPED = "gemfire.distributedsystem.cache.server.stopped";  

  /**
   * Notification type which indicates that a gateway receiver is started <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.gateway.receiver.started</CODE>.
   */
  public static final String GATEWAY_RECEIVER_STARTED = "gemfire.distributedsystem.gateway.receiver.started";

  /**
   * Notification type which indicates that a gateway receiver is stopped <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.gateway.receiver.stopped</CODE>.
   */
  public static final String GATEWAY_RECEIVER_STOPPED = "gemfire.distributedsystem.gateway.receiver.stopped";
  
  
  /**
   * Notification type which indicates that locator is started <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.locator.started</CODE>.
   */
  public static final String LOCATOR_STARTED = "gemfire.distributedsystem.locator.started";

  
  public static final String REGION_CREATED_PREFIX= "Region Created With Name ";
  public static final String REGION_CLOSED_PREFIX= "Region Destroyed/Closed With Name ";
  
  public static final String DISK_STORE_CREATED_PREFIX = "DiskStore Created With Name ";
  public static final String DISK_STORE_CLOSED_PREFIX= "DiskStore Destroyed/Closed With Name ";
  
  public static final String LOCK_SERVICE_CREATED_PREFIX= "LockService Created With Name ";
  public static final String LOCK_SERVICE_CLOSED_PREFIX= "Lockservice closed With Name ";
  
  public static final String CACHE_MEMBER_DEPARTED_PREFIX= "Member Departed ";
  public static final String CACHE_MEMBER_JOINED_PREFIX= "Member Joined ";
  public static final String CACHE_MEMBER_SUSPECT_PREFIX= "Member Suspected ";
  
  public static final String GATEWAY_SENDER_CREATED_PREFIX= "GatewaySender Created in the VM ";
  public static final String GATEWAY_SENDER_STARTED_PREFIX= "GatewaySender Started in the VM ";
  public static final String GATEWAY_SENDER_STOPPED_PREFIX= "GatewaySender Stopped in the VM ";
  public static final String GATEWAY_SENDER_PAUSED_PREFIX= "GatewaySender Paused in the VM ";
  public static final String GATEWAY_SENDER_RESUMED_PREFIX= "GatewaySender Resumed in the VM ";
  
  public static final String GATEWAY_RECEIVER_CREATED_PREFIX= "GatewayReceiver Created in the VM ";
  public static final String GATEWAY_RECEIVER_STARTED_PREFIX= "GatewayReceiver Started in the VM ";
  public static final String GATEWAY_RECEIVER_STOPPED_PREFIX= "GatewayReceiver Stopped in the VM ";
  
  public static final String ASYNC_EVENT_QUEUE_CREATED_PREFIX= "Async Event Queue is Created  in the VM ";
  
  public static final String CACHE_SERVER_STARTED_PREFIX= "Cache Server is Started in the VM ";
  public static final String CACHE_SERVER_STOPPED_PREFIX= "Cache Server is stopped in the VM ";
  
  public static final String CLIENT_JOINED_PREFIX= "Client joined with Id ";
  public static final String CLIENT_CRASHED_PREFIX= "Client crashed with Id ";
  public static final String CLIENT_LEFT_PREFIX= "Client left with Id ";
  
  public static final String LOCATOR_STARTED_PREFIX= "Locator is Started in the VM ";

}
