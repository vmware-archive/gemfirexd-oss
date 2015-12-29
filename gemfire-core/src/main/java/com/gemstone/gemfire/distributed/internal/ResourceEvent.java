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
package com.gemstone.gemfire.distributed.internal;

/**
 * GemFire Resource Events
 * @author rishim
 *
 */
public enum ResourceEvent {
    CACHE_CREATE, 
    REGION_CREATE, 
    DISKSTORE_CREATE, 
    GATEWAYSENDER_CREATE, 
    LOCKSERVICE_CREATE, 
    CACHE_REMOVE, 
    REGION_REMOVE, 
    DISKSTORE_REMOVE, 
    GATEWAYSENDER_REMOVE, 
    LOCKSERVICE_REMOVE,
    MANAGER_CREATE,
    MANAGER_START,
    MANAGER_STOP,
    LOCATOR_START,
    ASYNCEVENTQUEUE_CREATE,
    SYSTEM_ALERT,
    CACHE_SERVER_START,
    CACHE_SERVER_STOP,
    GATEWAYRECEIVER_START,
    GATEWAYRECEIVER_STOP,
    GATEWAYRECEIVER_CREATE,
    GATEWAYSENDER_START, 
    GATEWAYSENDER_STOP, 
    GATEWAYSENDER_PAUSE, 
    GATEWAYSENDER_RESUME 
    
}

