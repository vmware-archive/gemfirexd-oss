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
package org.jgroups.simple.cache;

public class CacheEvent
{
    public static final int OBJECT_INVALIDATED = 0x1;
    public static final int OBJECT_UPDATED = 0x2;
    public static final int OBJECT_DESTROYED = 0x3;
    
//    private static int LAST_ID = 0x3;
    
    private int reason;
    
    public CacheEvent(int reason)
    {
        this.reason = reason;
    }
    
    public int getId()
    {
        return reason;
    }
}
