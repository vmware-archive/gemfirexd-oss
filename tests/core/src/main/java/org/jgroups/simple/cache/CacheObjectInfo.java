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

public class CacheObjectInfo
{
    public String region;
    public String type;
    public Object name;
    public int refcount;
    public int accesses;
    public String expire;
    
    public CacheObjectInfo(String region, Object name, String type, int refCount,
                                int accesses, String expire)
    {
        this.region = region;
        this.name = name;
        this.type = type;
        this.refcount = refCount;
        this.accesses = accesses;
        this.expire = expire;
    }
    
    public String toString()
    {
        return "" + region + "." + name
             + "  type=" + type + "  ref=" + refcount
             + "  accesses=" + accesses + " expire=" + expire;
    }
}

