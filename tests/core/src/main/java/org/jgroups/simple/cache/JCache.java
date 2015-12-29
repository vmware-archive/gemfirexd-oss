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

import java.util.Enumeration;
import java.util.Hashtable;

public class JCache
{
    private static CacheAttributes cattr;
    
    public static void init(CacheAttributes attr)
    {
        cattr = attr;
    }
    
    public static void open(String config)
    {
        cattr = new CacheAttributes();
        // not implemented
    }
    
    public static void close()
    {
        // not implemented
    }
    
    public static void flush(int seconds)
    {
        // not implemented
    }

    public static void flushMemory(int seconds)
    {
        // not implemented
    }
    
    public static float getVersion(int seconds)
    {
        return (float)0.1;
    }
    
    public static boolean isDistributed()
    {
        return true;
    }
    
    public static Enumeration listCacheObjects()
    {
        Hashtable h = CacheAccess.getDefault().getCombinedView();
        return h.elements();
    }
    
    public static Enumeration listCacheObjects(String region)
    {
        return listCacheObjects();
    }
    
    public static CacheAttributes getAttributes()
    {
        return cattr;
    }
    
    
}
