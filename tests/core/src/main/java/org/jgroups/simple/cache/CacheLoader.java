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

public abstract class CacheLoader
        implements java.io.Serializable  // needed for now because it's distributed with cache entry
{
    
    abstract public Object load(Object handle, Object args) throws JCacheException;
    
    public void setAttributes(Object handle, Attributes attr) {
        CacheEntry ce = (CacheEntry)handle;
        ce.setAttributes(attr);
    }
    
    public Object getName(Object handle) {
        CacheEntry ce = (CacheEntry)handle;
        return ce.getName();
    }
    
    public String getRegion(Object handle) {
        return "Default";
    }
    
    public Object netSearch(Object handle, int timeout) throws JCacheException
    {
        throw new ObjectNotFoundException();
    }
    
    public void log(String msg) {
        System.out.println(msg);
    }
    
    
}
