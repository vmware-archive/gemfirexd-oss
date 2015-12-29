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

//import java.util.Hashtable;
//import java.util.Enumeration;

public class CacheEntry implements java.io.Serializable
{
    private Object name;
    private Object value;
    private Attributes attr;
    private boolean valid;
    
    protected CacheEntry(Object name, Attributes attr)
    {
        this.name = name;
        this.attr = attr;
        valid = false;
    }
    
    protected CacheEntry(Object name, Attributes attr, Object value)
    {
        this.name = name;
        this.attr = attr;
        this.value = value;
        valid = (value != null);
    }
    
    protected void invalidate()
    {
        valid = false;
    }
    
    protected boolean isValid()
    {
        if (value == null) valid = false;
        // timeout check here
        return valid;
    }
    
    protected void setAttributes(Attributes a)
    {
        attr = a;
    }
    
    protected Attributes getAttributes()
    {
        return attr;
    }
    
    protected Object getName()
    {
        return name;
    }
    
    protected Object getValue()
    {
        if (!valid) return null;
        else return value;
    }
    
    protected void setValue(Object value)
    {
        this.value = value;
        if (value != null) {
            attr.valueUpdated();
            valid = true;
        }
    }
    
    protected CacheObjectInfo getInfo()
    {
        return new CacheObjectInfo(
                "default",
                name,
                "memory",
                0,
                0,
                ""+getExpirationTime());
    }
    
    protected long getExpirationTime()
    {
        return 0;
    }
}
    

