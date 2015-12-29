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

public class Attributes implements Cloneable, java.io.Serializable
{
    public static final long DISTRIBUTE = 0x1;
    public static final long NOFLUSH = 0x2;
    public static final long REPLY = 0x4;
    public static final long SYNCHRONIZE = 0x8;
    public static final long SPOOL = 0x10;
    public static final long ORIGINAL = 0x20;
    
    private long flags;
    private long timeToLive;
    private long defaultTimeToLive;
    private long idleTime;
    private int  objSize;
    private long lastUpdateTime;
    private CacheLoader loader;
    private CacheEventListener listener;
    private long version;
    
    public Attributes()
    {
        lastUpdateTime = System.currentTimeMillis();
    }
    
    public long getFlags()
    {
        return flags;
    }
    
    public void setFlags(long flags)
    {
        this.flags = flags;
        checkLoader();
        checkListener();
    }
    
    public void setVersion(long version) {
        this.version = version;
    }
    
    public void setTimeToLive(long timeToLive) {
        this.timeToLive = timeToLive;
        lastUpdateTime = System.currentTimeMillis();
    }
    
    public void setDefaultTimeToLive(long ttl) {
        this.defaultTimeToLive = ttl;
    }
    
    public void setIdleTime(long idle) {
        this.idleTime = idle;
        lastUpdateTime = System.currentTimeMillis();
    }
    
    public void setListener(CacheEventListener l) {
        this.listener = l;
        checkListener();
    }
    
    public void setSize(int size) {
        this.objSize = size;
    }
    
    public int getSize() {
        return objSize;
    }
    
    public boolean isSet(long theFlags) {
        return (this.flags & theFlags) != 0;
    }
    
    public long getCreateTime() {
        return lastUpdateTime;
    }
    
    public CacheLoader getLoader() {
        return loader;
    }
    
    protected CacheEventListener getListener() {
        return listener;
    }
    
    public void setLoader(CacheLoader l) {
        loader = l;
        checkLoader();
    }
    
    public long getVersion() {
        return version;
    }
    
    public long getIdleTime() {
        return idleTime;
    }
    
    public long getTimeToLive() {
        return timeToLive;
    }
    
    protected void setLastUpdateTime(long t) {
        lastUpdateTime = t;
    }
    
    public long timeToSeconds(int days, int hours, int minutes, int seconds)
        throws InvalidArgumentException
    {
        long l;
        l = (long)seconds  * 1000;
        l += (long)minutes * 60000;
        l += (long)hours * 3600000;
        l += (long)days * 86400000;
        return l;
    }
    
    protected final boolean isDistributed()
    {
        return (flags & DISTRIBUTE) == DISTRIBUTE;
    }
    
    protected final void valueUpdated()
    {
        lastUpdateTime = System.currentTimeMillis();
    }
    
    protected void resetAttributes(Attributes other)
    {
        // not implemented
    }
    
    public final Object clone()
    {
        Attributes attr = new Attributes();
        attr.setFlags(flags);
        attr.setTimeToLive(timeToLive);
        attr.setDefaultTimeToLive(defaultTimeToLive);
        attr.setIdleTime(idleTime);
        attr.setSize(objSize);
        attr.setLastUpdateTime(lastUpdateTime);
        attr.setLoader(loader);
        attr.setListener(listener);
        attr.setVersion(version);
        return attr;
    }
    
    public String toString()
    {
        return "flags: " + flagsToString()
          + "\ntimeToLive: " + timeToLive + "  idleTime: " + idleTime
          + "\nsize: " + objSize + "  age: " + (System.currentTimeMillis()-lastUpdateTime);
    }
    
    private String flagsToString()
    {
        StringBuffer b = new StringBuffer();
        if ((flags & DISTRIBUTE) != 0) b.append("DISTR ");
        if ((flags & NOFLUSH) != 0) b.append("NOFLUSH ");
        if ((flags & SYNCHRONIZE) != 0) b.append("SYNCH ");
        if ((flags & REPLY) != 0) b.append("REPLY ");
        if ((flags & ORIGINAL) != 0) b.append("ORIG ");
        if ((flags & SPOOL) != 0) b.append("SPOOL ");
        return b.toString();
    }

    private void checkLoader()
    {
        /* no longer needed
        if ( loader != null && isDistributed() && !(loader instanceof java.io.Serializable) ) {
            System.out.println("Warning: non-serializable loader in distributed object");
            flags ^= DISTRIBUTE;
        }
        */
    }

    private void checkListener()
    {
        /* no longer needed
        if ( listener != null && isDistributed() && !(listener instanceof java.io.Serializable) ) {
            System.out.println("Warning: non-serializable listener in distributed object");
            flags ^= DISTRIBUTE;
        }
        */
    }
}
