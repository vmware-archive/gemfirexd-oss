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


import java.util.Vector;
import java.util.Map;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;

import com.gemstone.org.jgroups.*;

//import org.jgroups.*;
//import org.jgroups.util.Util;




 public class CacheAccess implements Runnable {

    private static CacheAccess instance;
    private static Channel jgroups;
    private static Attributes regionAttr;
//    private static boolean regionSync;
    
    /** code for distributed put message */
    private final static int PUT_OPERATION = 0;
    /** code for distributed destroy message */
    private final static int DESTROY_OPERATION = 1;
    
    /** the jgroups channel used for communications */
    private Channel jchannel;
    
    /** the jgroups reader thread */
    private Thread readerThread;
    
    /** the reader thread's shutdown variable */
    private volatile boolean shutdown;
    
    /** the jgroups group name */
    private static String groupname="GFCache";

    /** local storage map (should be a concurrent map) */
    private Map localm;
    
    /** for ocs4j-like ownership we have the concept of a current entry */
//    private CacheEntry currentEntry;
    
    /** whether currentEntry is owned by this access object */
//    private boolean ownedByMe;
    
    /** whether the cacheaccess object is closed */
    private boolean closed;
    
    /** whether to trace operations */
    private boolean trace;

    /** whether to use multicast to send messages */
    private boolean usemcast = Boolean.getBoolean("usemcast");

    public CacheAccess() {
    }

    public static CacheAccess getDefault()
    {
        return instance;
    }
    
    /** is this cache's jgroups channel open? */
    public static boolean isOpen() {
      if (instance == null) {
        System.out.println("CacheAcces.isOpen: no instances");
        return false;
      }
      Channel ch = instance.jchannel;
      if (ch == null) {
        System.out.println("CacheAccess.isOpen: no channel");
        return false;
      }
      if (!ch.isOpen()) {
        System.out.println("CacheAccess.isOpen: channel not open");
        return false;
      }
      if (!ch.isConnected()) {
        System.out.println("CacheAccess.isOpen: channel not connected");
        return false;
      }
      return true;
    }

    public void contentsSet(Map new_entries) {
      localm = new_entries;
    }
    
    public static void defineRegion(String regionName, Attributes attr) 
        throws ObjectExistsException, NullObjectNameException, CacheNotAvailableException
    {
        // for this prototype we ignore the region name and only track one region
        regionAttr = attr;
        groupname = regionName;
    }
    
    public static CacheAccess getAccess(String regionName, String props) throws JCacheException
    {
        boolean trace;
        
        groupname = regionName;

        trace = Boolean.getBoolean("trace");
        
        /* here's how you create a channel with the old version of javagroups
           that we have in gemfire */
        
        JChannelFactory factory = new JChannelFactory();
        
        /* the stuff commented out below is what you'd use to instantiate
           a channel in jgroups with an xml file */
        //File propsFile = new File(props);

        //if (trace) System.out.println("jg properties file="+props);


        instance = new CacheAccess();
        
        //JChannelFactory factory = null;
        //try {
        //  factory = new JChannelFactory(propsFile);
        //}
        //catch (ChannelException ce) {
        //  throw new JCacheException("unable to initialize jgroups", ce);
        //}
        
        instance.start(factory, props, trace);
        
        return instance;
    }
    
    public void close() throws JCacheException
    {
        if (!closed) {
            closed = true;
            stopReader();
            jchannel.close();
        }
    }
    
    public Object get(Object name) throws JCacheException
    {
        return get(name, null);
    }
    
    public Object get(Object name, Object args) throws JCacheException
    {
        CacheEntry ce = null;
        ce = (CacheEntry)localm.get(name);
        if (ce != null) {
//            currentEntry = ce;
            if (ce.isValid())
                return ce.getValue();
            if (trace) System.out.println("Found invalid entry for '" + name + "'");
        }
            
        // see if there's a loader in the region that could be used to
        // get the object
        CacheLoader cl = null;
        if (ce == null || ce.getAttributes().getLoader() == null)
            cl = regionAttr.getLoader();
        else
            cl = ce.getAttributes().getLoader();
        if (cl != null) {
            if (ce == null)
                ce = new CacheEntry(name, (Attributes)regionAttr.clone());
            if (trace) System.out.println("Invoking loader for '" + name + "'");
            Object loadedObj = cl.load(ce, args);
            if (loadedObj != null) {
                ce.setValue(loadedObj);
//                currentEntry = ce;
                localm.put(name, ce);
                if (ce.getAttributes().isDistributed())
                    distributePut(name, loadedObj);
                return loadedObj;
            }
        }
        throw new ObjectNotFoundException();
    }

    public void put(Object name, Object value) throws JCacheException
    {
        put(name, (Attributes)regionAttr.clone(), value);
    }
    
    public void put(Object name, Attributes attr, Object value)
        throws JCacheException
    {
        // ignore ownership for now
        //if (localm.containsKey(name))
        //    throw new ObjectExistsException();
        replace(name, attr, value);
    }
    
    public void replace(Object name, Object value) throws JCacheException
    {
        replace(name, (Attributes)null, value);
    }
    
    private void replace(Object name, Attributes attr, Object value)
        throws JCacheException
    {
        CacheEntry ce;
        
        if (localm.containsKey(name)) {
            ce = (CacheEntry)localm.get(name);
            ce.setValue(value);
            if (trace) System.out.println("replacing object '" + name + "' attributes:" + ce.getAttributes());
            if (ce.getAttributes().isDistributed()) {
                distributePut(name, value);
            }
        }
        else {
            if (attr == null)
                attr = (Attributes)regionAttr.clone();
            ce = new CacheEntry(name, attr, value);
            if (trace) System.out.println("adding object '" + name + "' attributes:" + attr);
            localm.put(name, ce);
//            currentEntry = ce;
            if (attr.isDistributed())
                distributePut(name, value);
        }
    }
        
    public void invalidate() throws JCacheException
    {
        // not implemented
    }
    
    
    public void invalidate(Object name) throws JCacheException
    {
        CacheEntry ce;
        ce = (CacheEntry)localm.get(name);
        if (ce != null) {
            if (trace) System.out.println("invalidating local object '" + name + "'");
            ce.invalidate();
            if (ce.getAttributes().isDistributed())
                distributePut(name, null);
            // localm.put(name, ce);
        }
        else
            throw new ObjectNotFoundException();
    }
    
    public void defineObject(Object name, Attributes attr)
        throws JCacheException
    {
        CacheEntry ce;

        if (localm.containsKey(name))
            throw new ObjectExistsException();
            
        if (attr == null)
            attr = (Attributes)regionAttr.clone();

        ce = new CacheEntry(name, attr);
        if (trace) System.out.println("defining object '" + name + "' attributes:" + attr);
        localm.put(name, ce);
        if (attr.isDistributed())
            distributePut(name, null);
    }
    
    private void distributePut(Object name, Object value)
      throws JCacheException
    {
      try {
        if (usemcast) {
          jchannel.send((Address)null, jchannel.getLocalAddress(),
            new Object[]{new Integer(PUT_OPERATION), name, value});
        }
        else {
          View v = jchannel.getView();
          Vector mbrs = v.getMembers();
          Address myAddr = jchannel.getLocalAddress();
          Message m = new Message((Address)null, jchannel.getLocalAddress(),
            new Object[]{new Integer(PUT_OPERATION), name, value});
          for (int i=mbrs.size()-1; i>=0; i--) {
            Address dest = (Address)mbrs.get(i);
            if (!dest.equals(myAddr)) {
              m.setDest(dest);
              jchannel.send(m);
            }
          }
        }
      }
      catch (Exception e) {
        throw new JCacheException("Unable to distributed put operation", e);
      }
    }

    private void distributeDestroy(Object name)
      throws JCacheException
    {
      try {
        jchannel.send((Address)null, jchannel.getLocalAddress(),
          new Object[]{new Integer(DESTROY_OPERATION), name});
        }
      catch (Exception e) {
        throw new JCacheException("Unable to distributed put operation", e);
      }
    }

    public void destroy(Object name)
      throws JCacheException
    {
        CacheEntry ce;
        ce = (CacheEntry)localm.get(name);
        if (ce != null) {
            if (trace) System.out.println("destroying object '" + name + "'");
            localm.remove(name);
            distributeDestroy(name);
        }
        else
            throw new ObjectNotFoundException();
    }
    
    public void waitForResponse(int timeout) throws JCacheException
    {
        // not needed - updates are synchronized
    }
    
    public void cancelResponse() throws JCacheException
    {
        // not needed - updates are synchronized
    }

    public void getOwnership(Object name, int timeout) throws JCacheException
    {
        // not implemented
    }
    
    public void releaseOwnership(int timeout) throws JCacheException
    {
        // not implemented
    }
    
    public void releaseOwnership(Object name, int timeout) throws JCacheException
    {
        // not implemented
    }
    
    public void resetAttributes(Attributes attr) throws JCacheException
    {
        regionAttr = attr;
//        regionSync = attr.isSet(Attributes.SYNCHRONIZE);
    }
    
    public void resetAttributes(Object name, Attributes attr) throws JCacheException,
        InvalidHandleException
    {
        CacheEntry ce;
        ce = (CacheEntry)localm.get(name);
        if (ce != null)
            (ce.getAttributes()).resetAttributes(attr);
        else
            throw new ObjectNotFoundException();
    }
    
    public Attributes getAttributes() throws JCacheException
    {
        return (Attributes)regionAttr.clone();
    }
    
    public Attributes getAttributes(Object name) throws JCacheException
    {
        CacheEntry ce;
        ce = (CacheEntry)localm.get(name);
        if (ce != null) {
            return (Attributes)(ce.getAttributes()).clone();
        }
        else
            throw new ObjectNotFoundException();
    }
    
    
    public void save()
    {
        // not implemented
    }
    
    public void preLoad(Object name)
    {
        // do it synchronously
        try {
            get(name);
        }
        catch (ObjectNotFoundException oe) {
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    
    public boolean isPresent(Object name)
    {
        CacheEntry ce;
        ce = (CacheEntry)localm.get(name);
        if (ce != null) {
            return ce.isValid();
        }
        return false;
    }


    protected Hashtable getCombinedView()
    {
        Hashtable t = new Hashtable();
        Iterator e = localm.entrySet().iterator();
        while (e.hasNext()) {
            CacheEntry ce = (CacheEntry)e.next();
            t.put(ce.getName(), ce.getInfo());
        }
        return t;
    }
    
    /** start jgroups channel reader thread */
    private void startReader() {
      Thread readerThread = new Thread(this);
      readerThread.setDaemon(true);
      readerThread.setName("jgroups reader thread");
      readerThread.start();
    }
    
    /** stop the jchannel reader thread */
    private void stopReader() {
      if (readerThread != null && readerThread.isAlive()) {
        shutdown = true;
        try { readerThread.join(20000); }
        catch (Exception e) { e.printStackTrace(); }
      }
    }
    
    /** jgroups channel reader thread's Runnable */
    public void run() {
      while (!shutdown) {
        readJGroupsMessage();
        if (Thread.currentThread().isInterrupted()) { // GemStoneAddition
          shutdown = true;
          System.out.println("shutting down jchannel thread due to interrupt");
        }
      }
    }
    
    
    /** read a jgroups message and process it */
    private void readJGroupsMessage() {
      try {
        Object obj = jchannel.receive(5000);
        if (Thread.currentThread().isInterrupted()) { // GemStoneAddition
//          Thread.currentThread().interrupt(); // GemStoneAddition
          return;
        }
        if (obj != null) {
          if (obj instanceof Message) {
            processMessage((Message)obj);
          }
          // else if (obj instanceof View) { etc. etc.
        }
      }
      catch (TimeoutException te) {
      }
      catch (ChannelClosedException cl) {
        shutdown = true;
      }
      catch (Exception e) {
        e.printStackTrace();
      }
    }
    
    /** process a jgroups Message from another process */
    private void processMessage(Message m) {
      // don't process the message if it's a loopback
      if (m.getSrc().equals(jchannel.getLocalAddress())) {
        return;
      }
      //if (trace) System.out.println("Processing message " + m);
      Object[] payload = (Object[])m.getObject();
      switch (((Integer)payload[0]).intValue()) {
        case PUT_OPERATION:
          // PUT
          if (trace) System.out.println("distributed put("+payload[1]+","+payload[2]);
          CacheEntry ce = (CacheEntry)localm.get(payload[1]);
          if (ce == null) {
            ce = new CacheEntry(payload[1], (Attributes)regionAttr.clone());
            ce.setValue(payload[2]);
            localm.put(payload[1], ce);
          }
          else {
            ce.setValue(payload[2]);
          }
          CacheEventListener l = ce.getAttributes().getListener();
          if (l != null) {
            try {
              if (trace) System.out.println("invoking listener");
              l.handleEvent(new CacheEvent(
                ce.isValid()? CacheEvent.OBJECT_UPDATED : 
                  CacheEvent.OBJECT_INVALIDATED));
            }
            catch (Throwable t) {
              System.err.println("Error in applying distributed Put");
              t.printStackTrace();
            }
          }
          break;
        case DESTROY_OPERATION:
          // DESTROY
          ce = (CacheEntry)localm.remove(payload[1]);
          if (ce != null) {
            l = ce.getAttributes().getListener();
            if (l != null) {
              try {
                l.handleEvent(new CacheEvent(CacheEvent.OBJECT_DESTROYED));
              }
              catch (Throwable t) {
                System.err.println("Error in applying distributed Put");
                t.printStackTrace();
              }
            }
          }
          break;
        default:
          System.err.println("Unknown message type: " + payload[0]);
          break;
      }
    }
    
    
    private void start(ChannelFactory factory, String props, boolean trace) {
        this.trace = trace;
        try {
          jchannel = factory.createChannel(props);
          jchannel.connect("bcache");
          startReader();
        }
        catch (ChannelException ce) {
          throw new RuntimeException("error initializing jgroups", ce);
        }
        localm = new HashMap();
        if (regionAttr == null)
            regionAttr = new Attributes();
//        regionSync = regionAttr.isSet(Attributes.SYNCHRONIZE);
        int tries = 0;
        while (!jchannel.isConnected()) {
          try { Thread.sleep(1000); } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            break;
          }
          tries += 1;
          if (tries > 4) {
            System.err.println("warning: jgroups channel still not connected after " + tries + " seconds");
            break;
          }
        }

    }

    


    public void entrySet(Object key, Object value)
    {
        CacheEntry ce = (CacheEntry)localm.get(key);
        Attributes attr;
        if (ce != null && (attr = ce.getAttributes()).isDistributed()) {
            if (trace) System.out.println("Update received for " + key);
            ce.setValue(value);
            if (attr.getListener() != null) {
                try {
                    attr.getListener().handleEvent(new CacheEvent(
                       (ce.isValid()? CacheEvent.OBJECT_UPDATED : CacheEvent.OBJECT_INVALIDATED)));
                }
                catch (Exception e) {
                    System.err.println("event listener error:");
                    e.printStackTrace();
                }
            }
        }
        else
            if (trace) System.out.println("Ignoring update for non-referenced object '" + key + "'");
    }

    public void entryRemoved(Object key)
    {
        //if (trace)
        //    System.out.println("entryRemoved("+key+")");
    }

    public void viewChange(Vector joined, Vector left) {
        if (trace)
            System.out.println("New members: " + joined + ", left members: " + left);
    }
    
}
