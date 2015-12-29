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

package koch;

import hydra.*;

public class BareTest { 

  /**
   *  Load a lot of data objects.
   */
  public void loadResults(MCache cache) {
    int size = KochPrms.getObjSize();
    int type = KochPrms.getObjType();
    int n = KochPrms.getNumObjsToDo();

    long start = System.currentTimeMillis();
    switch( type ) {
      case KochPrms.BYTE_ARRAY:
        byte[] myBytes = new byte[size];
        for (int i = 0; i < n; i++) {
           cache.putItem(String.valueOf(i), myBytes);
        }
        break;
      case KochPrms.STRING:
        for (int i = 0; i < n; i++) {
           cache.putItem(String.valueOf(i), String.valueOf(i)); 
        }
        break;
      default:
        throw new HydraRuntimeException( "Should not happen" );
    }
    long end = System.currentTimeMillis();
    long total = end - start;
    System.out.println (" To load: " + n + " into cache took "
           + total + " millis " + total / n + " apiece");

  }
  /**
   *  Fetch a lot of data objects.
   */
  public void readResults(MCache cache) {

    int type = KochPrms.getObjType();
    int n = KochPrms.getNumObjsToDo();

    long start = System.currentTimeMillis();
    switch( type ) {
      case KochPrms.BYTE_ARRAY:
        for (int i = 0; i < n; i++) {
           byte[] ds = (byte[]) cache.getItem(String.valueOf(i)); 
        }
        break;
      case KochPrms.STRING:
        for (int i = 0; i < n; i++) {
           String str = ((String) cache.getItem(String.valueOf(i)));
           if (! str.equals( String.valueOf(i) ) ) {
             throw new HydraRuntimeException("Bad value: " + str);
           }
        }
        break;
      default:
        throw new HydraRuntimeException( "Should not happen" );
    }
    long end = System.currentTimeMillis();
    long total = end - start;
    System.out.println (" To read : " + n + " from cache took "
           + total + " millis " +
           total / n + " apiece");
  }
  /**
   *  Inittask to create a cache.  Intended for use by single-threaded client,
   *  since it opens the cache in a non-thread-safe fashion.
   */
  public static void opencacheTask() {
    localcache.set( new MCache() );
  }
  /**
   *  Task to load data into a cache.
   */
  public static void loadcacheTask() { 
    BareTest test = new BareTest(); 
    test.loadResults((MCache) localcache.get());
  }
  /**
   *  Task to read data from a cache.
   */
  public static void readcacheTask() { 
    BareTest test = new BareTest(); 
    test.readResults((MCache) localcache.get());
  }
  /**
   *  Closetask to close a cache.  Intended for use by single-threaded client,
   *  since it closes the cache in a non-thread-safe fashion.
   */
  public static void closecacheTask() {
    MCache cache = (MCache) localcache.get();
    cache.closeCache();
    localcache.set( null );
  }
  protected static HydraThreadLocal localcache = new HydraThreadLocal();
} 
