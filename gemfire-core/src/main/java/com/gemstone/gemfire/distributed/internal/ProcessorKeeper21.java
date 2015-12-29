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

import com.gemstone.gemfire.internal.*;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A message processor class typically creates an instance of
 * ProcessorKeeper and holds it in a static variable.  In the
 * constructor of the processor class, do this:
 * 
 * <code>processorId = keeper.put()</code>
 *  
 * When a message is processed, do this:
 * 
 * <code>processor = <processorclass>.keeper().get(processorId);
 * 
 * <p>Processor ids are always greater than 0.</p>
 */
public class ProcessorKeeper21 {
  
  private static ArrayList<ProcessorKeeper21> cleanupAllProcessKeepers = new ArrayList<ProcessorKeeper21>();
  
  /** 
   * Key is a unique id, value is an instance of some processor class
   */
  private final ObjIdConcurrentMap<Object> map =
    new ObjIdConcurrentMap<Object>();

  private final ReferenceQueue processorIdReferenceQueue;
  
  /**
   * If true then use weak refs to reference the processors.
   */
  private final boolean useWeakRefs;

  private final AtomicInteger nextKey = new AtomicInteger(1);

  public ProcessorKeeper21() {
    this(true);
  }
  
  public ProcessorKeeper21(boolean useWeakRefs) {
    this.useWeakRefs = useWeakRefs;
    //should I use private construtor or this is fine
    if(useWeakRefs) {
      processorIdReferenceQueue = new ReferenceQueue();
      synchronized (cleanupAllProcessKeepers) {
        cleanupAllProcessKeepers.add(this);
      }
    } else {
      processorIdReferenceQueue = null;
    }
  }

  private int getNextId() {
    int id = this.nextKey.getAndIncrement();
    if (id <= 0) {
      // id must be >= 0 since ObjIdMap does not supports keys < 0.
      // We don't use 0 just to keep it reserved as an illegal id.
      synchronized (this.nextKey) {
        id = this.nextKey.get();
        if (id <= 0) {
          this.nextKey.set(1);
        }
      }
      id = this.nextKey.getAndIncrement();
    }
    return id;
  }

  /**
   * Save the processor in this keeper, generate an id for the processor,
   * and return that id so it can be used to retrieve the processor later.
   * This keeper keeps a weak reference to the processor.
   * @param processor the processor to keep
   * @return the unique id for processor
   */
  public int put(Object processor) {
    int id;
    Object obj;
    
    do {
      id = getNextId();
      if (this.useWeakRefs) {
        obj = new ProcessorHolder(id, processor, processorIdReferenceQueue);
      }
      else {
        obj = processor;
      }
    } while (this.map.putIfAbsent(id, obj) != null);
    Assert.assertTrue(id > 0);
    return id;
  }

  /**
   * Retrieve a processor that was previously put() in this keeper.  The
   * id is the value returned from put().  If there is no processor by
   * that id, or it has been garbage collected, null is returned.
   */
  public Object retrieve(int id) {
    Object o = null;
    if (this.useWeakRefs) {
      final WeakReference<?> ref = (WeakReference<?>)this.map.get(id);
      if (ref != null) {
        o = ref.get();
        if (o == null) {
          // Clean up
          this.map.remove(id, ref);
        }
      }
    } else {
      o = this.map.get(id);
    }
    // System.out.println("ProcessorKeeper.retrieve(" + int + ") returning " + processor);
    return o;
  }

  public static void cleanupProcessor(){
    synchronized (ProcessorKeeper21.cleanupAllProcessKeepers) {
      for(ProcessorKeeper21 pk : ProcessorKeeper21.cleanupAllProcessKeepers){
        pk.cleanup();
      }
      
    }
  }
  
  final class ProcessorHolder extends WeakReference {
    final int key;
    public ProcessorHolder(int key, Object val, ReferenceQueue queue){
      super(val, queue);
      this.key = key;
    }
    public int getKey(){
      return this.key;
    }
  }
  
  public void cleanup(){
    Object obj = null;
    while((obj = processorIdReferenceQueue.poll()) != null){
      ProcessorHolder pid = (ProcessorHolder)obj;
      this.map.remove(pid.getKey());
    }
  }
  /**
   * Remove the processor with the given id.  It's okay if no
   * processor with that id exists.
   */
  public void remove(int id) {
    map.remove(id);
  }

}
