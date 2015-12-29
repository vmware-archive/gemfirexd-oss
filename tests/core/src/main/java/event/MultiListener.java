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
package event; 

import hydra.*;
import com.gemstone.gemfire.cache.*;

/** 
 *  MultiListener.
 *
 *  This listener can be assigned a name either at the time of instantiation
 *  or afterwards.  The Listener names are assigned via util.NameFactory.
 *
 *  Upon invocation of the listener callbacks, this listener logs the call
 *  (along with the details of the event) and adds it's assigned name to the
 *  InvokedListeners list in the BB.
 *
 *  @see ListenerBB.InvokedListeners
 *  @see util.NameFactory
 */
public class MultiListener extends util.AbstractListener implements CacheListener, Declarable {

private boolean isCarefulValidation;  // serialExecution mode

/* name assigned to this MultiListener, e.g. MultiListener_XX */
private String name = "MultiListener";

/**
 * noArg contstructor 
 */
public MultiListener() {
   this.isCarefulValidation = TestConfig.tab().booleanAt(Prms.serialExecution);
}

/**
 *  Create a new listener and specify the assigned name for this listener
 */
public MultiListener(String name) {
   this.name = name;
   this.isCarefulValidation = TestConfig.tab().booleanAt(Prms.serialExecution);
}

/**
 *  Set method for name when instantiated with noArg constructor
 */
public void setName(String name) {
   this.name = name;
}

/**
 *  Get method for name 
 */
public String getName() {
   return this.name;
}

//=============================================================================
//  implementation of CacheListener Methods
//=============================================================================

/**
 *  Handles the event of a new key being added to region: logs call and
 *  adds the name of this listener to the ListenerBB.InvokedListeners list (String)
 */
public void afterCreate(EntryEvent event) {
   logCall("afterCreate", event);
   updateBB(event.getRegion().getName());
}

/**
 *  Handles the event of an entry being destroyed in a Region : logs call and
 *  adds the name of this listener to the ListenerBB.InvokedListeners list (String)
 */
public void afterDestroy(EntryEvent event) {
   logCall("afterDestroy", event);
   updateBB(event.getRegion().getName());
}

/**
 *  Handles the event of an entry's value being invalidated: logs call and
 *  adds the name of this listener to the ListenerBB.InvokedListeners list (String)
 */
public void afterInvalidate(EntryEvent event) {
   logCall("afterInvalidate", event);
   updateBB(event.getRegion().getName());
}

/**
 *  Handles the event of a region being destroyed: logs call and
 *  adds the name of this listener to the ListenerBB.InvokedListeners list (String)
 */
public void afterRegionDestroy(RegionEvent event) {
   logCall("afterRegionDestroy", event);

   if (!event.getOperation().isClose()) {
      updateBB(event.getRegion().getName());
   }
}

/**
 *  Handles the event of a region being invalidated: logs call and
 *  adds the name of this listener to the ListenerBB.InvokedListeners list (String)
 */
public void afterRegionInvalidate(RegionEvent event) {
   logCall("afterRegionInvalidate", event);
   updateBB(event.getRegion().getName());
}

/**
 *  Handles the event of a key being modified in a region: logs call and
 *  adds the name of this listener to the ListenerBB.InvokedListeners list (String)
 */
public void afterUpdate(EntryEvent event) {
   logCall("afterUpdate", event);
   updateBB(event.getRegion().getName());
}

/**
 *  Called when the region containing this callback is destroyed, when the cache
 *  is closed, or when a callback is removed from a region using an 
 *  <code>AttributesMutator</code>.  Simply logs invocation.
 */
public void close() {
   logCall("close", null);
}

/**
 *  Handles the event of a region being cleared:  logs call
 *  adds the name of this listener to the ListenerBB.InvokedListeners list (String)
 */
public void afterRegionClear(RegionEvent event) {
  logCall("afterRegionClear", event);
  updateBB(event.getRegion().getName());
}

/**
 *  Handles the event of a region being created:  logs call and 
 *  adds the name of this listener to the ListenerBB.InvokedListeners list (String)
 */
public void afterRegionCreate(RegionEvent event) {
  logCall("afterRegionCreate", event);
  if (event.getRegion().getParentRegion() != null) {
     // if the parent is null, this is the creation of the root region
     updateBB(event.getRegion().getParentRegion().getName());
  }
}

/**
 *  Handles the event of a region being live:  logs call and 
 *  adds the name of this listener to the ListenerBB.InvokedListeners list (String)
 */
public void afterRegionLive(RegionEvent event) {
  logCall("afterRegionLive", event);
  if (event.getRegion().getParentRegion() != null) {
     // if the parent is null, this is the creation of the root region
     updateBB(event.getRegion().getParentRegion().getName());
  }
}

/**
 *  Utility method to update the InvokedListeners list in the BB with the 
 *  name of this listener.
 */
private void updateBB(String regionName) {

  // We can only track the Listeners invoked in serialExecution mode
  if (!isCarefulValidation) {
     return;
  }

  String clientName = System.getProperty( "clientName" );
  String key = ListenerBB.InvokedListeners + clientName + "_" + regionName;
  String invokedList = (String)ListenerBB.getBB().getSharedMap().get(key);

  // In the case of addRegion, we're naming the expected & invoked lists after
  // the parent region, but afterCreates are reported for the new region (and
  // we won't have initialized.  We don't care about these listener invocations
  // anyway.
  if (invokedList != null) {
     StringBuffer invoked = new StringBuffer(invokedList);
     invoked.append(this.name + ":");
     ListenerBB.getBB().getSharedMap().put(key, invoked.toString());
  }
}

public void init(java.util.Properties prop) {
   logCall("init(Properties)", null);
}

}
