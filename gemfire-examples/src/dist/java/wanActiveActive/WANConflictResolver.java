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
package wanActiveActive;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.util.GatewayConflictHelper;
import com.gemstone.gemfire.cache.util.GatewayConflictResolver;
import com.gemstone.gemfire.cache.util.TimestampedEntryEvent;
import com.gemstone.gemfire.pdx.PdxInstance;

import java.util.List;
import java.util.Properties;


/**
 * Class <code>WANConflictResolver</code> is a <code>GatewayConflictResolver</code> 
 * provides several alternative methods for resolving conflicts in a WAN configuration
 * when multiple sites are modifying the same objects.<p>
 * The resolver is installed into the cache through eu-hub/cache.xml and
 * us-hub/cache.xml.  These determine the type of conflict resolution that
 * will be performed.  The options are:
 * <pre>
 *      use-latest
 *      merge
 *      highest-int
 * </pre>
 * <i>use-latest</i> will select to keep the event with the latest timestamp.<br>
 * <i>merge will</i> select to merge the values together.<br>
 * <i>highest-int</i> will select to keep the event with the highest modificationCount field.
 */
public class WANConflictResolver implements GatewayConflictResolver, Declarable,
  Dialog.CloseCallback {

  /** Counter tracking the number of conflicting events received */
  protected int numberOfConflicts;
  
  Dialog dialog;
  
  private Thread guiUpdateThread;
  
  /** Conflict resolution options */
  static enum conflictResolutionEnum { USE_LATEST, MERGE, HIGHEST_INT };
  
  conflictResolutionEnum resolutionType = conflictResolutionEnum.USE_LATEST;

  /**
   * Initializes this <code>WANConflictResolver</code>.
   * @param p The <code>Properties</code> with which to initialize this
   * <code>WANConflictResolver</code>
   */
  public void init(Properties p) {
    String rType = p.getProperty("resolution-type");
    if (rType == null) {
      System.out.println("WANConflictResolver: server conflict resolution will keep the latest change");
      resolutionType = conflictResolutionEnum.USE_LATEST;
    } else if (rType.equalsIgnoreCase("use-latest")) {
      System.out.println("WANConflictResolver: server conflict resolution will keep the latest change");
      resolutionType = conflictResolutionEnum.USE_LATEST;
    } else if (rType.equalsIgnoreCase("merge")) {
      System.out.println("WANConflictResolver: server conflict resolution will merge conflicting changes");
      resolutionType = conflictResolutionEnum.MERGE;
    } else if (rType.equalsIgnoreCase("highest-int")) {
      System.out.println("WANConflictResolver: server conflict resolution will keep the highest modificationCount");
      resolutionType = conflictResolutionEnum.HIGHEST_INT;
    }
    /** open GUI to show state and conflict count */
    String site = p.getProperty("site", "");
    this.dialog = new Dialog(site + " conflict resolver", this, true);
    this.dialog.open();
    this.dialog.setModification("conflict resolver initialized with \"" + rType + "\"");
     
    startGUIUpdateThread();
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.util.GatewayConflictResolver#onEvent(com.gemstone.gemfire.cache.util.TimestampedEntryEvent, com.gemstone.gemfire.cache.util.GatewayConflictHelper)
   */
  @Override
  public void onEvent(TimestampedEntryEvent event, GatewayConflictHelper helper) {
    LogWriter log = CacheFactory.getAnyInstance().getLogger();
    
    if (!event.getOperation().isUpdate()) {
      // for things other than updates use the default timestamp and ID based resolution
      if (event.getOldTimestamp() > event.getNewTimestamp()) {
        helper.disallowEvent();
      }
      if (event.getOldTimestamp() == event.getNewTimestamp()
          && event.getOldDistributedSystemID() > event.getNewDistributedSystemID()) {
        helper.disallowEvent();
      }
      return;
    }

    // cache.xml sets the PDX read-serialized attribute so that the conflict resolver
    // can work directly with serialized objects.
    
    PdxInstance oldObj = (PdxInstance)event.getOldValue();
    PdxInstance newObj = (PdxInstance)event.getNewValue();

    // log information about the conflict.  This will appear in cacheserver.log
    log.info("WANConflictResolver: \nexisting timestamp=" + event.getOldTimestamp()
        + "\nexisting value=" + oldObj
        + "\nproposed timestamp=" + event.getNewTimestamp()
        + "\nproposed value=" + newObj);

    switch (this.resolutionType) {
    case USE_LATEST:
      if (event.getOldTimestamp() > event.getNewTimestamp()) {
        log.info("event is older than cached value - disallowing event");
        helper.disallowEvent();
      }
      else if (event.getOldTimestamp() == event.getNewTimestamp()
          && event.getOldDistributedSystemID() != 2) {
        /* eu hub always wins if timestamps are the same */
        log.info("event has the same timestamp as the cached value but is from US - disallowing event");
        helper.disallowEvent();
      }
//      else {
//        log.info("event is newer than the cached value - allowing event");
//      }
      break;

    case MERGE:
      // Grab the conflicting event's history and add the current value's change to it.
      // This assumes there has been only one conflicting modification.  Otherwise we
      // would need to do a more thorough job of merging the histories.
      String lastChange = Value.getModification(oldObj);

      List<String> history = Value.getHistory(newObj);
      if (!history.contains(lastChange)) {
        log.info("merging existing change '" + lastChange +"' into new value");
        history.add(0, lastChange);
        PdxInstance newValue = Value.setHistory(newObj, history);
        helper.changeEventValue(newValue);
      } else {
        log.info("new history contains my last known change, so I'm allowing this event");
      }
      break;

    case HIGHEST_INT:
      // The value is stored as a PDX serialized object, so we use PDX to access it
      int oldInt = Value.getModificationCount(oldObj);
      int newInt = Value.getModificationCount(newObj);
      if (oldInt > newInt) {
        log.info("disallowing event");
        helper.disallowEvent();
      }
      break;
    }

    this.dialog.setConflictCount(++this.numberOfConflicts);
  }

  private void startGUIUpdateThread() {
    this.guiUpdateThread = new Thread("wanActiveActive GUI update thread") {
      public void run() {
        while (true) {
          try {
            Region region = CacheFactory.getAnyInstance().getRegion("/wanActiveActive");
            if (region != null) {
              PdxInstance serializedValue = (PdxInstance)region.get("MyValue");
              if (serializedValue != null) {
                dialog.setModification(Value.getModification(serializedValue));
                dialog.setHistory(Value.getHistory(serializedValue).toString());
              }
            }
            try { Thread.sleep(2000); } catch (InterruptedException e) { return; }
          } catch (CacheClosedException e) {
            return;
          } catch (RuntimeException e) {
            e.printStackTrace();
            return;
          }
        }
      }
    };
    this.guiUpdateThread.setDaemon(true);
    this.guiUpdateThread.start();
  }
  /** This is invoked when the dialog closes.  It will cause the server to shut down. */
  public void close() {
    System.exit(0);
  }

}
