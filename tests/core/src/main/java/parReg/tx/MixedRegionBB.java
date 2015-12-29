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
package parReg.tx;

import util.*;
import hydra.*;
import hydra.blackboard.*;

public class MixedRegionBB extends Blackboard {
   
// Blackboard variables
static String BB_NAME = "MixedRegion_Blackboard";
static String BB_TYPE = "RMI";

private static MixedRegionBB bbInstance = null;

// Counters for events expected in tx and cache writers
// writers are only invoked in 1 VM (locally for RR, in primary for PR)
public static int WRITER_CREATE;
public static int WRITER_CREATE_ISLOAD;
public static int WRITER_UPDATE;
public static int WRITER_UPDATE_ISLOAD;
public static int WRITER_DESTROY;
public static int WRITER_INVALIDATE;
public static int WRITER_LOCAL_DESTROY;
public static int WRITER_LOCAL_INVALIDATE;

// Counters for events expected in remote cache & tx listeners (collapsed)
// there are separate counters for partitioned (PR) and replicated (RR) regions
public static int PR_LISTENER_CREATE;
public static int PR_LISTENER_CREATE_ISLOAD;
public static int PR_LISTENER_UPDATE;
public static int PR_LISTENER_UPDATE_ISLOAD;
public static int PR_LISTENER_DESTROY;
public static int PR_LISTENER_INVALIDATE;
public static int PR_LISTENER_LOCAL_DESTROY;
public static int PR_LISTENER_LOCAL_INVALIDATE;

public static int RR_LISTENER_CREATE;
public static int RR_LISTENER_CREATE_ISLOAD;
public static int RR_LISTENER_UPDATE;
public static int RR_LISTENER_UPDATE_ISLOAD;
public static int RR_LISTENER_DESTROY;
public static int RR_LISTENER_INVALIDATE;
public static int RR_LISTENER_LOCAL_DESTROY;
public static int RR_LISTENER_LOCAL_INVALIDATE;

// Counter for TxEvent seen only in TX VM (when create/destroy conflated to destroy)
public static int CONFLATED_CREATE_DESTROY;
// Counter for TxEvent seen only in TX VM (CommitConflict/FailedCommit)
// seen when an entry-create is followed by a region-destroy on the host region
public static int CREATE_IN_DESTROYED_REGION;
public static int CREATE_IN_DESTROYED_REGION_ISLOAD;

/** Clear the event counters (associated with number of operations performed */
public void zeroEventCounters() {
   SharedCounters sc = getSharedCounters();
   sc.zero(WRITER_CREATE);
   sc.zero(WRITER_CREATE_ISLOAD);
   sc.zero(WRITER_UPDATE);
   sc.zero(WRITER_UPDATE_ISLOAD);
   sc.zero(WRITER_DESTROY);
   sc.zero(WRITER_INVALIDATE);
   sc.zero(WRITER_LOCAL_DESTROY);
   sc.zero(WRITER_LOCAL_INVALIDATE);

   sc.zero(CONFLATED_CREATE_DESTROY);
   sc.zero(CREATE_IN_DESTROYED_REGION);
   sc.zero(CREATE_IN_DESTROYED_REGION_ISLOAD);

   sc.zero(PR_LISTENER_CREATE);
   sc.zero(PR_LISTENER_CREATE_ISLOAD);
   sc.zero(PR_LISTENER_UPDATE);
   sc.zero(PR_LISTENER_UPDATE_ISLOAD);
   sc.zero(PR_LISTENER_DESTROY);
   sc.zero(PR_LISTENER_INVALIDATE);
   sc.zero(PR_LISTENER_LOCAL_DESTROY);
   sc.zero(PR_LISTENER_LOCAL_INVALIDATE);

   sc.zero(RR_LISTENER_CREATE);
   sc.zero(RR_LISTENER_CREATE_ISLOAD);
   sc.zero(RR_LISTENER_UPDATE);
   sc.zero(RR_LISTENER_UPDATE_ISLOAD);
   sc.zero(RR_LISTENER_DESTROY);
   sc.zero(RR_LISTENER_INVALIDATE);
   sc.zero(RR_LISTENER_LOCAL_DESTROY);
   sc.zero(RR_LISTENER_LOCAL_INVALIDATE);

}

/**
 *  Get the instance of MixedRegionBB
 */
public static MixedRegionBB getBB() {
   if (bbInstance == null) {
      synchronized ( MixedRegionBB.class ) {
         if (bbInstance == null) 
            bbInstance = new MixedRegionBB(BB_NAME, BB_TYPE);
      }
   }
   return bbInstance;
}
   
/**
 *  Zero-arg constructor for remote method invocations.
 */
public MixedRegionBB() {
}
   
/**
 *  Creates a sample blackboard using the specified name and transport type.
 */
public MixedRegionBB(String name, String type) {
   super(name, type, MixedRegionBB.class);
}
   
}
