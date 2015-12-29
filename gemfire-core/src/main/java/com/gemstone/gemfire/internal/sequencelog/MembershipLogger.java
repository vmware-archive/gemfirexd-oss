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
package com.gemstone.gemfire.internal.sequencelog;

import java.util.regex.Pattern;

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;

/**
 * @author dsmith
 *
 */
public class MembershipLogger {
  
  private static final SequenceLogger GRAPH_LOGGER = SequenceLoggerImpl.getInstance();
  private static final Pattern ALL = Pattern.compile(".*");
  
  public static void logCrash(InternalDistributedMember member) {
    GRAPH_LOGGER.logTransition(GraphType.MEMBER, "member", "crash", "destroyed", member, member);
    GRAPH_LOGGER.logTransition(GraphType.REGION, ALL, "crash", "destroyed", member, member);
    GRAPH_LOGGER.logTransition(GraphType.KEY, ALL, "crash", "destroyed", member, member);
    GRAPH_LOGGER.logTransition(GraphType.MESSAGE, ALL, "crash", "destroyed", member, member);
  }
  
  public static void logStartup(InternalDistributedMember member) {
    GRAPH_LOGGER.logTransition(GraphType.MEMBER, "member", "start", "running", member, member);
  }
  
  public static void logShutdown(InternalDistributedMember member) {
    GRAPH_LOGGER.logTransition(GraphType.MEMBER, "member", "stop", "destroyed", member, member);
  }

}
