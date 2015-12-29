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
package com.gemstone.gemfire.management.internal;

import java.util.List;
import java.util.Set;

import com.gemstone.gemfire.distributed.internal.MembershipListener;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * This listener is added to the cache when a node becomes Managing node. It
 * then starts to listen on various membership events to take steps accordingly
 * 
 * @author rishim
 * 
 */

public class ManagementMembershipListener implements MembershipListener {

  /**
   * Resource Manager
   */
  private SystemManagementService service;

  final LogWriterI18n logger;

  public ManagementMembershipListener(SystemManagementService service, LogWriterI18n logger) {
    this.service = service;
    this.logger = logger;
  }

  @Override
  public void memberDeparted(InternalDistributedMember id, boolean crashed) {
    if (logger.infoEnabled()) {
      logger.info(LocalizedStrings.DEBUG, "ManagementMembershipListener member departed.. " + id.getId());
    }
    if (service.isManager()) {
      if (logger.fineEnabled()) {
        logger.fine("Removing member artifacts for " + id.getId() + " from manager ");
      }
      service.getFederatingManager().removeMember(id, crashed);
    }
  }

  @Override
  public void memberJoined(InternalDistributedMember id) {

    if (logger.infoEnabled()) {
      logger.info(LocalizedStrings.DEBUG, "ManagementMembershipListener member joined .. " + id.getId());
    }
    if (service.isManager()) {
      if (logger.fineEnabled()) {
        logger.fine("Adding member artifacts for " + id.getId() + " to manager ");
      }
      service.getFederatingManager().addMember(id);
    }
  }

  @Override
  public void memberSuspect(InternalDistributedMember id, InternalDistributedMember whoSuspected) {

    if (logger.infoEnabled()) {
      logger.info(LocalizedStrings.DEBUG, "ManagementMembershipListener member suspected .. " + id.getId());
    }
    if (service.isManager()) {
      if (logger.fineEnabled()) {
        logger.fine("Suspecting member " + id.getId());
      }
      service.getFederatingManager().suspectMember(id, whoSuspected);
    }
  }

  public void quorumLost(Set<InternalDistributedMember> failures, List<InternalDistributedMember> remaining) {
  }
}
