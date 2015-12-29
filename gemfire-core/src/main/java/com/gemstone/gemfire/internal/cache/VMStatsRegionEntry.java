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

package com.gemstone.gemfire.internal.cache;

/**
 * Implementation class of RegionEntry interface.
 * VM -> entries stored in VM memory
 * Stats -> extra statistics
 *
 * @since 3.5.1
 *
 * @author Darrel Schneider
 *
 */
@SuppressWarnings("serial")
public abstract class VMStatsRegionEntry extends AbstractRegionEntry {
  protected VMStatsRegionEntry(RegionEntryContext context, Object value) {
    super(context, value);
  }

  /////////////////////////////////////////////////////////////
  /////////////////////////// fields //////////////////////////
  /////////////////////////////////////////////////////////////
  // Do not add any instance fields to this class.
  // Instead add them to the STATS section of LeafRegionEntry.cpp
}

