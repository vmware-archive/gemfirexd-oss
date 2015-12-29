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

package com.pivotal.gemfirexd.internal.impl.sql.execute;

import com.pivotal.gemfirexd.internal.iapi.services.loader.GeneratedMethod;
import com.pivotal.gemfirexd.internal.iapi.store.access.Qualifier;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;

/**
 * Encapsulates start, stop positions with operators for a single table/index
 * scan.
 * 
 * @author swale
 * @since 7.0
 */
public class SearchSpecification {

  final long conglomId;

  final GeneratedMethod startKeyGetter;

  final int startSearchOperator;

  final GeneratedMethod stopKeyGetter;

  final int stopSearchOperator;

  final boolean sameStartStopPosition;

  final Qualifier[][] qualifiers;

  final DataValueDescriptor[] probingVals;

  final boolean oneRowScan;

  public SearchSpecification(final long conglomId,
      final GeneratedMethod startKeyGetter, final int startSearchOperator,
      final GeneratedMethod stopKeyGetter, final int stopSearchOperator,
      final boolean sameStartStopPosition, final Qualifier[][] qualifiers,
      final DataValueDescriptor[] probeValues, final boolean oneRowScan) {
    this.conglomId = conglomId;
    this.startKeyGetter = startKeyGetter;
    this.startSearchOperator = startSearchOperator;
    this.stopKeyGetter = stopKeyGetter;
    this.stopSearchOperator = stopSearchOperator;
    this.sameStartStopPosition = sameStartStopPosition;
    this.qualifiers = qualifiers;
    this.probingVals = probeValues;
    this.oneRowScan = oneRowScan;
  }
}
