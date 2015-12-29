/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.SystemAggregator

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

/*
 * Changes for GemFireXD distributed data platform (some marked by "GemStone changes")
 *
 * Portions Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
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

import com.pivotal.gemfirexd.internal.engine.sql.compile.types.DVDSet;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecAggregator;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

/**
 * Abstract aggregator that is extended by all internal
 * (system) aggregators.
 *
 */
abstract class SystemAggregator implements ExecAggregator
// GemStone changes BEGIN
    , Comparable<Object>
// GemStone changes END
{

// GemStone changes BEGIN
    /* (original code)
    private boolean eliminatedNulls;
    */

  protected boolean eliminatedNulls;
  protected int inputColumnPos;
  protected int resultColumnPos;
  protected boolean isDistinct;
  protected boolean isAccumulateDistinctValues;

  protected final void setup(AggregatorInfo aggInfo) {
    this.inputColumnPos = aggInfo.inputColumn + 1;
    this.resultColumnPos = aggInfo.outputColumn + 1;
    this.isDistinct = aggInfo.isDistinct;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void accumulate(ExecRow inputRow) throws StandardException {
    final DataValueDescriptor addend = inputRow.getColumn(this.inputColumnPos);
    if (this.isAccumulateDistinctValues) {
      GenericAggregator.addEntryToDVDSet(addend,
          inputRow.getColumn(this.resultColumnPos));
    }
    else {
      if (addend != null && !addend.isNull()) {
        basicAccumulate(addend);
      }
      else {
        this.eliminatedNulls = true;
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setResult(ExecRow row, int columnPos) throws StandardException {
    row.setValue(columnPos - 1, getResult());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean finish(ExecRow row, boolean isByteArray)
      throws StandardException {
    final boolean eliminatedNulls = this.eliminatedNulls;
    this.eliminatedNulls = false;
    if (this.isAccumulateDistinctValues) {
      clear();
      if (SanityManager.DEBUG) {
        final DataValueDescriptor outputColumn = row
            .getColumn(this.resultColumnPos);
        if (outputColumn.getClass() != DVDSet.class) {
          SanityManager.THROWASSERT("aggregate Result must be of type "
              + DVDSet.class.getSimpleName() + ", but was "
              + outputColumn.getClass().getName());
        }
      }
      return false;
    }

    row.setValue(this.resultColumnPos - 1, getResult());
    clear();
    return eliminatedNulls;
  }

  //protected abstract void clear();
  public  void clear() {
    this.eliminatedNulls = false;
  }
// GemStone changes END
  
	public final boolean didEliminateNulls() {
		return eliminatedNulls;
	}

	public void accumulate(DataValueDescriptor addend /* GemStone change (original code), Object ga */) 
		throws StandardException
	{
		if ((addend == null) || addend.isNull()) {
			eliminatedNulls = true;
			return;
		}

		this.basicAccumulate(addend);
	}

	protected abstract void basicAccumulate(DataValueDescriptor addend)
		throws StandardException;
	/////////////////////////////////////////////////////////////
	// 
	// EXTERNALIZABLE INTERFACE
	// 
	/////////////////////////////////////////////////////////////

	public void writeExternal(ObjectOutput out) throws IOException
	{
		out.writeBoolean(eliminatedNulls);
	}

	public void readExternal(ObjectInput in) 
		throws IOException, ClassNotFoundException
	{
		eliminatedNulls = in.readBoolean();
	}
}
