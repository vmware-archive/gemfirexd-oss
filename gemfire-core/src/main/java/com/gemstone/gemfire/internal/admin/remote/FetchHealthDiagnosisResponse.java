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
   
   
package com.gemstone.gemfire.internal.admin.remote;

import com.gemstone.gemfire.*;
import com.gemstone.gemfire.admin.GemFireHealth;
//import com.gemstone.gemfire.internal.*;
//import com.gemstone.gemfire.internal.admin.*;
import com.gemstone.gemfire.distributed.internal.*;
import java.io.*;
//import java.util.*;
import com.gemstone.gemfire.distributed.internal.membership.*;

/**
 * The response to fetching the health diagnosis.
 * @since 3.5
 */
public final class FetchHealthDiagnosisResponse extends AdminResponse {
  // instance variables
  String[] diagnosis;
  
  /**
   * Returns a <code>FetchHealthDiagnosisResponse</code> that will be returned to the
   * specified recipient.
   */
  public static FetchHealthDiagnosisResponse create(DistributionManager dm, InternalDistributedMember recipient, int id,
                                                    GemFireHealth.Health healthCode) {
    FetchHealthDiagnosisResponse m = new FetchHealthDiagnosisResponse();
    m.setRecipient(recipient);
    {
      HealthMonitor hm = dm.getHealthMonitor(recipient);
      if (hm.getId() == id) {
        m.diagnosis = hm.getDiagnosis(healthCode);
      }
    }
    return m;
  }

  public String[] getDiagnosis() {
    return this.diagnosis;
  }

  // instance methods
  public int getDSFID() {
    return FETCH_HEALTH_DIAGNOSIS_RESPONSE;
  }

  @Override  
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeStringArray(this.diagnosis, out);
  }

  @Override  
  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.diagnosis = DataSerializer.readStringArray(in);
  }

  @Override  
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append("FetchHealthDiagnosisResponse from ");
    sb.append(this.getRecipient());
    sb.append(" diagnosis=\"");
    for (int i = 0; i < diagnosis.length; i++) {
      sb.append(diagnosis[i]);
      if (i < diagnosis.length - 1) {
        sb.append(" ");
      }
    }
    sb.append("\"");
    return sb.toString();
  }
}
