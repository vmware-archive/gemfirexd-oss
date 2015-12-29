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
package partitionedRegionQuerying;

import java.io.Serializable;

/**
 * A simple stock Portfolio. Instances of <code>Portfolio</code>
 * can be stored in a GemFire <code>Region</code> and their contents can be
 * queried using the GemFire query service.
 * 
 * <P>
 * 
 * This class is <code>Serializable</code> because we want it to be distributed
 * to multiple members of a distributed system.
 * 
 */

public class Portfolio implements Serializable {
  private String id;
  private String status;
  private String secId;
  public static String secIds[] = { "SUN", "IBM", "YHOO", "GOOG", "MSFT",
      "AOL", "APPL", "ORCL", "SAP", "DELL", "RHAT", "NOVL", "HP" };

  public Portfolio(int i) {
    id = "" + i;
    status = i % 2 == 0 ? "active" : "inactive";
    secId = secIds[i % secIds.length];
  }

  public String getId() {
    return id;
  }

  public String getStatus() {
    return status;
  }

  public String getSecId() {
    return secId;
  }

  public String toString() {
    return "Portfolio [id = " + id + " status = " + status + " secId = " + secId + "]";
  }

}
