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

package hydra;

import java.io.Serializable;

/**
 *  A HydraThreadGroupInfo is used to pass threadgroup information to a new
 *  client when it registers its existence.
 */
public class HydraThreadGroupInfo implements Serializable {

  //////////////////////////////////////////////////////////////////////////////
  ////    INSTANCE FIELDS                                                   ////
  //////////////////////////////////////////////////////////////////////////////

  /**
   *  The name of the threadgroup.
   */
  private String tgname;

  /**
   *  The logical ID used to identify the thread within its threadgroup.
   */
  private int tgid = -1;

  //////////////////////////////////////////////////////////////////////////////
  ////    CONSTRUCTORS                                                      ////
  //////////////////////////////////////////////////////////////////////////////

  protected HydraThreadGroupInfo( String tgname, int tgid ) {
    this.tgname = tgname;
    this.tgid = tgid;
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    ACCESSORS                                                         ////
  //////////////////////////////////////////////////////////////////////////////

  public String getThreadGroupName() {
    return this.tgname;
  }

  public int getThreadGroupId() {
    return this.tgid;
  }
}
