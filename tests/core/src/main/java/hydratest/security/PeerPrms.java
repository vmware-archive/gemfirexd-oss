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
package hydratest.security;

import hydra.BasePrms;

public class PeerPrms extends BasePrms {

  public static final String PEER_NO_DEFAULT_NO_SET_NO_TASK_NAME = "security-peer-no-default-no-set-no-task";
  public static final String PEER_HAS_DEFAULT_NO_SET_NO_TASK_NAME = "security-peer-has-default-no-set-no-task";
  public static final String PEER_NO_DEFAULT_HAS_SET_NO_TASK_NAME = "security-peer-no-default-has-set-no-task";
  public static final String PEER_HAS_DEFAULT_HAS_SET_NO_TASK_NAME = "security-peer-has-default-has-set-no-task";
  public static final String PEER_NO_DEFAULT_NO_SET_HAS_TASK_NAME = "security-peer-no-default-no-set-has-task";
  public static final String PEER_NO_DEFAULT_HAS_SET_HAS_TASK_NAME = "security-peer-no-default-has-set-has-task";
  public static final String PEER_HAS_DEFAULT_NO_SET_HAS_TASK_NAME = "security-peer-has-default-no-set-has-task";
  public static final String PEER_HAS_DEFAULT_HAS_SET_HAS_TASK_NAME = "security-peer-has-default-has-set-has-task";

  public static final String DEFAULT_PEER_HAS_DEFAULT_NO_SET_NO_TASK = "PeerDefaultVal";
  public static final String DEFAULT_PEER_HAS_DEFAULT_HAS_SET_NO_TASK = "PeerDefaultVal";
  public static final String DEFAULT_PEER_HAS_DEFAULT_NO_SET_HAS_TASK = "PeerDefaultVal";
  public static final String DEFAULT_PEER_HAS_DEFAULT_HAS_SET_HAS_TASK = "PeerDefaultVal";

  public static Long peerNoDefaultNoSetNoTask;
  public static Long peerHasDefaultNoSetNoTask;
  public static Long peerNoDefaultHasSetNoTask;
  public static Long peerHasDefaultHasSetNoTask;
  public static Long peerNoDefaultNoSetHasTask;
  public static Long peerNoDefaultHasSetHasTask;
  public static Long peerHasDefaultNoSetHasTask;
  public static Long peerHasDefaultHasSetHasTask;

  static {
    setValues(PeerPrms.class);
  }
}
