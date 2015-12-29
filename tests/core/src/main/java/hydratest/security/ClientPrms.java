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

public class ClientPrms extends BasePrms {

  public static final String CLIENT_NO_DEFAULT_NO_SET_NO_TASK_NAME = "security-client-no-default-no-set-no-task";
  public static final String CLIENT_HAS_DEFAULT_NO_SET_NO_TASK_NAME = "security-client-has-default-no-set-no-task";
  public static final String CLIENT_NO_DEFAULT_HAS_SET_NO_TASK_NAME = "security-client-no-default-has-set-no-task";
  public static final String CLIENT_HAS_DEFAULT_HAS_SET_NO_TASK_NAME = "security-client-has-default-has-set-no-task";
  public static final String CLIENT_NO_DEFAULT_NO_SET_HAS_TASK_NAME = "security-client-no-default-no-set-has-task";
  public static final String CLIENT_NO_DEFAULT_HAS_SET_HAS_TASK_NAME = "security-client-no-default-has-set-has-task";
  public static final String CLIENT_HAS_DEFAULT_NO_SET_HAS_TASK_NAME = "security-client-has-default-no-set-has-task";
  public static final String CLIENT_HAS_DEFAULT_HAS_SET_HAS_TASK_NAME = "security-client-has-default-has-set-has-task";

  public static final String DEFAULT_CLIENT_HAS_DEFAULT_NO_SET_NO_TASK = "ClientDefaultVal";
  public static final String DEFAULT_CLIENT_HAS_DEFAULT_HAS_SET_NO_TASK = "ClientDefaultVal";
  public static final String DEFAULT_CLIENT_HAS_DEFAULT_NO_SET_HAS_TASK = "ClientDefaultVal";
  public static final String DEFAULT_CLIENT_HAS_DEFAULT_HAS_SET_HAS_TASK = "ClientDefaultVal";

  public static Long clientNoDefaultNoSetNoTask;
  public static Long clientHasDefaultNoSetNoTask;
  public static Long clientNoDefaultHasSetNoTask;
  public static Long clientHasDefaultHasSetNoTask;
  public static Long clientNoDefaultNoSetHasTask;
  public static Long clientNoDefaultHasSetHasTask;
  public static Long clientHasDefaultNoSetHasTask;
  public static Long clientHasDefaultHasSetHasTask;

  static {
    setValues(ClientPrms.class);
  }
}
