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
package cacheperf.comparisons.security;

import hydra.BasePrms;

/**
 * A class used to store parameters for the dummy scheme.
 */
public class DummyUserPasswordPrms extends BasePrms {

  public static final String DEFAULT_USERNAME = "writer1";
  public static final String DEFAULT_PASSWORD = "writer1";
  public static final String DEFAULT_AUTHZ_XML_URI = "$JTESTS/lib/authz-dummy.xml";
  public static Long username;
  public static Long password;
  public static Long authzXmlUri;

  static {
    setValues(DummyUserPasswordPrms.class);
  }
}
