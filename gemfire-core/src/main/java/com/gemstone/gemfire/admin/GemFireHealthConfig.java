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
package com.gemstone.gemfire.admin;

/**
 * Provides configuration information relating to all of the
 * components of a GemFire distributed system.
 *
 * @author David Whitlock
 *
 * @since 3.5
 * @deprecated as of 7.0 use the {@link com.gemstone.gemfire.management} package instead
 * */
public interface GemFireHealthConfig
  extends MemberHealthConfig, CacheHealthConfig {

  /** The default number of seconds between assessments of the health
   * of the GemFire components. */
  public static final int DEFAULT_HEALTH_EVALUATION_INTERVAL = 30;

  //////////////////////  Instance Methods  //////////////////////

  /**
   * Returns the name of the host to which this configuration
   * applies.  If this is the "default" configuration, then
   * <code>null</code> is returned.
   *
   * @see GemFireHealth#getGemFireHealthConfig
   */
  public String getHostName();

  /**
   * Sets the number of seconds between assessments of the health of
   * the GemFire components.
   */
  public void setHealthEvaluationInterval(int interval);

  /**
   * Returns the number of seconds between assessments of the health of
   * the GemFire components.
   */
  public int getHealthEvaluationInterval();

}
