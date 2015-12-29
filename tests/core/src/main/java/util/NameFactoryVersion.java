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
package util;

import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * Provides version support for NameFactory.
 */
public class NameFactoryVersion {

  protected static String getListenerNamePrefix() {
    return LocalizedStrings.LISTENER_PREFIX.toLocalizedString();
  }
  protected static String getObjectNamePrefix() {
    return LocalizedStrings.OBJECT_PREFIX.toLocalizedString();
  }
  protected static String getRegionNamePrefix() {
    return LocalizedStrings.REGION_PREFIX.toLocalizedString();
  }
}
