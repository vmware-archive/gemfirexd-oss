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

import com.gemstone.gemfire.internal.cache.xmlcache.RegionAttributesCreation;

/**
 * Provides version-dependent support for DeclarativeGenerator.
 */
public class DeclarativeGeneratorVersionHelper {

/** For GemFire7.0 and above, set concurrencyChecksEnabled (if defined)
 *
 *  @param regDef The RegionDefinition for the region to create.
 *  @param attr   The region attributes creation object to update
 *
 * @returns The updated RegionAttributesCreation instance
 */
public static RegionAttributesCreation setConcurrencyChecksEnabled(RegionDefinition regDef, RegionAttributesCreation attr) {
   if (regDef.getConcurrencyChecksEnabled() != null) {
      attr.setConcurrencyChecksEnabled(regDef.getConcurrencyChecksEnabled().booleanValue());
   }
   return attr;
}

}
