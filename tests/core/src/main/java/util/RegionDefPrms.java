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

import hydra.BasePrms;

/**
 * Hydra parameters that let a test writer declaratively specify
 * configurations for GemFire cache Regions.  An example of the
 * declarative configuration format can be found in
 * <code>$JTESTS/util/regionDef.inc</code>.
 *
 * @author Lynn Gallinat
 * @since 3.0
 */
public class RegionDefPrms extends BasePrms {

/** (String) Used for specifying valid combinations of region
 *  attributes. Some region attribute combinations are illegal. This
 *  specifies legal combinations to use for a test. See
 *  util.regionDef.inc for how to specify.
 */
public static Long regionSpecs;

/** (String) Used for choosing a valid combinations of region
 *  attributes that is specific for this test. This is one of the spec
 *  names defined in regionSpecs.  
 */
public static Long VMRegionSpecName;

/** (String) Used for choosing a valid combinations of reliability
 *  attributes that is specific for this test. This is one of the spec
 *  names defined in regionSpecs.
 */
public static Long reliabilitySpecName;

/** (String) Indicates how to use the region specs defined in this
 *  test. A call to {@link RegionDefinition#createRegionDefinition()}
 *  will consider this setting. Can be one of:
 *
 *    <code>useAnyRegionSpec</code> - a potentially and randomly 
 *    different region definition will be returned each time a region 
 *    definition is fetched.
 *
 *    <Code>useOneRegionSpec</codE> - the same region definition will
 *    be returned each time a region definition is fetched.
 *
 *    <Code>useFixedSequence</codE> - each fetch of a region 
 *    definition will return a randomly different region definition, 
 *    but each VM will fetch the same sequence of random region
 *    definitions. In other words, a random sequence of region definitions
 *    is returned, but it is the identical sequence for each VM.
 *
 * Do NOT use <code>oneof</code> on this parameter.
 */
public static Long regionDefUsage;

// ================================================================================
static {
   BasePrms.setValues(RegionDefPrms.class);
}

}
