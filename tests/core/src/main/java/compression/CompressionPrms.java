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
package compression;

import hydra.BasePrms;

/**
 * The CompressionPrms class... </p>
 *
 * @author mpriest
 * @see ?
 * @since 7.x
 */
public class CompressionPrms extends BasePrms{

  static {
    BasePrms.setValues(CompressionPrms.class);
  }

  public static Long nbrOfEmptyAccessors;
  public static int getNbrOfEmptyAccessors() {
    Long key = nbrOfEmptyAccessors;
    int value = tasktab().intAt(key, tab().intAt(key, 0));
    return value;
  }

  public static Long nbrOfCompressedRegions;
  public static int getNbrOfCompressedRegions() {
    Long key = nbrOfCompressedRegions;
    int value = tasktab().intAt(key, tab().intAt(key, 0));
    return value;
  }

}
