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

public class ValueHolderPrms extends BasePrms {

/** (boolean) Use extra object. If true, then the extraObject field of ValueHolder will
 *  will be used. This allows scaling up the amount of data being added/replaced/destroyed/
 *  invalidated. If false, the extraObject field remains null.
 */
public static Long useExtraObject;  


public static Long objectType;

// ================================================================================
static {
   BasePrms.setValues(ValueHolderPrms.class);
}

}
