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
package com.pivotal.pxf.resolvers;

import java.util.ArrayList;
import java.util.List;

import com.pivotal.pxf.api.OneField;
import com.pivotal.pxf.api.OneRow;
import com.pivotal.pxf.api.io.DataType;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.gfxd.TestRecordReader;
import com.pivotal.pxf.gfxd.util.TestDataHelper;
import com.pivotal.pxf.plugins.gemfirexd.GemFireXDResolver;

public class TestGemFireXDResolver extends GemFireXDResolver {

  public TestGemFireXDResolver(InputData metaData) {
    super(metaData);
  }

  public List<OneField> GetData(OneRow row) throws Exception {
    List<OneField> result = new ArrayList<OneField>();
    Object[] fields = TestDataHelper.deserializeArray(getRegionName(),
        ((TestRecordReader.ValueClass) row.getData()).getValue());
    
    for (Object field : fields) {
      if (field instanceof Integer) {
        result.add(new OneField(DataType.INTEGER.getOID(), field));
      } else if (field instanceof String) {
        result.add(new OneField(DataType.VARCHAR.getOID(), field));
      } else if (field instanceof Boolean) {
        result.add(new OneField(DataType.BOOLEAN.getOID(), field));
      } else {
        throw new Exception("Column type not supported " + field.getClass());
      }
    }
    return result;
  }
}
