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
package com.gemstone.gemfire.cache.query.data;

import com.gemstone.gemfire.pdx.PdxInstance;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.pdx.PdxReader;
import com.gemstone.gemfire.pdx.PdxSerializable;
import com.gemstone.gemfire.pdx.PdxWriter;

import java.io.IOException;

public class PdxTestObject2 implements PdxSerializable {
    public int _id;
    public static int numInstance = 0;
    
    public PdxTestObject2(){
      numInstance++;
    }
    
    public PdxTestObject2(int id){
      this._id = id;
      numInstance++;
    }
    
    public int getId() {
      return this._id;
    }
    
    public void toData(PdxWriter out) {
      out.writeInt("id", this._id);
    }
    
    public void fromData(PdxReader in) {
      this._id = in.readInt("id");
    }
}
