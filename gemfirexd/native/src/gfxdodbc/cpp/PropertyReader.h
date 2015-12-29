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

/**
 * PropertyReader.h
 *
 *  Created on: Mar 20, 2013
 *      Author: shankarh
 */

#ifndef PROPERTYREADER_H_
#define PROPERTYREADER_H_

#include "DriverBase.h"

namespace com
{
  namespace pivotal
  {
    namespace gemfirexd
    {
      namespace impl
      {
        template<typename CHAR_TYPE>
        class PropertyReader
        {
        public:
          virtual void init(const CHAR_TYPE* inputRef, SQLINTEGER inputLen,
              void* propData) = 0;

          virtual SQLRETURN read(std::string& outPropName,
              std::string& outPropValue, GFXDHandleBase* handle) = 0;

          virtual ~PropertyReader() {
          }
        };
      }
    }
  }
}

#endif /* PROPERTYREADER_H_ */
