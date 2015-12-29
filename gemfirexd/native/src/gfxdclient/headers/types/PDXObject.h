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
 * PDXObject.h
 *
 *      Author: swale
 */

#ifndef PDXOBJECT_H_
#define PDXOBJECT_H_

#include "SQLException.h"
#include "WrapperBase.h"

namespace com
{
  namespace pivotal
  {
    namespace gemfirexd
    {
      namespace client
      {
        namespace types
        {

          class PDXObject: public WrapperBase<thrift::PDXObject>
          {
          private:
            PDXObject(thrift::PDXObject* pdx) :
                WrapperBase<thrift::PDXObject>(pdx, false)
            {
            }

            friend class com::pivotal::gemfirexd::client::Row;
            friend class com::pivotal::gemfirexd::client::Parameters;
            friend class com::pivotal::gemfirexd::client::UpdatableRow;

          public:
            PDXObject();

            PDXObject(const PDXObject& other) :
                WrapperBase<thrift::PDXObject>(other)
            {
            }

            PDXObject& operator=(const PDXObject& other)
            {
              WrapperBase<thrift::PDXObject>::operator=(other);
              return *this;
            }
          };

        } /* namespace types */
      } /* namespace client */
    } /* namespace gemfirexd */
  } /* namespace pivotal */
} /* namespace com */

#endif /* PDXOBJECT_H_ */
