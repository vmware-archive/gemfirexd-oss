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
 * GFXDDescriptor.hpp
 *
 *  Created on: Apr 17, 2013
 *      Author: shankarh
 */

#ifndef GFXDDESCRIPTOR_H_
#define GFXDDESCRIPTOR_H_

#include "DriverBase.h"

namespace com
{
  namespace pivotal
  {
    namespace gemfirexd
    {

      /**
       * Contains the ODBC descriptor handles for
       * Application Parameter Descriptor(APD),
       * Implementation Parameter Descriptor(IPD),
       * Application Row Descriptor(ARD),
       * Implementation Row Descriptor(IPD).
       * Encapsulates a JDBC parameter metadata and the resultsetmetadata.
       */
      class GFXDDescriptor : public GFXDHandleBase
      {
      private:
        int m_descType;
        /** let GFXDStatement access the private fields */
        friend class GFXDStatement;

        /**
         * Constructor for a GFXDConnection given handle to GFXDEnvironment.
         */
        GFXDDescriptor(int descType);

        ~GFXDDescriptor();

      public:
        static SQLRETURN newDescriptor(int descType, GFXDDescriptor*& descRef);

        static SQLRETURN freeDescriptor(GFXDDescriptor* desc);

      };
    }
  }
}

#endif /* GFXDDESCRIPTOR_H_ */
