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
 * ParameterDescriptor.h
 *
 *      Author: swale
 */

#ifndef PARAMETERDESCRIPTOR_H_
#define PARAMETERDESCRIPTOR_H_

#include "ColumnDescriptorBase.h"

namespace com
{
  namespace pivotal
  {
    namespace gemfirexd
    {
      namespace client
      {

        namespace ParameterMode
        {
          enum type
          {
            UNKNOWN = 0,
            IN = 1,
            INOUT = 2,
            OUT = 4
          };
        }

        class ParameterDescriptor: public ColumnDescriptorBase
        {
        private:
          ParameterDescriptor(thrift::ColumnDescriptor& descriptor,
              const uint32_t parameterIndex) :
              ColumnDescriptorBase(descriptor, parameterIndex)
          {
          }

          friend class PreparedStatement;

        public:
          ParameterMode::type getParameterMode() const throw()
          {
            int16_t flags = m_descriptor.descFlags;
            if ((flags & thrift::g_gfxd_constants.PARAMETER_MODE_IN)) {
              return ParameterMode::IN;
            }
            else if ((flags & thrift::g_gfxd_constants.PARAMETER_MODE_INOUT)) {
              return ParameterMode::INOUT;
            }
            if ((flags & thrift::g_gfxd_constants.PARAMETER_MODE_OUT)) {
              return ParameterMode::OUT;
            }
            else {
              return ParameterMode::UNKNOWN;
            }
          }

          ~ParameterDescriptor() throw ()
          {
          }
        };

      } /* namespace client */
    } /* namespace gemfirexd */
  } /* namespace pivotal */
} /* namespace com */

#endif /* PARAMETERDESCRIPTOR_H_ */
