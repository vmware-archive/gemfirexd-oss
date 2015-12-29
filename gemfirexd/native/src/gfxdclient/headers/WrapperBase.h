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
 * WrapperBase.h
 *
 *      Author: swale
 */

#ifndef WRAPPERBASE_H_
#define WRAPPERBASE_H_

#include "Utils.h"

namespace com
{
  namespace pivotal
  {
    namespace gemfirexd
    {
      namespace client
      {

        template<typename T>
        class WrapperBase
        {
        protected:
          T* m_p;
          bool m_isOwner;

          void cleanup()
          {
            if (m_isOwner && m_p != NULL) {
              delete m_p;
              m_p = NULL;
            }
          }

        public:
          WrapperBase(T* p, bool isOwner) throw () :
              m_p(p), m_isOwner(isOwner)
          {
          }

          WrapperBase(const WrapperBase& other) throw () :
              m_p(other.m_p), m_isOwner(false)
          {
          }

          WrapperBase& operator=(const WrapperBase& other) throw ()
          {
            m_p = other.m_p;
            m_isOwner = false;
            return *this;
          }

          bool isOwner() const throw ()
          {
            return m_p;
          }

          void reset(T* p, bool isOwner = false) throw ()
          {
            m_p = p;
            m_isOwner = isOwner;
          }

          virtual ~WrapperBase() throw ()
          {
            // destructor should *never* throw an exception
            try {
              cleanup();
            } catch (const SQLException& sqle) {
              Utils::handleExceptionInDestructor(typeid(T).name(), sqle);
            } catch (const std::exception& stde) {
              Utils::handleExceptionInDestructor(typeid(T).name(), stde);
            } catch (...) {
              Utils::handleExceptionInDestructor(typeid(T).name());
            }
          }
        };

      } /* namespace client */
    } /* namespace gemfirexd */
  } /* namespace pivotal */
} /* namespace com */

#endif /* WRAPPERBASE_H_ */
