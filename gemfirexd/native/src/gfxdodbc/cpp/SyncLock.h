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
 * SyncLock.h
 *
 *  Created on: Mar 20, 2013
 *      Author: shankarh
 */

#ifndef SYNCLOCK_H_
#define SYNCLOCK_H_

#include "DriverBase.h"

namespace com
{
  namespace pivotal
  {
    namespace gemfirexd
    {
      namespace native
      {
        // synchronization functions and class with a Guard class that
        // acquires/releases the lock in constructor/destructor
        class SyncLock
        {
        private:
          LOCK m_lock;
          int m_error;

        public:
          class Guard
          {
          private:
            SyncLock& m_sync;
            int m_state;

          public:
            inline Guard(SyncLock& sync) :
                m_sync(sync), m_state(-1) {
            }
            ~Guard();

            SQLRETURN acquire();
          };

          SyncLock();
          ~SyncLock();

          friend class Guard;
        };
      }
    }
  }
}

#endif /* SYNCLOCK_H_ */
