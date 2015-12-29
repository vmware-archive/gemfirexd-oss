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
 * ArrayIterator.h
 *
 *  Created on: Mar 20, 2013
 *      Author: shankarh
 */

#ifndef ARRAYITERATOR_H_
#define ARRAYITERATOR_H_

namespace com
{
  namespace pivotal
  {
    namespace gemfirexd
    {
      namespace impl
      {
        /**
         * Simple iterator class for an array that encapsulates the start and
         * end so does not require a reference to original array (unlike STL
         * iterator that requires a reference to original collection for end()).
         */
        template<typename TIter>
        class ArrayIterator
        {

        private:
          const TIter* m_array;
          const TIter* m_end;

        public:
          ArrayIterator(const TIter* array, const int size) :
              m_array(array), m_end(array + size) {
          }
          ArrayIterator() :
              m_array(0), m_end(0) {

          }

          const TIter& operator*() const {
            return *m_array;
          }

          const TIter* operator->() const {
            return m_array;
          }

          bool operator++() {
            return ((++m_array) < m_end);
          }

          bool hasCurrent() {
            return (m_array < m_end);
          }
        };

      }
    }
  }
}

#endif /* ARRAYITERATOR_H_ */
