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
 * Library.h
 *
 *  Created on: Mar 20, 2013
 *      Author: shankarh
 */

#ifndef LIBRARY_H_
#define LIBRARY_H_

#include "DriverBase.h"

#ifdef _WINDOWS
// for GC_new_thread()
extern "C"
{
  BOOL WINAPI GC_DllMain(HINSTANCE inst, ULONG reason, LPVOID reserved);
}
#endif

/**
 * Loads a library in a platform specific manner and get function
 * handles in the library.
 */
namespace com
{
  namespace pivotal
  {
    namespace gemfirexd
    {
      namespace native
      {
        class Library
        {
        private:
          void* m_libraryHandle;

        public:
          /**
           * Initialize and load a library with given path.
           *
           * @param path the path to the library file
           * @exception SQLException if the library could not be
           * loaded for some reason then this exception is thrown
           */
          Library(const char* path);

          /**
           * Destructor for the Library object that clears reference to any
           * loaded library allowing the library to be unloaded when all
           * references are freed.
           */
          ~Library();

          /**
           * Get handle to a function in this library.
           *
           * @param name the name of the function to be loaded
           * @exception SQLException if no such function exists
           * or could not be loaded
           */
          void* getFunction(const char* name);
        };

      }
    }
  }
}
#endif /* LIBRARY_H_ */
