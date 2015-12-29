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
package com.gemstone.gemfire.internal.cache.rollingupgrade;

import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;

/**
 */
public class ChildFirstClassLoader extends ClassLoader 
{
    private ExposeFindClassURLClassLoader childClassLoader;

    private class ExposeFindClassURLClassLoader extends URLClassLoader
    {
        private ClassLoader parentLoader;

        public ExposeFindClassURLClassLoader( URL[] urls, ClassLoader parentLoader )
        {
            super(urls, null);
            this.parentLoader = parentLoader;
        }

        //Allow us to access findClass
        public Class<?> findClass(String name) throws ClassNotFoundException
        {
          synchronized(this) {
          Class c = findLoadedClass(name);
          if (c != null) return c;
            try {
              //Look into the this class loader first.
              c = super.findClass(name);
              return c;
            }
            catch( ClassNotFoundException e )
            {
              //Look for the class in the parent
              if (parentLoader != null) {
                return parentLoader.loadClass(name);
              }
              else {
                throw e;
              }
            }
          }
        }
    }

    public ChildFirstClassLoader(URL[] urls, ClassLoader parentLoader)
    {
        super(parentLoader);
        childClassLoader = new ExposeFindClassURLClassLoader( urls, parentLoader );
    }

    @Override
    protected synchronized Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException
    {
        return childClassLoader.findClass(name); 
    }
    
    
    public URL getResource(String name) {
      return childClassLoader.getResource(name);
    }
    
    public InputStream getResourceAsStream(String name) {
      return childClassLoader.getResourceAsStream(name);
  }
}