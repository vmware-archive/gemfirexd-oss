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
 
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace GemFireXDDBI.DBObjects
{
    /// <summary>
    /// Represents a connection string setting object
    /// </summary>
    class DbConn
    {
        public String Name { get; set; }
        public String ConnString { get; set; }
        public String Provider { get; set; }

        public DbConn(String name, String connString, String provider)
        {
            Name = name;
            ConnString = connString;
            Provider = provider;
        }
    }
}
