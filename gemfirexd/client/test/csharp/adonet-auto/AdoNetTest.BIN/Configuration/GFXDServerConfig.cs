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
using System.Configuration;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace AdoNetTest.BIN.Configuration
{
    class GFXDServerConfig : ConfigurationSection
    {
        [ConfigurationProperty("settings")]
        public GFXDServerSettingsCollection GFXDServerSettings
        {
            get
            {
                return ((GFXDServerSettingsCollection)(base["settings"]));
            }
        }
    }

    [ConfigurationCollection(typeof(GFXDServerSetting))]
    class GFXDServerSettingsCollection : ConfigurationElementCollection
    {
        protected override ConfigurationElement CreateNewElement()
        {
            return new GFXDServerSetting();
        }

        protected override object GetElementKey(ConfigurationElement element)
        {
            return ((GFXDServerSetting)(element)).Key;
        }

        public GFXDServerSetting this[int idx]
        {
            get
            {
                return (GFXDServerSetting)BaseGet(idx);
            }
        }

        public GFXDServerSetting this[String key]
        {
            get
            {
                return (GFXDServerSetting)BaseGet(key);
            }
        }
    }

    class GFXDServerSetting : ConfigurationElement
    {
        [ConfigurationProperty("key", DefaultValue = "", IsKey = true, IsRequired = true)]
        public string Key
        {
            get
            {
                return ((string)(base["key"]));
            }
            set
            {
                base["key"] = value;
            }
        }

        [ConfigurationProperty("value", DefaultValue = "", IsKey = false, IsRequired = false)]
        public string Value
        {
            get
            {
                return ((string)(base["value"]));
            }
            set
            {
                base["value"] = value;
            }
        }
    }
}
    