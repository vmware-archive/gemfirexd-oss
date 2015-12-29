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
    class GFXDServerGroupConfig : ConfigurationSection
    {
        [ConfigurationProperty("settings")]
        public GFXDServerGroupSettingsCollection GFXDServerGroupSettings
        {
            get
            {
                return ((GFXDServerGroupSettingsCollection)(base["settings"]));
            }
        }
    }

    [ConfigurationCollection(typeof(GFXDServerGroupSetting))]
    class GFXDServerGroupSettingsCollection : ConfigurationElementCollection
    {
        protected override ConfigurationElement CreateNewElement()
        {
            return new GFXDServerGroupSetting();
        }

        protected override object GetElementKey(ConfigurationElement element)
        {
            return ((GFXDServerGroupSetting)(element)).Key;
        }

        public GFXDServerGroupSetting this[int idx]
        {
            get
            {
                return (GFXDServerGroupSetting)BaseGet(idx);
            }
        }

        public GFXDServerGroupSetting this[String key]
        {
            get
            {
                return (GFXDServerGroupSetting)BaseGet(key);
            }
        }
    }

    class GFXDServerGroupSetting : ConfigurationElement
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
