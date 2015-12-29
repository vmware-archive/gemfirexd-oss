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
using System.IO;
using System.Linq;
using System.Text;
using System.Xml.Serialization;


namespace ado_net_gemfirexd_client.ViewModels
{
    public abstract class BaseViewModel<T>   : ObservableObject<T>
    {
        /// <summary>
        /// ViewTitle private accessor
        /// </summary>
        private string viewTitle;
        /// <summary>
        /// Gets or sets the view title.
        /// </summary>
        /// <value>The view title.</value>
        public string ViewTitle
        {
            get { return viewTitle; }
            set
            {
                if (viewTitle == value)
                {
                    return;
                }
                viewTitle = value;
                OnPropertyChanged(vm => this.ViewTitle);
            }
        }

        //private string SerializeJobData()
        //{
        //    List<DataItem> tempdataitems = new List<DataItem>(myDictionary.Count);

        //    foreach (string key in dataitems.Keys)
        //    {
        //        tempdataitems.Add(new DataItem(key, dataitems[key].ToString()));
        //    }

        //    XmlSerializer serializer = new XmlSerializer(typeof(List<DataItem>));
        //    StringWriter sw = new StringWriter();
        //    XmlSerializerNamespaces ns = new XmlSerializerNamespaces();
        //    ns.Add("", "");

        //    serializer.Serialize(sw, tempdataitems, ns);
        //    return sw.ToString();
        //}

        //private void DeserializeData(string RawData)
        //{
        //    Dictionary<int, string> myDictionary = new Dictionary<int, string>();

        //    XmlSerializer xs = new XmlSerializer(typeof(List<DataItem>));
        //    StringReader sr = new StringReader(RawData);

        //    List<DataItem> templist = (List<DataItem>)xs.Deserialize(sr);

        //    foreach (DataItem di in templist)
        //    {
        //        myDictionary.Add(di.Key, di.Value);
        //    }
        //}  
    }
}
