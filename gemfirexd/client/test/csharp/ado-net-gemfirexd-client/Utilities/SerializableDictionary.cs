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
 Adapted from http://weblogs.asp.net/pwelter34/archive/2006/05/03/444961.aspx
 */

using System.Collections.Generic;
using System.Collections.Specialized;
using System.ComponentModel;
using System.Runtime.Serialization;
using System.Xml.Serialization;
using System.Windows;

namespace ado_net_sqlsvr_client.Utilities
{
    [XmlRoot("dictionary")]

    public class SerializableDictionary<TKey, TValue>
        : Dictionary<TKey, TValue>, IXmlSerializable, INotifyCollectionChanged, INotifyPropertyChanged
    {
        public SerializableDictionary()
            : base()
        {
        }
 
        public SerializableDictionary(IDictionary<TKey, TValue> dictionary)
            : base(dictionary)
        {
        }
 
        public SerializableDictionary(IEqualityComparer<TKey> comparer)
            : base(comparer)
        {
        }
 
        public SerializableDictionary(int capacity)
            : base(capacity)
        {
        }
 
        public SerializableDictionary(IDictionary<TKey, TValue> dictionary, IEqualityComparer<TKey> comparer)
            : base(dictionary, comparer)
        {
        }
 
        public SerializableDictionary(int capacity, IEqualityComparer<TKey> comparer)
            : base(capacity, comparer)
        {
        }
 
        protected SerializableDictionary(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }

        #region INotifyCollectionChanged

        event NotifyCollectionChangedEventHandler INotifyCollectionChanged.CollectionChanged
        {
            add { CollectionChanged += value; }
            remove { CollectionChanged -= value; }
        }

        protected virtual event NotifyCollectionChangedEventHandler CollectionChanged;

        #endregion INotifyCollectionChanged

        #region INotifyPropertyChanged

        event PropertyChangedEventHandler INotifyPropertyChanged.PropertyChanged
        {
            add { PropertyChanged += value; }
            remove { PropertyChanged -= value; }
        }

        protected virtual event PropertyChangedEventHandler PropertyChanged;

        #endregion INotifyPropertyChanged


        #region IXmlSerializable Members

        public System.Xml.Schema.XmlSchema GetSchema()
        {
            return null;
        }

        public void ReadXml(System.Xml.XmlReader reader)
        {
            XmlSerializer keySerializer = new XmlSerializer(typeof(TKey));
            XmlSerializer valueSerializer = new XmlSerializer(typeof(TValue));

            bool wasEmpty = reader.IsEmptyElement;

            reader.Read();

            if (wasEmpty)
                return;


            while (reader.NodeType != System.Xml.XmlNodeType.EndElement)
            {
                reader.ReadStartElement("item");
                reader.ReadStartElement("key");

                TKey key = (TKey)keySerializer.Deserialize(reader);

                reader.ReadEndElement();

                reader.ReadStartElement("value");

                TValue value = (TValue)valueSerializer.Deserialize(reader);

                reader.ReadEndElement();

                this.Add(key, value);

                reader.ReadEndElement();
                reader.MoveToContent();
            }

            reader.ReadEndElement();
        }

        public void WriteXml(System.Xml.XmlWriter writer)
        {
            XmlSerializer keySerializer = new XmlSerializer(typeof(TKey));
            XmlSerializer valueSerializer = new XmlSerializer(typeof(TValue));

            foreach (TKey key in this.Keys)
            {
                writer.WriteStartElement("item");
                writer.WriteStartElement("key");

                keySerializer.Serialize(writer, key);

                writer.WriteEndElement();
                writer.WriteStartElement("value");

                TValue value = this[key];

                valueSerializer.Serialize(writer, value);

                writer.WriteEndElement();
                writer.WriteEndElement();
            }
        }
        #endregion
    }
}
