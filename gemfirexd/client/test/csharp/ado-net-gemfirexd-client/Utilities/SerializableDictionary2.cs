/**
 Taken from http://www.dacris.com/blog/2010/07/31/c-serializable-dictionary-a-working-example/

 Copyright (c) Dacris Software Inc.

 Permission is hereby granted, free of charge, to any person obtaining a copy
 of this software and associated documentation files (the “Software”), to deal
 in the Software without restriction, including without limitation the rights
 to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 copies of the Software, and to permit persons to whom the Software is
 furnished to do so, subject to the following conditions:

 The above copyright notice and this permission notice shall be included in
 all copies or substantial portions of the Software.

 THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 THE SOFTWARE.

 */

using System;
using System.Runtime.Serialization;
using System.Xml;
using System.Xml.Serialization;
using System.Collections.Generic;
using System.Text;

namespace ado_net_gemfirexd_client.Utilities
{
    [Serializable()]
    public class SerializableDictionary2<TKey, TVal>
        : Dictionary<TKey, TVal>, IXmlSerializable, ISerializable
    {
        #region Constants
        private const string DictionaryNodeName = "Dictionary";
        private const string ItemNodeName = "Item";
        private const string KeyNodeName = "Key";
        private const string ValueNodeName = "Value";
        #endregion
        #region Constructors
        public SerializableDictionary2()
        {
        }

        public SerializableDictionary2(IDictionary<TKey, TVal> dictionary)
            : base(dictionary)
        {
        }

        public SerializableDictionary2(IEqualityComparer<TKey> comparer)
            : base(comparer)
        {
        }

        public SerializableDictionary2(int capacity)
            : base(capacity)
        {
        }

        public SerializableDictionary2(IDictionary<TKey, TVal> dictionary,
            IEqualityComparer<TKey> comparer) : base(dictionary, comparer)
        {
        }

        public SerializableDictionary2(int capacity, IEqualityComparer<TKey> comparer)
            : base(capacity, comparer)
        {
        }

        #endregion
        #region ISerializable Members

        protected SerializableDictionary2(SerializationInfo info, StreamingContext context)
        {
            int itemCount = info.GetInt32("ItemCount");
            for (int i = 0; i < itemCount; i++)
            {
                KeyValuePair<TKey, TVal> kvp = (KeyValuePair<TKey, TVal>)info.GetValue(
                    String.Format("Item{0}", i), typeof(KeyValuePair<TKey, TVal>));
                this.Add(kvp.Key, kvp.Value);
            }
        }

        void ISerializable.GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue("ItemCount", this.Count);
            int itemIdx = 0;
            foreach (KeyValuePair<TKey, TVal> kvp in this)
            {
                info.AddValue(String.Format("Item{0}", itemIdx), kvp,
                    typeof(KeyValuePair<TKey, TVal>));
                itemIdx++;
            }
        }

        #endregion
        #region IXmlSerializable Members

        void IXmlSerializable.WriteXml(System.Xml.XmlWriter writer)
        {
            //writer.WriteStartElement(DictionaryNodeName);
            foreach (KeyValuePair<TKey, TVal> kvp in this)
            {
                writer.WriteStartElement(ItemNodeName);
                writer.WriteStartElement(KeyNodeName);
                KeySerializer.Serialize(writer, kvp.Key);
                writer.WriteEndElement();
                writer.WriteStartElement(ValueNodeName);
                ValueSerializer.Serialize(writer, kvp.Value);
                writer.WriteEndElement();
                writer.WriteEndElement();
            }
            //writer.WriteEndElement();
        }

        void IXmlSerializable.ReadXml(System.Xml.XmlReader reader)
        {
            if (reader.IsEmptyElement)
            {
                return;
            }

            // Move past container
            if (!reader.Read())
            {
                throw new XmlException("Error in Deserialization of Dictionary");
            }

            //reader.ReadStartElement(DictionaryNodeName);
            while (reader.NodeType != XmlNodeType.EndElement)
            {
                reader.ReadStartElement(ItemNodeName);
                reader.ReadStartElement(KeyNodeName);
                TKey key = (TKey)KeySerializer.Deserialize(reader);
                reader.ReadEndElement();
                reader.ReadStartElement(ValueNodeName);
                TVal value = (TVal)ValueSerializer.Deserialize(reader);
                reader.ReadEndElement();
                reader.ReadEndElement();
                this.Add(key, value);
                reader.MoveToContent();
            }
            //reader.ReadEndElement();

            reader.ReadEndElement(); // Read End Element to close Read of containing node
        }

        System.Xml.Schema.XmlSchema IXmlSerializable.GetSchema()
        {
            return null;
        }

        #endregion
        #region Private Properties
        protected XmlSerializer ValueSerializer
        {
            get
            {
                if (valueSerializer == null)
                {
                    valueSerializer = new XmlSerializer(typeof(TVal));
                }
                return valueSerializer;
            }
        }

        private XmlSerializer KeySerializer
        {
            get
            {
                if (keySerializer == null)
                {
                    keySerializer = new XmlSerializer(typeof(TKey));
                }
                return keySerializer;
            }
        }
        #endregion
        #region Private Members
        private XmlSerializer keySerializer = null;
        private XmlSerializer valueSerializer = null;
        #endregion
    }
}
