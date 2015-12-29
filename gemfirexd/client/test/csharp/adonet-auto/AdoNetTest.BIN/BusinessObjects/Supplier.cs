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
using System.Data;
using System.Text;

namespace AdoNetTest.BIN.BusinessObjects
{
    /// <summary>
    /// Product supplier information
    /// </summary>
    public class Supplier : BusinessObject
    {
        public long SupplierId 
        { 
            get; 
            set; 
        }
        public string Name 
        { 
            get; 
            set; 
        }

        private Address address;
        public Address Address 
        { 
            get
            {
                if(address == null)
                    address = new Address();

                return address;
            }
            set
            {
                address = value;
            }
        }
        public string Phone 
        { 
            get; 
            set; 
        }
        public string Email 
        { 
            get;
            set; 
        }


        public Supplier()
            : this(0, string.Empty, 0, string.Empty,
            string.Empty)
        {
        }

        public Supplier(long supplierid)
            : this()
        {
            SupplierId = supplierid;
        }

        public Supplier(long supplierId, string name, long addressId, 
            string phone, string email)
        {
            SupplierId = supplierId;
            Name = name;
            Address = address;
            Phone = phone;
            Email = email;
        }

        public Supplier(DataRow row)
        {
            SupplierId = long.Parse(row[0].ToString());
            Name = row[1].ToString();
            Address.AddressId = long.Parse(row[2].ToString());
            Phone = row[3].ToString();
            Email = row[4].ToString();
        }

        public override bool Equals(object obj)
        {
            if (!(obj is Supplier))
                return false;

            Supplier supplier = (Supplier)obj;

            return (this.SupplierId == supplier.SupplierId
                && this.Name == supplier.Name
                && this.Address.AddressId == supplier.Address.AddressId
                && this.Phone == supplier.Phone
                && this.Email == supplier.Email);
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }

        public override string ToString()
        {
            return String.Format(
                "{0}, {1}, {2}, {3}, {4}\n",
                this.SupplierId, this.Name, this.Address.AddressId,
                this.Phone, this.Email);
        }
    }
}
