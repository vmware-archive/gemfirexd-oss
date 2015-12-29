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
    /// Encapsulates customer information and related orders
    /// </summary>
    public class Customer : BusinessObject
    {
        public long CustomerId 
        { 
            get; 
            set; 
        }
        public string FirstName 
        { 
            get; 
            set; 
        }
        public string LastName 
        { 
            get; 
            set; 
        }

        private Address address;
        public Address Address 
        {
            get
            {
                if (address == null)
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
        public DateTime LastOrderDate 
        { 
            get; 
            set; 
        }

        private IList<Order> orders;
        public IList<Order> Orders 
        {
            get
            {
                if (orders == null)
                    orders = new List<Order>();

                return orders;
            }
            set
            {
                orders = value;
            }
        }

        public Customer()
        {
            CustomerId = 0;
            FirstName = String.Empty;
            LastName = String.Empty;
            Address = new Address();
            Phone = String.Empty;
            Email = String.Empty;
            LastOrderDate = DateTime.Today;
        }

        public Customer(long customerId)
            : this()
        {
            CustomerId = customerId;
        }

        public Customer(long customerId, string firstName, string lastName,
            long addressId, string phone, string email, DateTime lastOrderDate)
            : this()
        {
            CustomerId = customerId;
            FirstName = firstName;
            LastName = lastName;
            Address.AddressId = addressId;
            Phone = phone;
            Email = email;
            LastOrderDate = LastOrderDate;
        }

        public Customer(DataRow row)
            : this()
        {
            CustomerId = long.Parse(row[0].ToString());
            FirstName = row[1].ToString();
            LastName = row[2].ToString();
            Address.AddressId = long.Parse(row[3].ToString());
            Phone = row[4].ToString();
            Email = row[5].ToString();
            LastOrderDate = DateTime.Parse(row[6].ToString());
        }

        public override bool Equals(object obj)
        {
            if (!(obj is Customer))
                return false;

            Customer customer = (Customer)obj;

            return (this.CustomerId == customer.CustomerId
                && this.FirstName == customer.FirstName
                && this.LastName == customer.LastName
                && this.Address.AddressId == customer.Address.AddressId
                && this.Phone == customer.Phone
                && this.Email == customer.Email
                && this.LastOrderDate == customer.LastOrderDate);
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }

        public override string ToString()
        {
            return String.Format(
                "{0}, {1}, {2}, {3}, {4}, {5}, {6}\n",
                this.CustomerId, this.FirstName, this.LastName, this.Address.AddressId,
                this.Phone, this.Email, this.LastOrderDate);
        }
    }
}
