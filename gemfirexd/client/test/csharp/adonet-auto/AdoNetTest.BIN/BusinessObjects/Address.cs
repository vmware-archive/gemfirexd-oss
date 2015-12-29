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
    /// Encapsulates address information to be linked with other entities 
    /// such as Customer, Supplier, etc. by object instantiation
    /// </summary>
    public class Address : BusinessObject
    {
        public long AddressId 
        { 
            get; 
            set; 
        }
        public string Address1 
        { 
            get; 
            set; 
        }
        public string Address2 
        { 
            get; 
            set; 
        }
        public string Address3 
        { 
            get; 
            set; 
        }
        public string City 
        { 
            get; 
            set; 
        }
        public StateCodes State 
        { 
            get;             
            set; 
        }
        public string ZipCode 
        { 
            get; 
            set; 
        }
        public string Province 
        { 
            get; 
            set; 
        }
        public CountryCodes CountryCode 
        { 
            get; 
            set; 
        }

        public Address()
        {
            AddressId = 0;
            Address1 = String.Empty;
            Address2 = String.Empty;
            Address3 = String.Empty;
            City = String.Empty;
            State = StateCodes.CA;
            ZipCode = String.Empty;
            Province = String.Empty;
            CountryCode = CountryCodes.UnitedStates;
        }

        public Address(long addressId)
            : this()
        {
            AddressId = addressId;
        }

        public Address(long addressId, string address1, string address2, string address3,
            string city, StateCodes state, string zipcode, string province, CountryCodes countrycode)
            : this()
        {
            AddressId = addressId;
            Address1 = address1;
            Address2 = address2;
            Address3 = address3;
            City = city;
            State = state;
            ZipCode = zipcode;
            Province = province;
            CountryCode = countrycode;
        }

        public Address(DataRow row)
            : this()
        {
            AddressId = long.Parse(row[0].ToString());
            Address1 = row[1].ToString();
            Address2 = row[2].ToString();
            Address3 = row[3].ToString();
            City = row[4].ToString();
            State = (StateCodes)Enum.Parse(
                typeof(StateCodes), row[5].ToString());
            ZipCode = row[6].ToString();
            Province = row[7].ToString();
            CountryCode = (CountryCodes)Enum.Parse(
                typeof(CountryCodes), row[8].ToString());
        }

        public override bool Equals(object obj)
        {
            if (!(obj is Address))
                return false;

            Address address = (Address)obj;

            return (this.AddressId == address.AddressId
                && this.Address1 == address.Address1
                && this.Address2 == address.Address2
                && this.Address3 == address.Address3
                && this.City == address.City
                && this.State == address.State
                && this.ZipCode == address.ZipCode
                && this.Province == address.Province
                && this.CountryCode == address.CountryCode);
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }

        public override string ToString()
        {
            return String.Format(
                "{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}\n",
                this.AddressId, this.Address1, this.Address2,
                this.Address3, this.City, this.State,
                this.ZipCode, this.Province, this.CountryCode);
        }
    }
}
