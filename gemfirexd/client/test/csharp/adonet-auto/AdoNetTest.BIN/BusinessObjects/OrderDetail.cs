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
using System.IO;

namespace AdoNetTest.BIN.BusinessObjects
{
    /// <summary>
    /// Represents a line item with associated order and product information
    /// </summary>
    public class OrderDetail : BusinessObject
    {
        public long OrderId 
        { 
            get; 
            set; 
        }
        public long ProductId 
        { 
            get; 
            set; 
        }
        public int Quantity 
        { 
            get; 
            set; 
        }
        public float UnitPrice 
        { 
            get; 
            set; 
        }
        public float Discount 
        { 
            get; 
            set; 
        }


        public OrderDetail()
        {
            OrderId = 0;
            ProductId = 0;
            Quantity = 0;
            UnitPrice = 0;
            Discount = 0;
        }

        public OrderDetail(long orderId, long productId)
            : this()
        {
            OrderId = orderId;
            ProductId = productId;
        }

        public OrderDetail(long orderId, long productId,
            int quantity, float unitPrice, float discount)
            : this()
        {
            OrderId = orderId;
            ProductId = productId;
            Quantity = quantity;
            UnitPrice = unitPrice;
            Discount = discount;
        }

        public OrderDetail(DataRow row)
            : this()
        {
            OrderId = long.Parse(row[0].ToString());
            ProductId = long.Parse(row[1].ToString());
            Quantity = int.Parse(row[2].ToString());
            UnitPrice = float.Parse(row[3].ToString());
            Discount = float.Parse(row[4].ToString());
        }

        public override bool Equals(object obj)
        {
            if(!(obj is OrderDetail))
                return false;

            OrderDetail ordDetail = (OrderDetail)obj;

            return (this.OrderId == ordDetail.OrderId
                && this.ProductId == ordDetail.ProductId
                && this.Quantity == ordDetail.Quantity
                && this.UnitPrice == ordDetail.UnitPrice
                && this.Discount == ordDetail.Discount);
        }

        public override string ToString()
        {
            return String.Format(
                "{0}, {1}, {2}, {3}, {4}\n",
                this.OrderId, this.ProductId, this.Quantity,
                this.UnitPrice, this.Discount);
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }
    }
}
