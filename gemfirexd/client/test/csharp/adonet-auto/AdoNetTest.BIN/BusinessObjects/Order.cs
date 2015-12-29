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
    /// Encapsulates order information and related order details
    /// </summary>
    public class Order : BusinessObject
    {
        public long OrderId 
        { 
            get; 
            set; 
        }
        public DateTime OrderDate 
        { 
            get; 
            set; 
        }
        public DateTime ShipDate 
        { 
            get; 
            set; 
        }

        private Customer customer;
        public Customer Customer 
        {
            get
            {
                if (customer == null)
                    customer = new Customer();

                return customer;
            }
            set
            {
                customer = value;
            }
        }

        private IList<OrderDetail> orderDetails;
        public IList<OrderDetail> OrderDetails 
        {
            get
            {
                if (orderDetails == null)
                    orderDetails = new List<OrderDetail>();

                return orderDetails;
            }
            set
            {
                orderDetails = value;
            }
        }
        public float SubTotal 
        { 
            get; 
            set; 
        }


        public Order()
        {
            OrderId = 0;
            OrderDate = DateTime.Today;
            ShipDate = DateTime.Today;
            Customer = new Customer();
            OrderDetails = new List<OrderDetail>();
            SubTotal = 0;
        }

        public Order(long orderId)
            : this()
        {
            OrderId = orderId;
        }

        public Order(long orderId, DateTime orderDate, DateTime shipDate,
            long customerId, float subtotal)
            : this()
        {
            OrderId = orderId;
            OrderDate = OrderDate;
            ShipDate = shipDate;
            Customer.CustomerId = customerId;
            SubTotal = subtotal;
        }

        public Order(DataRow row)
            : this()
        {
            OrderId = long.Parse(row[0].ToString());
            OrderDate = DateTime.Parse(row[1].ToString());
            ShipDate = DateTime.Parse(row[2].ToString());
            Customer.CustomerId = long.Parse(row[3].ToString());
            SubTotal = float.Parse(row[4].ToString());
        }

        public override bool Equals(object obj)
        {
            if (!(obj is Order))
                return false;

            Order order = (Order)obj;

            return (this.OrderId == order.OrderId
                && this.OrderDate == order.OrderDate
                && this.ShipDate == order.ShipDate
                && this.Customer.CustomerId == order.Customer.CustomerId
                && this.SubTotal == order.SubTotal);
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();

            sb.AppendFormat(
                "{0}, {1}, {2}, {3}, {4}\n",
                this.OrderId, this.OrderDate, this.ShipDate,
                this.Customer.CustomerId, this.SubTotal);

            foreach (OrderDetail detail in this.OrderDetails)
                sb.AppendFormat("\t{0}", detail.ToString());

            return sb.ToString();
        }

        private bool Equals(IList<OrderDetail> orderDetails)
        {
            for (int i = 0; i < this.OrderDetails.Count; i++)
            {
                if (!this.OrderDetails[i].Equals(orderDetails[i]))
                    return false;
            }

            return true;
        }        

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }
    }
}
