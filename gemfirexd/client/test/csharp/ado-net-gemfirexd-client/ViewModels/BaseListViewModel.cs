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
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;

namespace ado_net_gemfirexd_client.ViewModels
{
    public abstract class BaseListViewModel<TEntity> : BaseViewModel<TEntity>
    {
        /// <summary>
        /// Gets or sets the collection.
        /// </summary>
        /// <value>The collection.</value>
        public ObservableCollection<BaseDetailsViewModel<TEntity>> Collection 
        { 
            get;
            private set; 
        }

        /// <summary>
        /// Gets or sets the selected.
        /// </summary>
        /// <value>The selected.</value>
        public BaseDetailsViewModel<TEntity> Selected
        {
            get; 
            set;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BaseListViewModel&lt;TEntity&gt;"/> class.
        /// </summary>
        /// <param name="collection">The collection.</param>
        protected BaseListViewModel(List<BaseDetailsViewModel<TEntity>> collection)
        {
            Collection = new
            ObservableCollection<BaseDetailsViewModel<TEntity>>(collection);
        }
    }
}
