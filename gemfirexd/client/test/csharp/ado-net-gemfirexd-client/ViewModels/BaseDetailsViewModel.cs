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
using System.Text;

namespace ado_net_gemfirexd_client.ViewModels
{
    public abstract class BaseDetailsViewModel<TEntity> : BaseViewModel<TEntity>
    {
        /// <summary>
        /// Initializes a new instance of the class.
        /// </summary>
        /// <param name="currentEntity">The current entity.</param>
        protected BaseDetailsViewModel(TEntity currentEntity)
        {
            CurrentEntity = currentEntity;
        }
        /// <summary>
        /// Gets the current entity.
        /// </summary>
        /// <value>The current entity.</value>
        public TEntity CurrentEntity { get; private set; }
        /// <summary>
        /// Froms the model to view.
        /// </summary>
        protected abstract void FromModelToView();
        /// <summary>
        /// Froms the view to model.
        /// </summary>
        protected abstract void FromViewToModel();
    }
}
