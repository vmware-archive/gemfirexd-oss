/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.catalog.IndexInfoImpl

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package com.pivotal.gemfirexd.internal.impl.sql.catalog;



import com.pivotal.gemfirexd.internal.catalog.UUID;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.CatalogRowFactory;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.ConglomerateDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.IndexRowGenerator;

/**
* A poor mans structure used in DataDictionaryImpl.java.
* Used to save information about system indexes.
*
*/
public class IndexInfoImpl
{
	private IndexRowGenerator	irg;

	private long				conglomerateNumber;
    
    private final CatalogRowFactory crf;
    private final int indexNumber;

	/**
	 * Constructor
	 *
	 * @param indexNumber			(0-based) number of index within catalog's indexes
	 * @param crf					CatalogRowFactory for the catalog
	 */
	IndexInfoImpl(int indexNumber, CatalogRowFactory crf)
	{
        this.crf = crf;
        this.indexNumber = indexNumber;
		this.conglomerateNumber = -1;
	}

    /**
	 * Get the conglomerate number for the index.
	 *
	 * @return long	The conglomerate number for the index.
	 */
	long getConglomerateNumber()
	{
		return conglomerateNumber;
	}

	/**
	 * Set the conglomerate number for the index.
	 *
	 * @param conglomerateNumber	The conglomerateNumber for the index.
	 */
	void setConglomerateNumber(long conglomerateNumber)
	{
		this.conglomerateNumber = conglomerateNumber;
	}

	/**
	 * Get the index name for the index.
	 *
	 * @return String	The index name for the index.
	 */
	String getIndexName()
	{
		return crf.getIndexName(indexNumber);
	}

	/**
	 * Get the column count for the index.
	 *
	 * @return int	The column count for the index.
	 */
	int getColumnCount()
	{
		return crf.getIndexColumnCount(indexNumber);
	}

	/**
	 * Get the IndexRowGenerator for this index.
	 *
	 * @return IndexRowGenerator	The IRG for this index.
	 */
	public IndexRowGenerator getIndexRowGenerator()
	{
		return irg;
	}

	/**
	 * Set the IndexRowGenerator for this index.
	 *
	 * @param irg			The IndexRowGenerator for this index.
	 */
	void setIndexRowGenerator(IndexRowGenerator irg)
	{
		this.irg = irg;
	}

	/**
	 * Get the base column position for a column within a catalog
	 * given the (0-based) column number for the column within the index.
	 *
	 * @param colNumber		The column number within the index
	 *
	 * @return int		The base column position for the column.
	 */
	int getBaseColumnPosition(int colNumber)
	{
		return crf.getIndexColumnPositions(indexNumber)[colNumber];
	}

	/**
	 * Return whether or not this index is declared unique
	 *
	 * @return boolean		Whether or not this index is declared unique
	 */
	boolean isIndexUnique()
	{
		return crf.isIndexUnique(indexNumber);
	}
}
