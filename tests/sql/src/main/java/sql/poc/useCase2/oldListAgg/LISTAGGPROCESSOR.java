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
package sql.poc.useCase2.oldListAgg;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;
import java.util.logging.Logger;

import com.pivotal.gemfirexd.procedure.IncomingResultSet;
import com.pivotal.gemfirexd.procedure.ProcedureProcessorContext;
import com.pivotal.gemfirexd.procedure.ProcedureResultProcessor;

public class LISTAGGPROCESSOR implements ProcedureResultProcessor {
	private ProcedureProcessorContext context;
	int counter = 0;
	long startTime = 0;

	TreeMap<String, ResultRow> tm = null;
	ArrayList<ResultRow> aggResults = null;
	private Iterator<ResultRow> resultsIter;
	static final Logger logger = Logger.getLogger("com.pivotal.gemfirexd");

	static {
	  logger.config("LISTAGGRESULT: Using version: " + ListAggProcedure.version);
	  logger.config("LISTAGGRESULT: Initializing the ListAggResultProcessor");
	}

	@Override
	public void close() {
		//logger.config("Closing result set processor");
		this.context = null;

	}

	@Override
	public List<Object> getNextResultRow(int resultSetNumber)
			throws InterruptedException {
		//logger.config("LISTAGGRESULT: Start of get Next Result Row.  ResultsetNumber = "
		//		+ resultSetNumber);

		List<Object> lesserRow = gatherFancyRow(resultSetNumber);
		//logger.config("LISTAGGRESULT: End of get Next Result Row.");

		return lesserRow;
	}

	private List<Object> gatherFancyRow(int resultSetNumber)
			throws InterruptedException {
		assert resultSetNumber == 0 : "unexpected resultSetNumber="
				+ resultSetNumber;
		if (aggResults == null) {
			aggResults = new ArrayList<ResultRow>();

			startTime = new Date().getTime();
			OrganizeResults2(this.context.getIncomingResultSets(0));
			//logger.config("LISTAGGPROC:Before Sort:"+(new Date().getTime()-startTime));
			//Collections.sort(aggResults);
			
			resultsIter = aggResults.iterator();
		} else {
			//logger.config("LISTAGGPROC: aggResults is null");
		}

		// now return all the rows
		if (resultsIter.hasNext()) {
			//logger.config("LISTAGGPROC: Returning next row");
			ResultRow rr = (ResultRow)resultsIter.next();
			return (List<Object>) rr.getValues();
		} else {
			//logger.config("LISTAGGPROC: Returning null value");
			return null;
		}
	}


	private void OrganizeResults2(IncomingResultSet[] inSets)
			throws InterruptedException {
		for (IncomingResultSet inSet : inSets) {
			List<Object> nextRow;

			while ((nextRow = inSet.takeRow()) != IncomingResultSet.END_OF_RESULTS) {
				ResultRow rr = new ResultRow(nextRow);
				int index = aggResults.indexOf(rr); 
				
				if ( index > -1) {
					ResultRow current = (ResultRow) aggResults.get(index);
					current.merge(rr);
					aggResults.set(index, current);
				} else {
					aggResults.add(rr);
				}
				
			}


		}

	}
	
	@Override
	public Object[] getOutParameters() throws InterruptedException {
		throw new AssertionError("this procedure has no out parameters");
	}

	@Override
	public void init(ProcedureProcessorContext context) {
		this.context = context;
		//logger.config("LISTAGGRESULT: Using version:"+ListAggProcedure.version);
		//logger.config("LISTAGGRESULT: Initializing the ListAggResultProcessor");

	}



}
