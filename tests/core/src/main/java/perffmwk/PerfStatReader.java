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
package perffmwk;

import com.gemstone.gemfire.*;
import com.gemstone.gemfire.internal.*;

import hydra.Log;

import java.io.*;
import java.text.NumberFormat;
import java.util.*;
//import java.util.regex.*;

/**
 *
 *  Reads statistics from statarchive files.
 *
 */
public class PerfStatReader {

//------------------------------------------------------------------------------
// Timestamps
//------------------------------------------------------------------------------

  static Calendar calendar = Calendar.getInstance();

  private static String convertTimestamp(long ms) {
    calendar.setTimeInMillis(ms);

    int hrs  = calendar.get(Calendar.HOUR_OF_DAY);
    int mins = calendar.get(Calendar.MINUTE);
    int secs = calendar.get(Calendar.SECOND);

    NumberFormat nf = NumberFormat.getIntegerInstance();
    nf.setMinimumIntegerDigits(2);
    return nf.format(hrs) + ":" + nf.format(mins) + ":" + nf.format(secs);
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    INTERNALS                                                         ////
  //////////////////////////////////////////////////////////////////////////////

  /**
   *  Fetches the values for all statistics specifications in the configuration,
   *  trimmed as specified by the trim specifications.  Returns a map keyed by
   *  the logical name of the statistics specification, where the value is a
   *  (possibly empty) list of {@link PerfStatValue}s for that specification.
   */
  protected static SortedMap processStatConfig( StatConfig statconfig ) {

    List tmpArchives = statconfig.getStatisticArchivesAsList();
    if (tmpArchives == null || tmpArchives.size() == 0) {
      return null;
    }
    List archives = new ArrayList();
    archives.addAll(tmpArchives);

    List statspecs = new ArrayList();
    statspecs.addAll(statconfig.getStatSpecInstances().values());

    if ( Log.getLogWriter().finestEnabled() ) {
      Log.getLogWriter().finest( "PerfStatReader: archives ==> " + archives );
      Log.getLogWriter().finest( "PerfStatReader: generating from ==> "
         + statconfig.getStatSpecs().keySet() + "==>"
         + statconfig.getStatSpecs().values() );
    }
    Map rawValues = new HashMap();
    try {
      StatArchiveReader reader = new StatArchiveReader(
          (File[])archives.toArray(new File[archives.size()]),
          (StatSpec[])statspecs.toArray(new StatSpec[statspecs.size()]),
          true);
      Iterator it = statspecs.iterator();
      while (it.hasNext()) {
        StatSpec spec = (StatSpec)it.next();
        StatArchiveReader.StatValue[] values = reader.matchSpec(spec);
        if (Log.getLogWriter().finestEnabled()) {
          for (int i = 0; i < values.length; i++) {
            StatArchiveReader.StatValue value = values[i];
            long[] times = value.getRawAbsoluteTimeStamps();
            // this is filter=none, untrimmed
            double[] rawvals = value.getRawSnapshots();
            // this is filter=whatever it is for the statspec, untrimmed
            double[] vals = value.getSnapshots();
            Log.getLogWriter().finest(times.length + " timestamps with "
                                     + vals.length + " values");
            String s = "";
            for (int j = 0; j < Math.min(times.length, vals.length); j++) {
              s += "\ntime " + convertTimestamp(times[j]) + " " + times[j] + " filtered = " + vals[j] + " raw = " + rawvals[j] + " ";
            }
            Log.getLogWriter().finest("Values: " + s);
          }
          Log.getLogWriter().finest( "PerfStatReader: Read spec ==> " + spec );
	  for ( int p = 0; p < values.length; p++ ) {
	    Log.getLogWriter().finest("PerfStatReader: Initial value[" + p + "]=" + values[p]);
	  }
        }
        rawValues.put(spec.getName(), Arrays.asList(values));
      }
    } catch( IOException e ) {
      throw new StatConfigException( "Unable to read archive", e );
    }
    if ( Log.getLogWriter().finerEnabled() ) { 
      for ( Iterator i = rawValues.keySet().iterator(); i.hasNext(); ) {
        String statspecname = (String) i.next();
        List statspecvalues = (List) rawValues.get( statspecname );
        Log.getLogWriter().finer( "PerfStatReader: Raw values ==> " + statspecname + "..." + statspecvalues );
      }
    }

    // now do the reading, then filter, trim, and do operations on each result
    SortedMap processedValues = new TreeMap();
    for ( Iterator i = rawValues.keySet().iterator(); i.hasNext(); ) {
      String statspecname = (String) i.next();
      List rawValueList = (List) rawValues.get( statspecname );
      List processedValueList = new ArrayList();
      for ( Iterator j = rawValueList.iterator(); j.hasNext(); ) {
	StatSpec statspec = statconfig.getStatSpec( statspecname );
	TrimSpec trimspec = statconfig.getTrimSpec( statspec.getTrimSpecName() );
	StatArchiveReader.StatValue sv = (StatArchiveReader.StatValue) j.next();
	PerfStatValue psv = getPerfStatValue( statspec, trimspec, sv );

        StatArchiveReader.ResourceType type = sv.getType();
        StatArchiveReader.StatDescriptor desc =
          type.getStat(statspec.getStatName());
        if (desc == null) {
          String s = "Could not find a StatDescriptor for \"" +
            statspec.getStatName() + "\"";
          throw new InternalGemFireException(s);
        }

        if (PerfReporter.brief) {
          StringBuffer sb = new StringBuffer();
          sb.append(desc.getDescription());
          sb.append(" (");
          sb.append(desc.getUnits());
          if (statspec.getFilter() ==
              StatArchiveReader.StatValue.FILTER_PERSEC) {
            sb.append("/sec");
          }
          sb.append(")");
          statspec.setDescription(sb.toString());
        }

	processedValueList.add( psv );
        if (Log.getLogWriter().finestEnabled()) { 
          Log.getLogWriter().finest( "PerfStatReader: PSV: " + psv.toString());
        }
        // @todo lises need to sort these for later comparisons, but on what?
        //             any way to use the logical archive it came from?
      }
      if ( Log.getLogWriter().finestEnabled() ) { 
        Log.getLogWriter().finest( "PerfStatReader: Put " + statspecname + " values as " + processedValueList );
      }
      processedValues.put( statspecname, processedValueList );
    }
    if ( Log.getLogWriter().finerEnabled() ) { 
      for ( Iterator i = processedValues.keySet().iterator(); i.hasNext(); ) {
        String statspecname = (String) i.next();
        List statspecvalues = (List) processedValues.get( statspecname );
        Log.getLogWriter().finer( "PerfStatReader: Processed values ==> " + statspecname + "..." + statspecvalues );
      }
    }

    // now evaluate the expressions
    List exprs = new ArrayList();
    exprs.addAll(statconfig.getExprInstances().values());
    if (Log.getLogWriter().finestEnabled()) { 
      Log.getLogWriter().finest("PerfStatReader: exprs ==> " + exprs);
    }
    for (Iterator i = exprs.iterator(); i.hasNext();) {
      Expr expr = (Expr)i.next();
      String numerator = expr.getNumerator().getName();
      String denominator = expr.getDenominator().getName();
      List numerators = (List)processedValues.get(numerator);
      List denominators = (List)processedValues.get(denominator);
      if ( Log.getLogWriter().finestEnabled() ) { 
        Log.getLogWriter().finest("Dividing " + numerator + " by " + denominator
                          + " ==> " + numerators + " OVER " + denominators);
      }
      List processedValueList = new ArrayList();
      if (numerators.size() != 0 && denominators.size() != 0) {
        for (int j = 0; j < numerators.size(); j++) {
          PerfStatValue npsv = (PerfStatValue)numerators.get(j);
          PerfStatValue dpsv = (PerfStatValue)denominators.get(j);
  	PerfStatValue psv = npsv.dividedBy(dpsv);
          if (psv != null) {
            processedValueList.add(psv);
          }
        }
      } else if (numerators.size() == 0 || denominators.size() == 0) {
        // nothing to do
      } else if (numerators.size() != denominators.size()) {
        String s = "Dividing " + numerator + " by " + denominator
                 + " found numerators (" + numerators.size()
                 + ") and denominators (" + denominators.size()
                 + " have different sizes ==> " + numerators
                 + " OVER " + denominators;
        throw new PerfComparisonException(s);
      }
      processedValues.put(expr.getName(), processedValueList);
    }

    return processedValues;
  }

  /**
   *  Translates a StatValue into a PerfStatValue with the specified filter,
   *  with the specified operations, and the specified trim.  For the latter,
   *  if start is -1, uses the beginning of the statistic's existence, and
   *  if end is -1, goes to the end of the statistic's existence.
   */
  protected static PerfStatValue getPerfStatValue( StatSpec statspec, TrimSpec trimspec,
                                                   StatArchiveReader.StatValue sv ) {
    long start = trimspec.getStart();
    long end = trimspec.getEnd();
    sv = sv.createTrimmed( start, end );
    if ( Log.getLogWriter().finestEnabled() ) { 
      Log.getLogWriter().finest( "PerfStatReader: Trimmed from " + trimspec.getStartStr() + " (" + start + ") to " + trimspec.getEndStr() + " (" + end + ")" );
    }

    int filter = statspec.getFilter();
    double mean = -1;
    if ( filter == StatArchiveReader.StatValue.FILTER_PERSEC && statspec.getMean() ) {
      if ( start == -1 ) {
        start = sv.getRawAbsoluteTimeStamps()[0];
      }
      if ( end == -1 ) {
        long[] rats = sv.getRawAbsoluteTimeStamps();
        end = rats[ rats.length - 1 ];
      }
      long elapsedSec = ( end - start ) / 1000;
      sv.setFilter( StatArchiveReader.StatValue.FILTER_NONE );
      double del = sv.getSnapshotsMaximum() - sv.getSnapshotsMinimum();
      mean = del / elapsedSec;
    }
    sv.setFilter( filter );

    // @todo lises see if psv really needs to hang onto specs
    PerfStatValue psv = new PerfStatValue( statspec, trimspec );
    psv.setIsLargerBetter( sv.getDescriptor().isLargerBetter() );
    psv.setSamples( sv.getSnapshotsSize() );

    if ( statspec.getMin() )
      psv.setMin( sv.getSnapshotsMinimum() );
    if ( statspec.getMax() )
      psv.setMax( sv.getSnapshotsMaximum() );
    if ( statspec.getMaxMinusMin() )
      psv.setMaxMinusMin( sv.getSnapshotsMaximum() - sv.getSnapshotsMinimum() );
    if ( statspec.getMean() ) {
      if ( filter == StatArchiveReader.StatValue.FILTER_PERSEC ) {
        psv.setMean( mean );
      } else {
        psv.setMean( sv.getSnapshotsAverage() );
      }
    }
    if ( statspec.getStddev() )
      psv.setStddev( sv.getSnapshotsStandardDeviation() );

    SortedSet archives = new TreeSet();
    StatArchiveReader.ResourceInst[] resources = sv.getResources();
    String productVersion = null;
    for ( int i = 0; i < resources.length; i++ ) {
      String archive = resources[i].getArchive().getFile().getParentFile().getName();
      if ( productVersion == null ) {
        productVersion = resources[i].getArchive().getArchiveInfo().getProductVersion();
      }
      if ( ! archives.contains( archive ) ) {
        archives.add( archive );
      }
    }
    psv.setArchives( archives );
    psv.setProductVersion( productVersion );

    return psv;
  }
}
