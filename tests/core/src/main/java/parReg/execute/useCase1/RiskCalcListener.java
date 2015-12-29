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
package parReg.execute.useCase1;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

import hydra.*;

public class RiskCalcListener extends CacheListenerAdapter implements
    Declarable {

  // id_param_rsk values
  public static final String DELT = "DELT";

  public static final String VEGA = "VEGA";

  public static final String GAMM = "GAMM";

  // id_type_imnt values
  public static final String STK = "STK";

  public static final String XFX = "XFX";

  long totalTime = 0;

  public void afterCreate(EntryEvent event) {
    afterUpdate(event);
  }

  public void afterUpdate(EntryEvent event) {

    // check if this is an update on a Position
    // otherwise, return
    if (event.getNewValue() == null) {
      return;
    }

    RiskPartitionKey eventKey = (RiskPartitionKey)event.getKey();
    // if(callCount++ % 10000 == 0) {
    // System.out.println(" ***** " + callCount + " ***** ");
    // }

    // we currently only trigger the risk calculation on position updates.
    // this if causes calculations to be aborted for other types of updates.
    if (eventKey.getType() != RiskPartitionKey.TYPE_POSITION) {
      return;
    }

    Position position = (Position)event.getNewValue();

    Log.getLogWriter().info("Listener invoked for Position " + position);

    // Calculate PositionRisk.am_fv

    // Lookup the actual Imnt_risk_sensitivity (local get on PR)
    Log.getLogWriter().info("Instrument id is " + position.id_imnt);
    String irsKey = new StringBuffer().append(position.id_imnt).append("|VEGA")
        .toString();
    ImntRiskSensitivity irs = (ImntRiskSensitivity)UseCase1
        .getInstrumentSensitivityRegion().get(
            new RiskPartitionKey(irsKey, position.id_imnt,
                RiskPartitionKey.TYPE_INSTRUMENT_RISK_SENSITIVITY));
    Log.getLogWriter().info(
        "Doing get with "
            + new RiskPartitionKey(irsKey, position.id_imnt,
                RiskPartitionKey.TYPE_INSTRUMENT_RISK_SENSITIVITY));
    Log.getLogWriter()
        .info(
            "Region size is "
                + UseCase1.getInstrumentSensitivityRegion().size());
    Log.getLogWriter().info(
        "In Listener Received Instrument Risk sensitivity : " + irs);
    if (irs == null) {
      // if there is no ImntRiskSensitivity for this Instrument, we abort the
      // calculation
      // System.out.println("Got null ImntRiskSensitivity: " +
      // position.id_imnt);
      return;
    }
    float am_exp_rsk = irs.am_exp_rsk;
    Log.getLogWriter().info("am_exp_rsk value is " + am_exp_rsk);

    // Lookup the actual Instrument (get on PR)
    Instrument instrument = (Instrument)UseCase1.getInstrumentRegion().get(
        new RiskPartitionKey(String.valueOf(position.id_imnt),
            position.id_imnt, RiskPartitionKey.TYPE_INSTRUMENT));
    Log.getLogWriter().info("In Listener Received Instrument : " + instrument);
    if (instrument == null) {
      // if there is no Instrument for this Position, we abort the calculation
      // System.out.println("Got null Instrument: " + position.id_imnt);
      return;
    }

    // lookup the fx_mult value
    String fxKey = instrument.id_ccy_main + "|USD";
    Region fxRateRegion = UseCase1.getFxRateRegion();
    FxRate rate = (FxRate)fxRateRegion.get(fxKey);
    Log.getLogWriter().info("In Listener Received fxRate : " + rate);
    if (rate == null) {
      // if there is no FxRate for this Instrument.id_ccy_main, we abort the
      // calculation
      // System.out.println("Got null FxRate: " + fxKey);
      return;
    }
    float fx_mult = rate.rt_fx;

    PositionRisk positionRisk = new PositionRisk();
    positionRisk.id_posn_new = position.id_posn_new;

    positionRisk.am_fv = position.am_posn * am_exp_rsk * fx_mult
        * instrument.am_sz_ctrt;
    // * instrument.am_mult_trd; //TODO: Where is this field? Use 1.0 for now.

    // Calculate PositionUndRisk.am_risk for each UndRiskSensitivity

    // Get all undRiskSens where id_imnt = instrument.id_imnt
    PartitionedRegion undSensitivityRegion = UseCase1
        .getUndSensitivityRiskRegion();

    UseCase1Function useCase1Function = new UseCase1Function();
    FunctionService.registerFunction(useCase1Function);

    ArrayList ursList = null;
    ;
    try {
      // this executes a query against only the current partition bucket. We
      // know the UndRiskSensitivity objects
      // will be in this bucket, so there is no need to query the entire PR.
      HashSet set = new HashSet();
      set.add(eventKey);
      Execution ds = FunctionService.onRegion(undSensitivityRegion).withFilter(
          set).withArgs(new Integer(position.id_imnt));
      ursList = (ArrayList)ds.execute(useCase1Function).getResult();
      Log.getLogWriter().info("List size is " + ursList.size());
      Log.getLogWriter().info("List is " + ursList.toString());

    }
    catch (Exception e) {
      e.printStackTrace();
    }

    if (ursList == null) {
      // There were no UndRiskSensitivitys for this Instrument, so abort the
      // calculation
      // System.out.println("Got null UndRiskSensitivity[]: " +
      // instrument.id_imnt);
      return;
    }

    ArrayList combinedList = new ArrayList();
    for (int i = 0; i < ursList.size(); i++) {
      combinedList.addAll((List)ursList.get(i));
    }

    // create a PositionUndRisk for each UndRiskSensitivity
    PositionUndRisk[] undRisks = new PositionUndRisk[combinedList.size()];

    for (int i = 0; i < combinedList.size(); i++) {
      Log.getLogWriter().info(
          "The list has " + ((UndRiskSensitivity)combinedList.get(i)));

      // Lookup und_price for this UndRiskSensitivity.
      // if this value is the same for all UndRiskSensitivitys,
      // then we should change this to only do the lookup once.
      String undKey = new StringBuffer().append(position.id_imnt).append("|")
          .append(((UndRiskSensitivity)combinedList.get(i)).id_imnt_und)
          .append("|DELT").toString();
      UndRiskSensitivity und = (UndRiskSensitivity)UseCase1
          .getUndSensitivityRiskRegion().get(
              new RiskPartitionKey(undKey, position.id_imnt,
                  RiskPartitionKey.TYPE_UND_RISK_SENSITIVITY));
      Log.getLogWriter()
          .info("In Listener, received UndRiskSensitivity " + und);
      if (und == null) {
        // unable to find und_price for this UndRiskSensitivity.
        // For now, abort the calculation. This could be modified to abort
        // only the current PositionUndRisk
        // System.out.println("Got null UndRiskSensitivity: " + undKey);
        return;
      }
      float und_price = und.am_exp_rsk;

      // populate undRisks[i]
      undRisks[i] = new PositionUndRisk();
      undRisks[i].id_posn_new = position.id_posn_new;
      undRisks[i].id_imnt_und = ((UndRiskSensitivity)combinedList.get(i)).id_imnt_und;
      undRisks[i].id_param_rsk = ((UndRiskSensitivity)combinedList.get(i)).id_param_rsk;
      undRisks[i].am_risk = position.am_posn
          * ((UndRiskSensitivity)combinedList.get(i)).am_exp_rsk * fx_mult
          * instrument.am_sz_ctrt *
          // instrument.am_mult_trd * //TODO: Where is this field? Use 1.0 for
          // now.
          und_price;
    }

    // Sum and average PositionRisk values based on new PosUndRisks

    positionRisk.am_delta = 0;
    positionRisk.am_delta_avg = 0;
    positionRisk.am_delta_equity = 0;
    positionRisk.am_delta_ccy = 0;
    positionRisk.am_vega = 0;
    positionRisk.am_gamma = 0;

    for (int i = 0; i < undRisks.length; i++) {
      float thisRiskDelta = undRisks[i].am_risk;
      if (undRisks[i].id_param_rsk.equals(DELT)) {
        positionRisk.am_delta += thisRiskDelta;

        // NOTE: This undInst lookup is likely on another bucket in the PR, not
        // local
        // This is a PR get, not a query, so it shouldn't be too expensive.
        // To shortcut this, use the original instrument object.
        Instrument undInst = (Instrument)event.getRegion().get(
            new RiskPartitionKey(String.valueOf(undRisks[i].id_imnt_und),
                undRisks[i].id_imnt_und, RiskPartitionKey.TYPE_INSTRUMENT));

        // if we can't find the underlying Instrument, use the current
        // Instrument.
        if (undInst == null) {
          undInst = instrument;
        }

        if (undInst.id_typ_imnt.equals(STK)) {
          positionRisk.am_delta_equity += thisRiskDelta;
        }
        if (undInst.id_typ_imnt.equals(XFX)) {
          positionRisk.am_delta_ccy += thisRiskDelta;
        }
      }

      if (undRisks[i].id_param_rsk.equals(VEGA)) {
        positionRisk.am_vega += thisRiskDelta;
      }

      if (undRisks[i].id_param_rsk.equals(GAMM)) {
        positionRisk.am_gamma += thisRiskDelta;
      }

    }

    // avoid divide by 0
    if (undRisks.length == 0) {
      positionRisk.am_delta_avg = 0;
    }
    else {
      positionRisk.am_delta_avg = positionRisk.am_delta / undRisks.length;
      Log.getLogWriter().info(
          "Calculating the average : " + positionRisk.am_delta + "/"
              + undRisks.length + "=" + positionRisk.am_delta_avg);
    }

    Log.getLogWriter().info(
        "Postion risk values : positionRisk.am_delta " + positionRisk.am_delta
            + " positionRisk.am_delta_avg " + positionRisk.am_delta_avg
            + " positionRisk.am_delta_equity " + positionRisk.am_delta_equity
            + " positionRisk.am_delta_ccy " + positionRisk.am_delta_ccy
            + " positionRisk.am_vega " + positionRisk.am_vega
            + " positionRisk.am_gamma " + positionRisk.am_gamma);

    // Fill in the remaining hierarchy and sector fields to
    // flatten the objects so we can slice and dice them later

    Hierarchy hierarchy = (Hierarchy)UseCase1.getHierarchyRegion().get(
        String.valueOf(position.id_book));
    if (hierarchy == null) {
      // Unable to find Hierarchy info. Abort calculation.
      // System.out.println("Hierarchy was null for key " + position.id_book);
      return;
    }

    // currently, there is no need to actually look up the IndustrySector, since
    // we can get the id_sector from the Instrument

    // IndustrySector s = (IndustrySector)
    // CacheService.getCacheService().getIndustrySectorRegion().get(String.valueOf(instrument.id_sector));

    // if(s==null) {
    // System.out.println("IndustrySector was null for key " +
    // instrument.id_sector);
    // return;
    // }

    positionRisk.id_book = hierarchy.id_book;
    positionRisk.id_prtf = hierarchy.id_prtf;
    positionRisk.id_country = hierarchy.id_country;
    positionRisk.id_desk = hierarchy.id_desk;

    for (int i = 0; i < undRisks.length; i++) {
      undRisks[i].id_desk = hierarchy.id_desk;
      undRisks[i].id_country = hierarchy.id_country;
      undRisks[i].id_prtf = hierarchy.id_prtf;
      undRisks[i].id_sector = instrument.id_sector;

      Log.getLogWriter().info(
          "undRisks[" + i + "] has the value " + undRisks[i].toString());
    }

    // Put the resulting PositionRisk and PositionUndRisks into their
    // respective regions

    PartitionedRegion posRiskRegion = UseCase1.getPositionRiskRegion();
    PartitionedRegion posUndRiskRegion = UseCase1.getPositionUndRiskRegion();

    Log.getLogWriter().info(
        "Putting in the position Risk region with key "
            + positionRisk.id_posn_new + " and value " + positionRisk
            + " in the region " + posRiskRegion.getPartitionAttributes());
    posRiskRegion.put(new Integer(positionRisk.id_posn_new), positionRisk);
    Log.getLogWriter().info(
        "Did put in the position Risk region with key "
            + positionRisk.id_posn_new + " and value " + positionRisk);

    // debug print outs
    // if(undRisks.length > 0) {
    // System.out.println(positionRisk.id_desk + " " + positionRisk.id_country +
    // " " + positionRisk.id_prtf + " " + positionRisk.id_book + " " +
    // instrument.id_sector + " " + undRisks[0].id_param_rsk);
    // }
    // System.out.println("Put PosRisk with " + undRisks.length + " UNDs");

    for (int i = 0; i < undRisks.length; i++) {
      StringBuffer key = new StringBuffer();
      key.append(undRisks[i].id_posn_new);
      key.append("|");
      key.append(undRisks[i].id_imnt_und);
      key.append("|");
      key.append(undRisks[i].id_param_rsk);

      Log.getLogWriter().info(
          "Putting in the position Und Risk region with key " + key.toString()
              + " and value " + undRisks[i]);
      posUndRiskRegion.put(key.toString(), undRisks[i]);
    }

    // System.out.print("+" + undRisks.length);

    // totalTime += (System.currentTimeMillis() - startTime);
    // Log.getLogWriter().info("Total time is "+ totalTime);

    // This println was used to determine how much cpu clock time is spend doing
    // calculations on this node
    // System.out.println("Total calculcation time was " + totalTime + " ms.");

  }

  public void init(Properties arg0) {

  }

}
