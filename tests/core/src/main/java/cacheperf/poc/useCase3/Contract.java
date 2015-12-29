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
package cacheperf.poc.useCase3;

import com.useCase3.rds.model.indicative.RDSAccount.AltAct;
import com.useCase3.rds.model.trade.RDSCDSContract;
import com.useCase3.rds.model.trade.RDSCDSContract.AccountRole;
import com.useCase3.rds.model.trade.RDSCDSContract.Amount;
import java.util.*;

public class Contract {

  public static RDSCDSContract init(int index) {
    return createData((long)index);
  }

  private static RDSCDSContract createData(long id)
  {
    RDSCDSContract contract = new RDSCDSContract(id);

    contract.setActvCrCrvId(id);
    contract.setBuySellC("SELL");
    contract.setCpnDRollTyp("ROLL");
    contract.setCrCrvCrcy("USD");
    contract.setCrCrvSubord("SUBORDINATION");
    contract.setCustmCshFlowF("TRUE");
    contract.setDaysTyp("DAYS TYPE");
    contract.setEOMRollF("ROLL FLAG");
    contract.setExecD(new Date());
    contract.setFixdRcvryA(12346.0);
    contract.setFrstCpnD(new Date());
    contract.setHldyClndrs("US");
    contract.setIntPerAdjAccrlRule("SOME DATA");
    contract.setISDACrEvtDefnDoc("ISDA DOC ID");
    contract.setIssCrcyC("USD");
    contract.setIssrCntryN("USA");
    //contract.setIssrCntryN("UNITED STATES");
    contract.setIssrSubord("ISSR SUBORD");
    contract.setLstCpnD(new Date());
    contract.setLstUpdtTmStmp(new Date());
    contract.setMarxIssrI(18738738783L);
    contract.setMatD(new Date());
    contract.setMatDRollF("ROLL FLAG");
    contract.setNotionalA(1878228.0);
    contract.setOrigCrCrvN("THIS IS CURVE NAME");
    contract.setPremPayFreq("PAY FREQ");
    contract.setPremR(18718781);
    contract.setRdsI(id);
    contract.setRestructTypC("RESTRUCTURING CODE");
    contract.setSchedMarchDir("MARCHNG DIR");
    contract.setStlmtD(new Date());
    contract.setStrtgy("A"+id);
    contract.setStubRule("STUB RULE");
    contract.setSubStrtgy("A53");
    contract.setTrdDesc("THIS IS A STORY DESCRIPION OF TRADE");
    contract.setTrdEffD(new Date());
    contract.setTrdFlatDfltF("TRUE");
    contract.setTrdStatus("ACTIVE");
    contract.setTrdTerminateD(new Date());

    for(int i = 0; i < 10; i++){
      AccountRole role = contract.new AccountRole(i+"");
      role.setActAltI("ABCCFFDF");
      role.setActAltITyp("sahgshghdgd");
      role.setActRoleC("agfsgfgsf");
      
      contract.setActRole(role);
    }
    
    for(int i = 0; i < 10; i++){
      Amount amt = contract.new Amount(i+"", new Date());
      amt.setAmtCrcyC("USD");
      amt.setAmtD(new Date());
      amt.setAmtPayRecvC("PAY RECD");
      amt.setAmtR(37657673);
      amt.setAmtTyp("MY PAYMENT");
      amt.setAmtValA(776767676);
      amt.setOasysLglEntl("yuyuyue");
      
      contract.setAmt(amt);
    }
    return contract;
  }
}
