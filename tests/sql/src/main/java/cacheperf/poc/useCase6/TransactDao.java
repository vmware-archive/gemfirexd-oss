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
package cacheperf.poc.useCase6;

import cacheperf.comparisons.gemfirexd.QueryPerfStats;
import hydra.Log;
import hydra.HydraRuntimeException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import perffmwk.HistogramStats;

public class TransactDao extends JdbcDaoBase {

  private SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  private SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddHHmmss");

  private long start;

  QueryPerfStats querystats;
  private HistogramStats histogram;
  
  public TransactDao(){
    super.getConnection();
  }

  public TransactDao(Connection connection, QueryPerfStats qstats,
                                            HistogramStats hstats) {
    super(connection);
    this.querystats = qstats;
    this.histogram = hstats;
  }

  String formatDayString(Timestamp bdate){
    String adate = null;
    adate = sdf2.format(bdate);
    return adate;
  }
  
  private String formatDate(String stringDate){
    
      String year = stringDate.substring(0, 4);
      String month = stringDate.substring(4, 6);
      String day = stringDate.substring(6, 8);
      String hour = stringDate.substring(8, 10);
      String minute = stringDate.substring(10, 12);
      String second = stringDate.substring(12, 14);
      
      String date = year + "-" + month + "-" + day + " " + hour + ":" + minute + ":" + second;

    return date;
  }  
  
  public List<TransactEntity> select(String fld07, String fld08, String d_str, boolean isValue) throws SQLException {

    // build statement
    //start = querystats.startPrepare();
    // format yyyy-MM-dd HH:mm:ss
    String date = formatDate(d_str);
      
    Date fld026 = null;
    Date fld027 = null;
    try {
      fld026 = sdf1.parse(date);
      fld027 = sdf1.parse("2009-01-01 00:00:00");
    } catch (ParseException e) {
      throw new HydraRuntimeException("Parse error", e);
    }

    SqlBuilder sql = super.createSqlBuilder(300);
    if(isValue){
      sql.append("SELECT * FROM Transact");
    } else {
      sql.append("SELECT TransactNo FROM Transact");
    }
    sql.append("WHERE");
    sql.append("Field001 = 1 ");
    sql.append("AND Field007 = ? ", fld07);
    sql.append("AND Field008 = ? ", fld08);
    sql.append("AND Field016 = 1111 ");
    sql.append("AND Field018 = 1111 ");
    sql.append("AND Field034 = 1111 ");
    sql.append("AND Field041 = 1111 ");
    sql.append("AND Field051 = 11 ");
    sql.appendIfNotNull("AND Field026 >= ?", fld026);
    sql.append("AND Field027 >= ?", fld027);

    //  prepareStatemet
    super.prepareStatemet(sql);
    //querystats.endPrepare(start);
    
    // executeQuery
    start = querystats.startQuery();
    ResultSet rs = ps.executeQuery();
    querystats.endQuery(start, histogram);
    // end
    
    // ORM
    //start = querystats.startORM();
    List<TransactEntity> ret = new ArrayList<TransactEntity>();
    try {
      while (rs.next()){
        TransactEntity entity = mappingTransact(rs,isValue);
        ret.add(entity);
      }
    } catch (SQLException e){
      super.sqlTransrator(e, "ret.size()=" + ret.size());
    } finally {
      try {
        close();
      } catch (SQLException e) {
        e.printStackTrace();
        Log.getLogWriter().info("Failed to close: " + e.getMessage());
      }
    }
    //querystats.endORM(start);
    
    if (fineEnabled) {
      Log.getLogWriter().fine(" Result set size = " + ret.size());
    }
    return ret;
  }
  
  private TransactEntity mappingTransact(ResultSet rs, boolean isValue)
  throws SQLException {
    TransactEntity entity = new TransactEntity();
    entity.setId(new TransactEntityId()); 
    
    if (isValue) {
      entity.setField001(rs.getDouble("Field001"));
      entity.setField002(rs.getString("Field002"));
      entity.setTransactNo(rs.getDouble("TransactNo"));
      entity.setField004(rs.getDouble("Field004"));
      entity.setField005(rs.getString("Field005"));
      entity.setField006(rs.getString("Field006"));
      entity.setField007(rs.getString("Field007"));
      entity.setField008(rs.getString("Field008"));
      entity.setField009(rs.getDouble("Field009"));
      entity.setField010(rs.getString("Field010"));
      entity.setField011(rs.getString("Field011"));
      entity.setField012(rs.getString("Field012"));
      entity.setField013(rs.getDouble("Field013"));
      entity.setField014(rs.getString("Field014"));
      entity.setField015(rs.getDouble("Field015"));
      entity.setField016(rs.getDouble("Field016"));
      entity.setField017(rs.getString("Field017"));
      entity.setField018(rs.getDouble("Field018"));
      entity.setField019(rs.getString("Field019"));
      entity.setField020(rs.getString("Field020"));
      entity.setField021(formatDayString(rs.getTimestamp("Field021")));
      entity.setField022(formatDayString(rs.getTimestamp("Field022")));
      entity.setField023(formatDayString(rs.getTimestamp("Field023")));
      entity.setField024(formatDayString(rs.getTimestamp("Field024")));
      entity.setField025(formatDayString(rs.getTimestamp("Field025")));
      entity.setField026(formatDayString(rs.getTimestamp("Field026")));
      entity.setField027(formatDayString(rs.getTimestamp("Field027")));
      entity.setField028(rs.getDouble("Field028"));
      entity.setField029(rs.getString("Field029"));
      entity.setField030(rs.getString("Field030"));
      entity.setField031(rs.getDouble("Field031"));
      entity.setField032(rs.getDouble("Field032"));
      entity.setField033(rs.getDouble("Field033"));
      entity.setField034(rs.getDouble("Field034"));
      entity.setField035(rs.getString("Field035"));
      entity.setField036(rs.getString("Field036"));
      entity.setField037(rs.getString("Field037"));
      entity.setField038(rs.getDouble("Field038"));
      entity.setField039(rs.getString("Field039"));
      entity.setField040(formatDayString(rs.getTimestamp("Field040")));
      entity.setField041(rs.getDouble("Field041"));
      entity.setField042(rs.getString("Field042"));
      entity.setField043(rs.getString("Field043"));
      entity.setField044(rs.getString("Field044"));
      entity.setField045(rs.getString("Field045"));
      entity.setField046(rs.getDouble("Field046"));
      entity.setField047(rs.getString("Field047"));
      entity.setField048(rs.getDouble("Field048"));
      entity.setField049(rs.getString("Field049"));
      entity.setField050(rs.getString("Field050"));
      entity.setField051(rs.getDouble("Field051"));
      entity.setField052(rs.getString("Field052"));
      entity.setField053(rs.getString("Field053"));
      entity.setField054(rs.getString("Field054"));
      entity.setField055(rs.getString("Field055"));
      entity.setField056(rs.getString("Field056"));
      entity.setField057(rs.getString("Field057"));
      entity.setField058(rs.getString("Field058"));
      entity.setField059(rs.getDouble("Field059"));
      entity.setField060(rs.getDouble("Field060"));
      entity.setField061(rs.getDouble("Field061"));
      entity.setField062(rs.getDouble("Field062"));
      entity.setField063(rs.getDouble("Field063"));
      entity.setField064(rs.getDouble("Field064"));
      entity.setField065(rs.getDouble("Field065"));
      entity.setField066(rs.getDouble("Field066"));
      entity.setField067(rs.getDouble("Field067"));
      entity.setField068(rs.getDouble("Field068"));
      entity.setField069(rs.getDouble("Field069"));
      entity.setField070(rs.getDouble("Field070"));
      entity.setField071(rs.getDouble("Field071"));
      entity.setField072(rs.getDouble("Field072"));
      entity.setField073(rs.getDouble("Field073"));
      entity.setField074(rs.getDouble("Field074"));
      entity.setField075(rs.getDouble("Field075"));
      entity.setField076(rs.getDouble("Field076"));
      entity.setField077(rs.getDouble("Field077"));
      entity.setField078(rs.getDouble("Field078"));
      entity.setField079(rs.getDouble("Field079"));
      entity.setField080(rs.getDouble("Field080"));
      entity.setField081(rs.getDouble("Field081"));
      entity.setField082(rs.getDouble("Field082"));
      entity.setField083(formatDayString(rs.getTimestamp("Field083")));
      entity.setField084(rs.getString("Field084"));
      entity.setField085(formatDayString(rs.getTimestamp("Field085")));
      entity.setField086(rs.getString("Field086"));
      entity.setField087(rs.getDouble("Field087"));
      entity.setField088(rs.getString("Field088"));
      entity.setField089(rs.getString("Field089"));
      entity.setField090(formatDayString(rs.getTimestamp("Field090")));
      entity.setField091(formatDayString(rs.getTimestamp("Field091")));
      entity.setField092(rs.getString("Field092"));
      entity.setField093(formatDayString(rs.getTimestamp("Field093")));
      entity.setField094(rs.getString("Field094"));
      entity.setField095(rs.getString("Field095"));
      entity.setField096(rs.getString("Field096"));
      entity.setField097(formatDayString(rs.getTimestamp("Field097")));
      entity.setField098(rs.getDouble("Field098"));
      entity.setField099(rs.getString("Field099"));
      entity.setField100(formatDayString(rs.getTimestamp("Field100")));
      entity.setField101(rs.getString("Field101"));
      entity.setField102(formatDayString(rs.getTimestamp("Field102")));
      entity.setField103(rs.getDouble("Field103"));
      entity.setField104(rs.getString("Field104"));
      entity.setField105(rs.getString("Field105"));
      entity.setField106(formatDayString(rs.getTimestamp("Field106")));
      entity.setField107(rs.getString("Field107"));
    } else {
      entity.setTransactNo(rs.getDouble("TransactNo"));

    }

    return entity;
  }
}
