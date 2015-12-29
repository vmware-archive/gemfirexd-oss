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
import hydra.HydraRuntimeException;
import hydra.Log;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import perffmwk.HistogramStats;

public class CashflowDao extends JdbcDaoBase {

    private SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddHHmmss");

    private long start;
    private QueryPerfStats querystats;
    private HistogramStats histogram;

    public CashflowDao(){
        super.getConnection();
    }

    public CashflowDao(Connection connection, QueryPerfStats qstats,
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

    private String convertDate(String stringDate){

        String year = stringDate.substring(0, 4);
        String month = stringDate.substring(4, 6);
        String day = stringDate.substring(6, 8);
        String hour = stringDate.substring(8, 10);
        String minute = stringDate.substring(10, 12);
        String second = stringDate.substring(12, 14);

        String date = year + "-" + month + "-" + day + " " + hour + ":" + minute + ":" + second;

        return date;
    }

    public List<CashflowEntity> select(Double tran, String fld05, String fld47, boolean isValue) throws SQLException {

        // build statement
        //start = querystats.startPrepare();
        // format yyyy-MM-dd HH:mm:ss
        String date = convertDate(fld47);

        Date fld047 = null;
        Date fld048 = null;
        try {
            fld047 = sdf1.parse(date);
            fld048 = sdf1.parse("2009-01-01 00:00:00");
        } catch (ParseException e) {
          throw new HydraRuntimeException("Parse error", e);
        }

        SqlBuilder sql = super.createSqlBuilder(400);
        if(isValue){
            sql.append("SELECT * FROM Cashflow");
        } else {
            sql.append("SELECT TransactNo, LegNo, CashflowNo FROM Cashflow");
        }
        sql.append("WHERE");
        sql.append("TransactNo = ?", tran);
        sql.append("AND Field005 = ?", fld05);
        sql.append("AND Field006 = 'AAA'");
        sql.append("AND Field007 = 'AAA'");
        sql.append("AND Field014 = 1111");
        sql.append("AND Field045 = 111111111");
        sql.append("AND Field046 = 111111111");
        sql.append("AND Field051 = 111111111");
        sql.append("AND Field053 = 'AAAAAAAAAA'");
        sql.append("AND Field054 = 'AAAAAAAAAA'");
        sql.appendIfNotNull("AND Field047 >= ?", fld047);
        sql.append("AND Field048 >= ?", fld048);
        sql.append("ORDER BY TransactNo, LegNo, CashflowNo, Field010, Field011");
        super.prepareStatemet(sql);
        //querystats.endPrepare(start, histogram);

        // executeQuery
        start = querystats.startQuery();
        ResultSet rs = ps.executeQuery();
        querystats.endQuery(start, histogram);

        // ORM
        //start = querystats.startORM();
        List<CashflowEntity> ret = new ArrayList<CashflowEntity>();
        try {
            while (rs.next()){
                CashflowEntity entity = mappingCashflow(rs,isValue);
                ret.add(entity);
            }

            close();
        } catch (SQLException e){
            super.sqlTransrator(e, "ret.size()=" + ret.size());
        }
        //querystats.endORM(start, histogram);

        if (fineEnabled) {
          Log.getLogWriter().fine(" Result set size = " + ret.size());
        }
        return ret;
    }

    private CashflowEntity mappingCashflow(ResultSet rs, boolean isValue)
    throws SQLException {
        CashflowEntity entity = new CashflowEntity();
        entity.setId(new CashflowEntityId());

        if (isValue) {
            entity.setField001(rs.getDouble("Field001"));
            entity.setTransactNo(rs.getDouble("TransactNo"));
            entity.setLegNo(rs.getDouble("LegNo"));
            entity.setCashflowNo(rs.getDouble("CashflowNo"));
            entity.setField005(rs.getString("Field005"));
            entity.setField006(rs.getString("Field006"));
            entity.setField007(rs.getString("Field007"));
            entity.setField008(formatDayString(rs.getTimestamp("Field008")));
            entity.setField009(formatDayString(rs.getTimestamp("Field009")));
            entity.setField010(formatDayString(rs.getTimestamp("Field010")));
            entity.setField011(formatDayString(rs.getTimestamp("Field011")));
            entity.setField012(formatDayString(rs.getTimestamp("Field012")));
            entity.setField013(formatDayString(rs.getTimestamp("Field013")));
            entity.setField014(rs.getDouble("Field014"));
            entity.setField015(rs.getDouble("Field015"));
            entity.setField016(rs.getDouble("Field016"));
            entity.setField017(rs.getDouble("Field017"));
            entity.setField018(rs.getDouble("Field018"));
            entity.setField019(rs.getDouble("Field019"));
            entity.setField020(rs.getDouble("Field020"));
            entity.setField021(rs.getDouble("Field021"));
            entity.setField022(rs.getDouble("Field022"));
            entity.setField023(rs.getDouble("Field023"));
            entity.setField024(rs.getDouble("Field024"));
            entity.setField025(rs.getString("Field025"));
            entity.setField026(rs.getDouble("Field026"));
            entity.setField027(rs.getDouble("Field027"));
            entity.setField028(rs.getDouble("Field028"));
            entity.setField029(rs.getDouble("Field029"));
            entity.setField030(rs.getDouble("Field030"));
            entity.setField031(rs.getDouble("Field031"));
            entity.setField032(rs.getDouble("Field032"));
            entity.setField033(rs.getDouble("Field033"));
            entity.setField034(rs.getDouble("Field034"));
            entity.setField035(rs.getDouble("Field035"));
            entity.setField036(rs.getDouble("Field036"));
            entity.setField037(rs.getDouble("Field037"));
            entity.setField038(rs.getDouble("Field038"));
            entity.setField039(formatDayString(rs.getTimestamp("Field039")));
            entity.setField040(rs.getString("Field040"));
            entity.setField041(formatDayString(rs.getTimestamp("Field041")));
            entity.setField042(rs.getString("Field042"));
            entity.setField043(rs.getDouble("Field043"));
            entity.setField044(rs.getDouble("Field044"));
            entity.setField045(rs.getDouble("Field045"));
            entity.setField046(rs.getDouble("Field046"));
            entity.setField047(formatDayString(rs.getTimestamp("Field047")));
            entity.setField048(formatDayString(rs.getTimestamp("Field048")));
            entity.setField049(rs.getString("Field049"));
            entity.setField050(rs.getDouble("Field050"));
            entity.setField051(rs.getDouble("Field051"));
            entity.setField052(rs.getDouble("Field052"));
            entity.setField053(rs.getString("Field053"));
            entity.setField054(rs.getString("Field054"));
            entity.setField055(rs.getString("Field055"));
            entity.setField056(rs.getDouble("Field056"));
            entity.setField057(rs.getString("Field057"));
            entity.setField058(rs.getString("Field058"));
            entity.setField059(rs.getString("Field059"));
            entity.setField060(rs.getString("Field060"));
            entity.setField061(formatDayString(rs.getTimestamp("Field061")));
            entity.setField062(formatDayString(rs.getTimestamp("Field062")));
            entity.setField063(rs.getDouble("Field063"));
            entity.setField064(rs.getDouble("Field064"));
            entity.setField065(rs.getDouble("Field065"));
            entity.setField066(rs.getDouble("Field066"));
            entity.setField067(rs.getString("Field067"));
            entity.setField068(rs.getDouble("Field068"));
            entity.setField069(rs.getString("Field069"));
            entity.setField070(formatDayString(rs.getTimestamp("Field070")));
        } else {
            entity.setTransactNo(rs.getDouble("TransactNo"));
            entity.setLegNo(rs.getDouble("LegNo"));
            entity.setCashflowNo(rs.getDouble("CashflowNo"));
        }

        return entity;
    }
}
