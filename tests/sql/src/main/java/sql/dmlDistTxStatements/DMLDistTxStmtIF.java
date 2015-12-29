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
package sql.dmlDistTxStatements;

import java.sql.Connection;

public interface DMLDistTxStmtIF {

  /*
  public void insert(Connection dConn, Connection gConn, SQLException derbyse, SQLException gfxdse);
  public void update(Connection dConn, Connection gConn, SQLException derbyse, SQLException gfxdse);
  public void delete(Connection dConn, Connection gConn, SQLException derbyse, SQLException gfxdse);
  public void query(Connection dConn, Connection gConn, SQLException derbyse, SQLException gfxdse);
  */
  
  /**
   * perform insert operation in gemfirexd
   * @param gConn
   * @return true when operation successful with out conflict exception
   *         false when operation gets conflict exception (lock not held)
   */
  public boolean insertGfxd(Connection gConn, boolean withDerby);
  public boolean updateGfxd(Connection gConn, boolean withDerby);
  public boolean deleteGfxd(Connection gConn, boolean withDerby);
  public boolean queryGfxd(Connection gConn, boolean withDerby);
  
  public void insertDerby(Connection dConn, int index);
  public void updateDerby(Connection dConn, int index);
  public void deleteDerby(Connection dConn, int index);
  public void queryDerby(Connection dConn, int index);
  
  public void populate(Connection dConn, Connection gConn);
}
