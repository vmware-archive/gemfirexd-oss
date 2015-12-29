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
package sql.testDBs;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Random;

import sql.SQLHelper;
import sql.alterTable.AlterTableTest;
import sql.SQLTest;

;

public class MusicHelper extends SQLTest {
  protected static MusicHelper musicHelper;
  protected static final Random rand = new Random();

  public static synchronized void HydraTask_initialize() {
    if (musicHelper == null) {
      musicHelper = new MusicHelper();
      musicHelper.initialize();
    }
  }

  protected void initialize() {
    // Load several parameters, such as hasDerbyServer
    super.initialize();
  }

  public static void HydraTask_populateMusicDB() {
    if (musicHelper == null) {
      musicHelper = new MusicHelper();
    }
    try {
      if (hasDerbyServer) {
        Connection dConn = musicHelper.getDiscConnection();
        MusicPopulator.addToTables(dConn, MusicPrms.getNumGeneratedArtists(), MusicPrms.getNumGeneratedAlbums(),
            MusicPrms.getNumGeneratedCopyrightOwners(), MusicPrms.getNumGeneratedSongs(), MusicPrms
                .getNumGeneratedGenres(), MusicPrms.getNumGeneratedTags(), MusicPrms.getNumGeneratedTracks());
        dConn.close();
      }
      Connection gConn = musicHelper.getGFEConnection();
      MusicPopulator.addToTables(gConn, MusicPrms.getNumGeneratedArtists(), MusicPrms.getNumGeneratedAlbums(),
          MusicPrms.getNumGeneratedCopyrightOwners(), MusicPrms.getNumGeneratedSongs(), MusicPrms
              .getNumGeneratedGenres(), MusicPrms.getNumGeneratedTags(), MusicPrms.getNumGeneratedTracks());
      gConn.close();
    } catch (SQLException sqle) {
      SQLHelper.handleSQLException(sqle);
    }

  }

}
