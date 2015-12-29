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

import hydra.Log;

import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.util.HashSet;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import sql.SQLHelper;
import util.TestException;

public class MusicPopulator {
  static final String VERSION_STRINGS[] = { "studio", "live", "unplugged", "funky remix", "rap mix", "disco mix" };
  static final int MAX_RATING = 5;

  static public Integer getColMaxInt(Connection conn, String tableName, String colName) throws SQLException {
    Integer retVal = null;
    ResultSet rs = conn.createStatement().executeQuery("select max(" + colName + ") from " + tableName);
    if (rs != null) {
      rs.next();
      retVal = new Integer(rs.getInt(1));
    }
    rs.close();
    return retVal;
  }

  static public HashSet<String> getStringVals(Connection conn, String tableName, String colName) throws SQLException {
    HashSet<String> retSet = null;
    ResultSet rs = conn.createStatement().executeQuery("select " + colName + " from " + tableName);
    if (rs != null) {
      retSet = new HashSet<String>();
      while (rs.next()) {
        retSet.add(rs.getString(1));
      }
    }
    return retSet;
  }

  /*
   * Add generated data into the Music database
   */
  static public void addToTables(Connection conn, int artistCount, int albumCount, int copyrightOwnerCount,
      int songCount, int genreCount, int tagCount, int tracksCount) {
    Log.getLogWriter().info(
        "Entered addToTables with artistCount=" + artistCount + ", albumCount=" + albumCount + ", copyrightOwnerCount="
            + copyrightOwnerCount + ", songCount=" + songCount + ", genreCount=" + genreCount + ", tagCount="
            + tagCount + ", tracksCount=" + tracksCount);
    try {
      // First, collect information on what's already in the database
      int artistExistingMax = getColMaxInt(conn, "music.artist", "artist_id");
      int albumExistingMax = getColMaxInt(conn, "music.album", "album_id");
      int copyrightOwnerExistingMax = getColMaxInt(conn, "music.copyright_owner", "owner_id");
      int songExistingMax = getColMaxInt(conn, "music.song", "song_id");
      int copyrightExistingMax = getColMaxInt(conn, "music.copyright", "copyright_id");
      int genreMax = getColMaxInt(conn, "music.genre", "genre_id");
      HashSet<String> existingTags = getStringVals(conn, "music.tag", "tag_name");
      Log.getLogWriter().info(
          "addToTables found artistExistingMax=" + artistExistingMax + ", albumExistingMax=" + albumExistingMax
              + ", copyrightOwnerExistingMax=" + copyrightOwnerExistingMax + ", songExistingMax=" + songExistingMax
              + ", genreMax=" + genreMax);

      // Now start adding data
      // Start with artist table
      PreparedStatement ps = conn.prepareStatement("insert into music.artist(artist_id, artist_name) values(?,?)");
      for (int i = artistExistingMax + 1; i <= (artistExistingMax + artistCount); i++) {
        ps.setInt(1, i);
        ps.setString(2, "artist" + i);
        ps.executeUpdate();
      }
      if (!conn.getAutoCommit()) {
        conn.commit();
      }

      // Now add album table
      ps = conn
          .prepareStatement("insert into music.album(album_id, album_name, primary_artist_id, publish_date) values(?,?,?,?)");
      for (int i = 1; i <= albumCount; i++) {
        ps.setInt(1, albumExistingMax + i);
        ps.setString(2, "album" + (albumExistingMax + i));
        ps.setInt(3, artistExistingMax + 1 + (i % artistCount));
        try {
          ps.setDate(4, new Date(new SimpleDateFormat("yyyy/MM/dd").parse("1999/12/12").getTime()));
        } catch (ParseException e) {
          throw new TestException("Invalid date in MusicPopulator");
        }
        ps.executeUpdate();
      }
      if (!conn.getAutoCommit()) {
        conn.commit();
      }

      // Now add the copyright owners
      ps = conn.prepareStatement("insert into music.copyright_owner(owner_id, owner_name) values(?,?)");
      for (int i = 1; i <= copyrightOwnerCount; i++) {
        ps.setInt(1, copyrightOwnerExistingMax + i);
        ps.setString(2, "copyrightOwner" + (copyrightOwnerExistingMax + i));
        ps.executeUpdate();
      }
      if (!conn.getAutoCommit()) {
        conn.commit();
      }

      // Now add the song table
      ps = conn
          .prepareStatement("insert into music.song(song_id, artist_id,song_name,version,copyright_owner_id,rating) values(?,?,?,?,?,?)");
      for (int i = songExistingMax + 1; i <= (songExistingMax + songCount); i++) {
        ps.setInt(1, i);
        // randomly pick an artist_id, copyright_owner_id

        ps.setInt(2, i % artistCount + artistExistingMax + 1);
        ps.setString(3, "song" + i);
        ps.setString(4, VERSION_STRINGS[i % VERSION_STRINGS.length]);
        ps.setInt(5, i % copyrightOwnerCount + copyrightOwnerExistingMax + 1);
        ps.setInt(6, i % MAX_RATING);
        ps.executeUpdate();
      }
      if (!conn.getAutoCommit()) {
        conn.commit();
      }

      // Now add the copyright table
      ps = conn
          .prepareStatement("insert into music.copyright(copyright_id,song_id,owner_id,copyright_fee,copyright_notes) values (?,?,?,?,?)");
      for (int i = 1; i <= songCount; i++) {
        ps.setInt(1, copyrightExistingMax + i);
        ps.setInt(2, songExistingMax + i);
        ps.setInt(3, i % copyrightOwnerCount + copyrightOwnerExistingMax + 1);
        ps.setInt(4, 10 + (i % 10));
        ps.setString(5, "note for cw " + copyrightExistingMax + i);
        ps.executeUpdate();
      }
      if (!conn.getAutoCommit()) {
        conn.commit();
      }

      // add the genres
      ps = conn.prepareStatement("insert into music.genre(genre_id,genre_name) values(?,?)");
      for (int i = genreMax + 1; i <= (genreMax + genreCount); i++) {
        ps.setInt(1, i);
        ps.setString(2, "genre" + i);
        ps.executeUpdate();
      }
      if (!conn.getAutoCommit()) {
        conn.commit();
      }

      // add the tags
      ps = conn.prepareStatement("insert into music.tag(tag_name) values(?)");
      for (int i = existingTags.size() + 1; i <= (existingTags.size() + tagCount); i++) {
        ps.setString(1, "tag" + i);
        ps.executeUpdate();
      }
      if (!conn.getAutoCommit()) {
        conn.commit();
      }

      // add the music tracks table
      ps = conn
          .prepareStatement("insert into music.tracks(album_id,disk_number,track_number,duration_secs,song_id,genre_id,track_price_cents, track_price_to_copyright_owner_cents, track_price_to_artist_cents) values (?,?,?,?,?,?,?,?,?)");
      // Compute the number of tracks per album, so we get a balance
      int tracksPerAlbum = tracksCount / albumCount;
      int totalTrackCtr = 0;
      for (int albumID = albumExistingMax+1; albumID <= albumCount + albumExistingMax; albumID++ ) {
        for (int trackNumOnDisk=1; trackNumOnDisk <= tracksPerAlbum && totalTrackCtr < tracksCount; trackNumOnDisk++) {
          totalTrackCtr++;
          //Log.getLogWriter().info("generating track album="+albumID+", disk=1, track="+trackNumOnDisk+" for "+totalTrackCtr);
          ps.setInt(1, albumID);
          ps.setInt(2, 1);  // disk 1
          ps.setInt(3, trackNumOnDisk);
          ps.setInt(4, 240); // hard-code 4 minutes per song
          ps.setInt(5, totalTrackCtr % songCount + songExistingMax + 1);
          ps.setInt(6, totalTrackCtr % genreCount + genreMax + 1);
          ps.setInt(7, 99);  // track price
          ps.setInt(8, 10);  // track price to copyright owner
          ps.setInt(9, 25);  // track price to artist
          ps.executeUpdate();
        }
      }
      // If we've got some tracks not on albums yet, add 2nd disks to albums until we get there.
      if (totalTrackCtr < tracksCount) {
        for (int albumID = albumExistingMax+1; albumID <= albumCount + albumExistingMax; albumID++ ) {
          for (int trackNumOnDisk=1; trackNumOnDisk <= tracksPerAlbum && totalTrackCtr < tracksCount; trackNumOnDisk++) {
            totalTrackCtr++;
            Log.getLogWriter().info("generating track album="+albumID+", disk=2, track="+trackNumOnDisk+" for "+totalTrackCtr);
            ps.setInt(1, albumID);
            ps.setInt(2, 2);  // disk 2
            ps.setInt(3, trackNumOnDisk);
            ps.setInt(4, 240); // hard-code 4 minutes per song
            ps.setInt(5, totalTrackCtr % songCount + songExistingMax + 1);
            ps.setInt(6, totalTrackCtr % genreCount + genreMax + 1);
            ps.setInt(7, 99);  // track price
            ps.setInt(8, 10);  // track price to copyright owner
            ps.setInt(9, 25);  // track price to artist
            ps.executeUpdate();
          }
        }

      }
      if (!conn.getAutoCommit()) {
        conn.commit();
      }

    } catch (SQLException sqle) {
      SQLHelper.handleSQLException(sqle);
    }
  }
}
