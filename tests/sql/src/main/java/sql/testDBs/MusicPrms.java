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

import hydra.BasePrms;
import hydra.Log;

public class MusicPrms extends BasePrms {
  static {
    setValues( MusicPrms.class );
  }

  /*
   * (Integer) Number of generated artists desired in the music database
   */
  public static Long numGeneratedArtists;
  public static int getNumGeneratedArtists() {
    if (tasktab().get(MusicPrms.numGeneratedArtists) != null) {
      Log.getLogWriter().info("TMP_DEBUG: we DO have numGeneratedArtists in the task table...");
      return tasktab().intAt(MusicPrms.numGeneratedArtists);
    } else {
      return tab().intAt(MusicPrms.numGeneratedArtists);
    }
  }
  /*
   * (Integer) Number of generated albums desired in the music database
   */
  public static Long numGeneratedAlbums;
  public static int getNumGeneratedAlbums() {
    if (tasktab().get(MusicPrms.numGeneratedAlbums) != null) {
      return tasktab().intAt(MusicPrms.numGeneratedAlbums);
    } else {
      return tab().intAt(MusicPrms.numGeneratedAlbums);
    }
  }
  /*
   * (Integer) Number of generated genres desired in the music database
   */
  public static Long numGeneratedGenres;
  public static int getNumGeneratedGenres() {
    if (tasktab().get(MusicPrms.numGeneratedGenres) != null) {
      return tasktab().intAt(MusicPrms.numGeneratedGenres);
    } else {
      return tab().intAt(MusicPrms.numGeneratedGenres);
    }
  }
  /*
   * (Integer) Number of generated tags desired in the music database
   */
  public static Long numGeneratedTags;
  public static int getNumGeneratedTags() {
    if (tasktab().get(MusicPrms.numGeneratedTags) != null) {
      return tasktab().intAt(MusicPrms.numGeneratedTags);
    } else {
      return tab().intAt(MusicPrms.numGeneratedTags);
    }
  }
  /*
   * (Integer) Number of generated copyright owners desired in the music database
   */
  public static Long numGeneratedCopyrightOwners;
  public static int getNumGeneratedCopyrightOwners() {
    if (tasktab().get(MusicPrms.numGeneratedCopyrightOwners) != null) {
      return tasktab().intAt(MusicPrms.numGeneratedCopyrightOwners);
    } else {
      return tab().intAt(MusicPrms.numGeneratedCopyrightOwners);
    }
  }
  /*
   * (Integer) Number of generated tracks desired in the music database
   */
  public static Long numGeneratedTracks;
  public static int getNumGeneratedTracks() {
    if (tasktab().get(MusicPrms.numGeneratedTracks) != null) {
      return tasktab().intAt(MusicPrms.numGeneratedTracks);
    } else {
      return tab().intAt(MusicPrms.numGeneratedTracks);
    }
  }
  /*
   * (Integer) Number of generated songs desired in the music database
   */
  public static Long numGeneratedSongs;
  public static int getNumGeneratedSongs() {
    if (tasktab().get(MusicPrms.numGeneratedSongs) != null) {
      return tasktab().intAt(MusicPrms.numGeneratedSongs);
    } else {
      return tab().intAt(MusicPrms.numGeneratedSongs);
    }
  }

}
