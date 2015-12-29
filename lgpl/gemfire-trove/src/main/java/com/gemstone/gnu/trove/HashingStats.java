/*
 * Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package com.gemstone.gnu.trove;

/**
 * Interface to gather hash statistics for trove map reads/writes etc.
 * 
 * @author swale
 * @since gfxd 1.0
 */
public interface HashingStats {

  /**
   * Returns the current NanoTime or, if clock stats are disabled, zero.
   */
  long getNanoTime();

  /**
   * Increment the statistic indicating a hash collision.
   */
  void incQueryResultsHashCollisions();

  /**
   * Increment the statistic for time required to lookup map for hash
   * collisions.
   */
  void endQueryResultsHashCollisionProbe(long start);
}
