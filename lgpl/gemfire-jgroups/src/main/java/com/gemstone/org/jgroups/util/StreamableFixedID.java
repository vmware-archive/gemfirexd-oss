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
package com.gemstone.org.jgroups.util;

/**
 * This is a parallel class to DataSerializableFixedID providing fixed ID
 * serialization for specific JGroups classes for better performance.
 * 
 * @author bschuchardt
 *
 */
public interface StreamableFixedID extends VersionedStreamable {

  public static final byte JGROUPS_VIEW = 1;
  public static final byte JGROUPS_JOIN_RESP = 2;
  public static final byte IP_ADDRESS = 70;

  /** returns the ID used to identify the class when serialized with GemFire DataSerialization */ 
  int getDSFID();
  
}
