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
package com.gemstone.org.jgroups;

/**
 * GemStoneAddition. This class is used in SUSPECT_WITH_ORIGIN events to
 * hold both the suspected members and the origin of suspicion
 *
 * @author bruce
 */
public class SuspectMember
{
  /** the source of suspicion */
  public Address whoSuspected;
  
  /** suspected member */
  public Address suspectedMember;
  
  /** create a new SuspectMember */
  public SuspectMember(Address whoSuspected, Address suspectedMember) {
    this.whoSuspected = whoSuspected;
    this.suspectedMember = suspectedMember;
  }
  
  @Override // GemStoneAddition
  public String toString() {
    return "{source="+whoSuspected+"; suspect="+suspectedMember+"}";
  }
  
  @Override // GemStoneAddition
  public int hashCode() {
    return this.suspectedMember.hashCode();
  }
  
  @Override // GemStoneAddition
  public boolean equals(Object other) {
    if ( !(other instanceof SuspectMember) ) {
      return false;
    }
    return this.suspectedMember.equals(((SuspectMember)other).suspectedMember);
  }
}
