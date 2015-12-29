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
package com.gemstone.org.jgroups.protocols;

import com.gemstone.org.jgroups.Event;
import com.gemstone.org.jgroups.Message;

/**
 * FRAG3 is a version of FRAG2 that is used below GMS to allow for larger
 * membership view messages than will fit in a single packet.
 * 
 * @author bruces
 *
 */
public class FRAG3 extends FRAG2 {

  @Override // GemStoneAddition  
  public String getName() {
      return "FRAG3";
  }

  @Override
  public void down(Event evt) {
    if (evt.getType() == Event.MSG) {
      Message msg=(Message)evt.getArg();
      if (msg.getHeader("FRAG2") != null) {
        // the message is already fragmented - don't mess with it
        passDown(evt);
        return;
      }
    }
    super.down(evt);
  }
}
