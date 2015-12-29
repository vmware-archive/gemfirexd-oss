///////////////////////////////////////////////////////////////////////////////
// Copyright (c) 2001, Eric D. Friedman All Rights Reserved.
//
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public
// License as published by the Free Software Foundation; either
// version 2.1 of the License, or (at your option) any later version.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
///////////////////////////////////////////////////////////////////////////////
/*
 * Contains changes for GemFireXD distributed data platform.
 *
 * Portions Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
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
 * This object hashing strategy uses the System.identityHashCode
 * method to provide identity hash codes.  These are identical to the
 * value produced by Object.hashCode(), even when the type of the
 * object being hashed overrides that method.
 * 
 * Created: Sat Aug 17 11:13:15 2002
 *
 * @author Eric Friedman
 * @version $Id: TObjectIdentityHashingStrategy.java,v 1.2 2002/08/18 19:14:28 ericdf Exp $
 */

public final class TObjectIdentityHashingStrategy implements TObjectHashingStrategy {
  
    private static final long serialVersionUID = -5193908370672076866L;

    /**
     * Delegates hash code computation to the System.identityHashCode(Object) method.
     *
     * @param object for which the hashcode is to be computed
     * @return the hashCode
     */
    public final int computeHashCode(Object object) {
        return System.identityHashCode(object);
    }

    /**
     * Compares object references for equality.
     *
     * @param o1 an <code>Object</code> value
     * @param o2 an <code>Object</code> value
     * @return true if o1 == o2
     */
    public final boolean equals(Object o1, Object o2) {
        return o1 == o2;
    }

    // GemStoneAddition
    private static final TObjectIdentityHashingStrategy instance =
        new TObjectIdentityHashingStrategy();

    // GemStoneAddition
    public static TObjectIdentityHashingStrategy getInstance() {
      return instance;
    }
} // TObjectIdentityHashingStrategy
