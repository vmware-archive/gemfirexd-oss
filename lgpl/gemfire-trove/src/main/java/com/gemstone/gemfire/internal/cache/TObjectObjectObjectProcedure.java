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
package com.gemstone.gemfire.internal.cache;

/**
 * Interface for procedures that take three Object parameters.
 * 
 * @author swale
 * @since 7.0
 */
public interface TObjectObjectObjectProcedure {

  /**
   * Executes this procedure. A false return value indicates that the
   * application executing this procedure should not invoke this procedure
   * again.
   * 
   * @param a
   *          an <code>Object</code> value
   * @param b
   *          an <code>Object</code> value
   * @param c
   *          an <code>Object</code> value
   * 
   * @return true if additional invocations of the procedure are allowed.
   */
  public boolean execute(Object a, Object b, Object c);
}
