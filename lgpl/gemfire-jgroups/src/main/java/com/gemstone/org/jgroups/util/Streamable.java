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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Implementations of Streamable can add their state directly to the output stream, enabling them to bypass costly
 * serialization
 * @author Bela Ban
 * @version $Id: Streamable.java,v 1.2 2005/07/25 16:21:47 belaban Exp $
 */
public interface Streamable {

    /** Write the entire state of the current object (including superclasses) to outstream.
     * Note that the output stream <em>must not</em> be closed */
    void writeTo(DataOutputStream out) throws IOException;

    /** Read the state of the current object (including superclasses) from instream
     * Note that the input stream <em>must not</em> be closed */
    void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException;
}
