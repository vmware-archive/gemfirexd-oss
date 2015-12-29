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
package com.gemstone.gemfire.internal.tools.gfsh.app.cache.index;

/**
 * LookupService throws LookupServiceException if it encounters
 * an error from the underlying GemFire communications mechanism.
 */
class LookupServiceException extends RuntimeException
{
	private static final long serialVersionUID = 1L;

	/**
	 * Constructs a new LookupServiceException.
	 */
    LookupServiceException()
    {
        super();
    }

    /**
     * Constructs a new LookupServiceException exception with the specified detail message.
     *
     * @param  message the detail message (which is saved for later retrieval
     *         by the {@link #getMessage()} method).
     */
    public LookupServiceException(String message)
    {
        super(message, null);
    }

    /**
     * Constructs a new LookupServiceException exception with the specified detail message and
     * cause.
     * <p>Note that the detail message associated with
     * <code>cause</code> is <i>not</i> automatically incorporated in
     * this exception's detail message.
     *
     * @param  message the detail message (which is saved for later retrieval
     *         by the {@link #getMessage()} method).
     * @param  cause the cause (which is saved for later retrieval by the
     *         {@link #getCause()} method).  (A <tt>null</tt> value is
     *         permitted, and indicates that the cause is nonexistent or
     *         unknown.)
     */
    public LookupServiceException(String message, Throwable cause)
    {
        super(message, cause);
    }

    /**
     * Constructs a new LookupServiceException exception with the specified cause.
     * <p>Note that the detail message associated with
     * <code>cause</code> is <i>not</i> automatically incorporated in
     * this exception's detail message.
     *
     * @param  cause the cause (which is saved for later retrieval by the
     *         {@link #getCause()} method).  (A <tt>null</tt> value is
     *         permitted, and indicates that the cause is nonexistent or
     *         unknown.)
     */
    public LookupServiceException(Throwable cause)
    {
        super(cause);
    }
}
