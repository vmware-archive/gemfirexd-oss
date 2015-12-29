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

package com.gemstone.gemfire.cache.query.internal;

import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.InternalGemFireError;



public class Support
{
    public static final boolean ASSERTIONS_ENABLED = true;

    private static final int OTHER = 0;
    private static final int STATE = 1;
    private static final int ARG = 2;
    
    
    public static void assertArg(boolean b, String message)
    {
        if (!ASSERTIONS_ENABLED)
            return;
        Assert(b, message, ARG);
    }

    public static void assertState(boolean b, String message)
    {
        if (!ASSERTIONS_ENABLED)
            return;
        Assert(b, message, STATE);
    }
    
    
    public static void Assert(boolean b)
    {
        if (!ASSERTIONS_ENABLED)
            return;
        Assert(b, "", OTHER);
    }

    public static void Assert(boolean b, String message)
    {
        if (!ASSERTIONS_ENABLED)
            return;
        Assert(b, message, OTHER);
    }
    
    public static void assertionFailed(String message)
    {
        assertionFailed(message, OTHER);
    }
    
    public static void assertionFailed()
    {
        assertionFailed("", OTHER);
    }
    
    private static void Assert(boolean b, String message, int type)
    {
        if (!b)
            assertionFailed(message, type);
    }    

    private static void assertionFailed(String message, int type)
    {
        switch (type)
        {
            case ARG:
                throw new IllegalArgumentException(message);
            case STATE:
                throw new IllegalStateException(message);
            default:
                throw new InternalGemFireError(LocalizedStrings.Support_ERROR_ASSERTION_FAILED_0.toLocalizedString(message));
        }
        
            // com.gemstone.persistence.jdo.GsRuntime.notifyCDebugger(null);
    }
    

}
