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
package query.common.joinsWithOR.data;

import java.io.Serializable;
import java.util.UUID;

import query.common.joinsWithOR.data.LoginCommunityAssignment;

public class LoginCommunityAssignmentImpl implements LoginCommunityAssignment, Serializable  {
    private static final long serialVersionUID = 1L;
    
    private UUID id;
    private UUID communityId;
    private String loginId;

    /**
     * @param id
     * @param communityId
     * @param loginId
     */
    public LoginCommunityAssignmentImpl(UUID id, UUID communityId, String loginId) {
        this.id = id;
        this.communityId = communityId;
        this.loginId = loginId;
    }

    /** {@inheritDoc} */
    public UUID getId() {
        return id;
    }

    /** {@inheritDoc} */
    public UUID getCommunityId() {
        return communityId;
    }

    /** {@inheritDoc} */
    public String getLoginId() {
        return loginId;
    }

}
