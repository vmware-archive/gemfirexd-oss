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
/**
 * 
 */
package query.common.joinsWithOR.data;

import java.io.Serializable;
import java.util.UUID;

import query.common.joinsWithOR.data.Community;

public class CommunityImpl implements Community, Serializable {
    private static final long serialVersionUID = 1L;
    
    private UUID id;
    private String name;
    private String ownerId;
    private String communityType;
    private String description;
    private int ordinal;

    
    /**
     * @param id
     * @param name
     * @param ownerId
     * @param communityType
     * @param description
     * @param ordinal
     */
    public CommunityImpl(UUID id, String name, String ownerId, String communityType, String description, int ordinal) {
        this.id = id;
        this.name = name;
        this.ownerId = ownerId;
        this.communityType = communityType;
        this.description = description;
        this.ordinal = ordinal;
    }

    /** {@inheritDoc} */
    public UUID getId() {
        return id;
    }

    /** {@inheritDoc} */
    public String getName() {
        return name;
    }

    /** {@inheritDoc} */
    public String getOwnerId() {
        return ownerId;
    }

    /** {@inheritDoc} */
    public String getCommunityType() {
        return communityType;
    }

    /** {@inheritDoc} */
    public String getDescription() {
        return description;
    }

    /** {@inheritDoc} */
    public int getOrdinal() {
        return ordinal;
    }

}
