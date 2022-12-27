package org.springframework.data.couchbase.domain;

import lombok.Data;
import org.springframework.data.couchbase.core.mapping.Field;

@Data
public class Asset {
    @Field
    private String id;
    @Field
    private String desc;
}

