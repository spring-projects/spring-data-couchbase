package org.springframework.data.couchbase.domain;

import jakarta.validation.constraints.NotNull;
import org.springframework.data.annotation.Id;
import org.springframework.data.couchbase.core.mapping.Field;

public class MyPerson {
    @NotNull
    @Id
    public String id;

    @Field
    public Object myObject;

    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("MyPerson:{");
        sb.append("id:");
        sb.append(id);
        sb.append(", myObject:");
        sb.append(myObject);
        sb.append("}");
        return sb.toString();
    }
}
