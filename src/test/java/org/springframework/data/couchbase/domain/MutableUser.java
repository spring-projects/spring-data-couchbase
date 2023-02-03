package org.springframework.data.couchbase.domain;

import lombok.Getter;
import lombok.Setter;
import org.springframework.data.couchbase.core.mapping.Document;

import java.util.List;

@Document
public class MutableUser extends User{
    public MutableUser(String id, String firstname, String lastname) {
        super(id, firstname, lastname);
    }
    
    @Getter
    @Setter
    private Address address;

    @Getter
    @Setter
    private MutableUser subuser;

    @Getter
    @Setter
    private List<String> roles;
    
    
}
