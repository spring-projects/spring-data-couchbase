package org.springframework.data.couchbase.domain;

import java.util.List;

import org.springframework.data.couchbase.core.mapping.Document;

@Document
public class MutableUser extends User{
    public MutableUser(String id, String firstname, String lastname) {
        super(id, firstname, lastname);
    }

    private Address address;

    private MutableUser subuser;

    private List<String> roles;

    public void setRoles(List<String> roles) {
        this.roles = roles;
    }

    public List<String> getRoles() {
        return roles;
    }

    public void setAddress(Address address) {
        this.address = address;
    }

    public Address getAddress() {
        return address;
    }

    public void setSubuser(MutableUser subuser) {
        this.subuser = subuser;
    }

    public MutableUser getSubuser() {
        return subuser;
    }
}
