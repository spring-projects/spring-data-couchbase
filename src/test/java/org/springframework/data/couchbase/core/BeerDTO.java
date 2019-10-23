package org.springframework.data.couchbase.core;


/**
 * Test DTO for projecting from {@link CouchbaseTemplate}.
 *
 * @author Subhashni Balakrishnan
 */
public class BeerDTO{
    private String name;

    private String description;

    public BeerDTO(String name, String description) {
        this.name = name;
        this.description = description;
    }

    public BeerDTO setName(String name) {
        this.name = name;
        return this;
    }

    public String getName() { return name; }

    public BeerDTO setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getDescription() { return description; }
}
