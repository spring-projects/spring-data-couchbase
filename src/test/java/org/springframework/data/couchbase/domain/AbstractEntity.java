package org.springframework.data.couchbase.domain;

//import com.mysema.query.annotations.QuerySupertype;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.annotation.Id;
import org.springframework.data.couchbase.core.mapping.Document;
import org.springframework.data.couchbase.core.mapping.id.GeneratedValue;
import org.springframework.data.couchbase.core.mapping.id.GenerationStrategy;

import java.util.UUID;

/**
 * @author Oliver Gierke
 *
 */
//@QuerySupertype
@Document
//@Data
public class AbstractEntity {
    static protected Logger log = LoggerFactory.getLogger(AbstractEntity.class);

    // this id does not seem to get set.
    @Id
    @GeneratedValue(strategy = GenerationStrategy.UNIQUE)
    private UUID id;

    public AbstractEntity(){
        //id= UUID.randomUUID();
        //if(id == null)
        //    id = new String("123");
    }
    /**
     * set the id
     */
    public void setId(UUID id) {
        this.id=id;
    }
    /**
     * @return the id
     */
    public UUID getId() {
        return id;
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {

        if (this == obj) {
            return true;
        }

        if (this.id == null || obj == null || !(this.getClass().equals(obj.getClass()))) {
            return false;
        }

        AbstractEntity that = (AbstractEntity) obj;

        return this.id.equals(that.getId());
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        return id == null ? 0 : id.hashCode();
    }
}

