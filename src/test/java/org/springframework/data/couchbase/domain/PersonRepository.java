package org.springframework.data.couchbase.domain;

//import com.couchbase.client.java.repository.CouchbaseRepository;

import org.springframework.data.couchbase.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

//import org.springframework.data.couchbase.repository.CouchbaseRepository;

public interface PersonRepository extends CrudRepository<Person, String> {

    public  List<Person> findByLastname(String lastname);

    @Query("#{#n1ql.selectEntity} where firstname = 'Reba' and lastname = $last")
    public  List<Person> any(@Param("last") String any);

    @Query("#{#n1ql.selectEntity} where firstname = 'Reba' and lastname = 'McIntyre'")
    public  List<Person> none();

    @Query("#{#n1ql.selectEntity} where firstname = 'Reba' and lastname = $1")
    public  List<Person> one(@Param("last") String any);

    @Query("#{#n1ql.selectEntity} where lastname = $2 and firstname = $1")
    public  List<Person> two(@Param("one") String one, @Param("two") String two);

    public  List<Person> findByFirstname(String firstname);

    public  List<Person> findByFirstnameLike(String firstname);

    public  List<Person> findByFirstnameIsNull();

    public  List<Person> findByFirstnameIsNotNull();

    public  List<Person> findByFirstnameNotLike(String firstname);

    public  List<Person> findByFirstnameStartingWith(String firstname);

    public  List<Person> findByFirstnameEndingWith(String firstname);

    public  List<Person> findByFirstnameContaining(String firstname);

    public  List<Person> findByFirstnameNotContaining(String firstname);

    public  List<Person> findByFirstnameBetween(String firstname1, String firstname2);

    public  List<Person> findByFirstnameIn(String... firstnames);

    public  List<Person> findByFirstnameNotIn(String... firstnames);

    public  List<Person> findByFirstnameTrue(Object... o);

    public  List<Person> findByFirstnameFalse(Object... o);

    List<Person> findByFirstnameAndLastname(String firstname, String lastname);

    List<Person> findByFirstnameOrLastname(String firstname, String lastname);

    /////////////////////////////////////////////////////////////////////////


    <S extends Person> S save(S var1);

    <S extends Person> Iterable<S> saveAll(Iterable<S> var1);

    Optional<Person> findById(UUID var1);

    boolean existsById(UUID var1);

    Iterable<Person> findAll();

    //Iterable<Person> findAllById(Iterable<String> var1);

    long count();

    void deleteById(UUID var1);

    void delete(Person var1);

    void deleteAll(Iterable<? extends Person> var1);

    void deleteAll();

    /*
    default public void ftsTravel(){
        try {
            Config config = new Config();
            Cluster cluster =  config.couchbaseCluster( config.couchbaseClusterEnvironment() );
            Bucket b=cluster.bucket("travel-sample");
            Collection coll=b.defaultCollection();
            final SearchResult search_result = cluster
                    .searchQuery("hotels",
                            SearchQuery.match("international").field("airportname"),
                            SearchOptions.searchOptions().fields("city","airportname").highlight("city","country")
                    );

            System.out.println(search_result);
            for (SearchRow row : search_result.rows())

            {         System.out.println("Found row: " + row);     }
        } catch (CouchbaseException ex)

        {     ex.printStackTrace(); }


    }
    */

}