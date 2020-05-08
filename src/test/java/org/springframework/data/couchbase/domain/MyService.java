package org.springframework.data.couchbase.domain;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class MyService {

    private Logger log = LoggerFactory.getLogger(MyService.class);

    private PersonRepository repository;
    public static MyService theService;

    public MyService(PersonRepository repository) {
        System.out.println("new Initialized MyService");
        this.repository = repository;
        theService = this;
    }

    static public MyService getInstance() {
        return theService;
    }

    static public PersonRepository getRepository() {
        return getInstance().repository;
    }

    public void doWork() {
        log.info("======================== doWork");
        getRepository().deleteAll();
        Person person= upsertPerson("Couch", "Base"); // this one first so it has time to be save in CB
        //cluster.waitUntilReady(Duration.parse("PT10S")        );
        upsertPerson("Dave", "Thomas");
        upsertPerson("Garth", "Brooks");
        upsertPerson("Randy", "Travis");
        upsertPerson("Kenny", "Rogers");
        upsertPerson("Reba", "McIntyre");
        upsertPerson(null, "Kramer");
        upsertPerson("Madonna", null);
        upsertPerson("Simon", "Shuster");
        upsertPerson("", "MyFirstNameIsNull");
        upsertPerson("MyLastNameIsNull","");
    }

    public  Person upsertPerson(String firstname, String lastname) {

        Iterable<Person> persons = repository.findByFirstnameOrLastname( firstname, lastname);
        for(Person p:persons) {
            try {
                System.out.println("deleting: "+p);
                repository.deleteById(p.getId().toString()); // needs toString()
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        Person person = new Person(firstname,lastname);
        try {
            Object r=repository.save(person);
            log.info("new person is \n" + r);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return person;
    }
    //public void doFtsQuery(){
    //     repository.ftsTravel();
    //}
}


