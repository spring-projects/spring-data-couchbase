package org.springframework.data.couchbase.domain;

//import com.couchbase.client.core.env.Authenticator;
//import com.couchbase.client.core.env.PasswordAuthenticator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.data.couchbase.config.BeanNames;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.RemoveResult;
import org.springframework.data.couchbase.repository.Query;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.lang.reflect.Method;
import java.util.*;
import java.util.stream.Collectors;

@RestController
public class HomeController {
    @Autowired
    private ApplicationContext context;
    private CouchbaseTemplate couchbaseTemplate;

    private Logger log = LoggerFactory.getLogger(HomeController.class);
    static String finderMethods=finderMethods();
    static String deleteMethods=deleteMethods();

    static void waitAsecond(){ try { Thread.sleep(1000); } catch (InterruptedException e) {} }

    public HomeController(){
        super();
    }

    @RequestMapping("/")
    public @ResponseBody String greeting() {

         Person person = new Person("firstname", "lastname");  // firstname="" will produce firstname : null

        try {
            //couchbaseClientFactory = new SimpleCouchbaseClientFactory(connectionString(), authenticator(), bucketName());
            //CouchbaseConverter couchbaseConverter = new MappingCouchbaseConverter();
            couchbaseTemplate = (CouchbaseTemplate) context.getBean(BeanNames.COUCHBASE_TEMPLATE);
        }catch(Exception e){
            e.printStackTrace();
        }

        Person modified = couchbaseTemplate.upsertById(Person.class).one(person);
        System.out.println("modified: "+modified);
        waitAsecond();
        /*
        //try out repository save
        MyService.getRepository().deleteById(person.getId().toString());  // this needs .toString()
        waitAsecond();
        MyService.getRepository().save(modified);
        waitAsecond();
        */

        Collection<Person> persons = MyService.getRepository().findByLastname(person.getLastname());
        System.out.println("persons: "+persons);

        persons = MyService.getRepository().any("McIntyre");
        System.out.println("++++++++++++++++++++++++++++++++++++++ any: "+persons);

        // why does this compile, run, and fail with doc with id not found?
        // there is no way to know what the ID type parameter is, so Collection<?> is allowed.
        // intellij gives a warning
        // "unchecked call to all(Collection<ID>) as a member of raw time (i.e. andy Collection)
        // casting the arg will give a compile-time error

        try {
            if (!persons.isEmpty())
                if (!(persons.iterator().next().getClass().isAssignableFrom(UUID.class)))
                    System.out.println("WARNING: not the right collection");
            // don't make this call as it will fail
            //couchbaseTemplate.removeById().all(/*(Collection<String>) */persons);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // this is the proper call
        couchbaseTemplate.removeById().all((Collection<String>) persons.stream().map( p -> p.getId().toString()).collect(Collectors.toList()) );

        // ok, now put that one back
        modified = couchbaseTemplate.upsertById(Person.class).one(person);
        System.out.println("modified: "+modified);
        waitAsecond();
        //persons = MyService.getRepository().findByLastname(person.getLastname());
        persons = couchbaseTemplate.findByQuery(Person.class).all();
        person = persons.iterator().next();
        System.out.println("person 91 :"+person);


        // delete via repository
        MyService.getRepository().deleteById(person.getId().toString());  // this needs .toString()

        // ok, now put that one back
        modified = couchbaseTemplate.upsertById(Person.class).one(person);
        waitAsecond();

        persons=MyService.getRepository().findByLastname(person.getLastname());
        if(!persons.isEmpty())
            person=persons.iterator().next();
        else
            System.out.println("person matching lastname="+person.getLastname()+" not found!!");
        Person p=couchbaseTemplate.findById(Person.class).one(person.getId().toString());
        RemoveResult r=couchbaseTemplate.removeById().one(person.getId().toString());

        MyService.getInstance().doWork();
        waitAsecond();

        return findAll();

    }

    @RequestMapping("/findByLastname")
    public @ResponseBody String findByLastname(@RequestParam(value = "name", defaultValue = "Base") String name) {
        List<Person> p=MyService.getRepository().findByLastname(name);
        return toString("name="+name,p,finderMethods);
    }

    @RequestMapping("/findByFirstname")
    public @ResponseBody String findByFirstname(@RequestParam(value = "name", defaultValue = "Couch") String name) {
        Iterable<Person> p=MyService.getRepository().findByFirstname(name);
        return toString("name="+name,p,finderMethods);
    }

    @RequestMapping("/findById")
    public @ResponseBody String findById(@RequestParam(value = "name", defaultValue = "Couch") String name) {
        Iterable<Person> p=MyService.getRepository().findByFirstname(name);
        Optional<Person> person=MyService.getRepository().findById(p.iterator().next().getId().toString());
        List<Person> people=new ArrayList<Person>();
        people.add(person.get());
        return toString("name="+name,people,finderMethods);
    }

    @RequestMapping("/findAll")
    public @ResponseBody String findAll() {
        Iterable<Person> p=MyService.getRepository().findAll();
        return toString("all",p,finderMethods);
    }

    @RequestMapping("/findByFirstnameContaining")
    public @ResponseBody String findByFirstnameContaining(@RequestParam(value = "name", defaultValue = "ouch") String name) {
        Iterable<Person> p=MyService.getRepository().findByFirstnameContaining(name);
        return toString("name="+name,p,finderMethods);
    }

    @RequestMapping("/findByFirstnameIsNull")
    public @ResponseBody String findByFirstnameIsNull() {
        Iterable<Person> p=MyService.getRepository().findByFirstnameIsNull();
        return toString("isNull",p,finderMethods);
    }


    @RequestMapping("/findByFirstnameIsNotNull")
    public @ResponseBody String findByFirstnameIsNotNull() {
        Iterable<Person> p=MyService.getRepository().findByFirstnameIsNotNull();
        return toString("isNotNull",p,finderMethods);
    }

//    @RequestMapping("/findByFirstnameIsMissing")
//    public @ResponseBody String findByFirstnameIsMissing() {
//        List<Person> p=MyService.getRepository().findByFirstnameIsMissing();
//        return toString(p);
//    }

//    @RequestMapping("/findByFirstnameIsNotMissing")
//    public @ResponseBody String findByFirstnameIsNotMissing() {
//        List<Person> p=MyService.getRepository().findByFirstnameIsNotMissing();
//        return toString(p);
//    }


//    @RequestMapping("/findByFirstnameIsValued")
//    public @ResponseBody String findByFirstnameIsValued() {
//        List<Person> p=MyService.getRepository().findByFirstnameIsValued();
//        return toString(p);
//    }

//    @RequestMapping("/findByFirstnameIsNotValued")
//    public @ResponseBody String findByFirstnameIsNotValued() {
//        List<Person> p=MyService.getRepository().findByFirstnameIsNotValued();
//        return toString(p);
//    }

    @RequestMapping("/findByFirstnameNotContaining")
    public @ResponseBody String findByFirstnameNotContaining(@RequestParam(value = "name", defaultValue = "Couch") String name) {
        Iterable<Person> p=MyService.getRepository().findByFirstnameNotContaining(name);
        return toString("name="+name,p,finderMethods);
    }

    @RequestMapping("/findByFirstnameLike")
    public @ResponseBody String findByFirstnameLike(@RequestParam(value = "name", defaultValue = "%ouch%") String name) {
        Iterable <Person> p=MyService.getRepository().findByFirstnameLike(name);
        return toString("name="+name,p,finderMethods);
    }

    @RequestMapping("/findByFirstnameNotLike")
    public @ResponseBody String findByFirstnameNotLike(@RequestParam(value = "name", defaultValue = "%ouch%") String name) {
        Iterable<Person> p=MyService.getRepository().findByFirstnameNotLike(name);
        return toString("name="+name,p,finderMethods);
    }
    @RequestMapping("/findByFirstnameStartingWith")
    public @ResponseBody String findByFirstnameStartingWith(@RequestParam(value = "name", defaultValue = "Cou") String name) {
        Iterable<Person> p=MyService.getRepository().findByFirstnameStartingWith(name);
        return toString("name="+name,p,finderMethods);
    }

    @RequestMapping("/findByFirstnameEndingWith")
    public @ResponseBody String findByFirstnameEndingWith(@RequestParam(value = "name", defaultValue = "ouch") String name) {
        Iterable<Person> p=MyService.getRepository().findByFirstnameEndingWith(name);
        return toString("name="+name,p,finderMethods);
    }

    @RequestMapping("/findByFirstnameBetween")
    public @ResponseBody String findByFirstnameBetween(
            @RequestParam(value = "begin", defaultValue = "B") String begin,
            @RequestParam(value = "end", defaultValue = "D") String end) {
        Iterable<Person> p=MyService.getRepository().findByFirstnameBetween(begin,end);
        return toString("begin="+begin+" end="+end,p,finderMethods);
    }
/*
    @RequestMapping("/notbetween")
    public @ResponseBody String findByFirstnameNotBetween() {
        List<Person> p=MyService.getRepository().findByFirstnameNotBetween("B", "D");
        return ( p.size() > 0 ? p.get(0).toString() : "no matching records");
    }

 */

    @RequestMapping("/findByFirstnameIn")
    public @ResponseBody String findByFirstnameIn(
            @RequestParam(value = "name", defaultValue = "Couch,Kenny,Madonna" ) String names ) {
        Iterable<Person> p=MyService.getRepository().findByFirstnameIn(names.split(","));
        return toString("names="+names,p,finderMethods);
    }

    @RequestMapping("/findByFirstnameNotIn")
    public @ResponseBody String findByFirstnameNotIn(
            @RequestParam(value = "name", defaultValue = "Couch,Kenny,Madonna" ) String names) {
        Iterable<Person> p=MyService.getRepository().findByFirstnameNotIn(names.split(","));
        return toString("names="+names,p,finderMethods);
    }

    @RequestMapping("/findByFirstnameTrue")
    public @ResponseBody String findByFirstnameTrue() {
        Iterable<Person> p=MyService.getRepository().findByFirstnameTrue();
        return toString("true",p,finderMethods);
    }

    @RequestMapping("/findByFirstnameFalse")
    public @ResponseBody String findByFirstnameFalse() {
        Iterable<Person> p=MyService.getRepository().findByFirstnameFalse();
        return toString("false",p, finderMethods);
    }

    @RequestMapping("/findStringN1qlNone")
    public @ResponseBody String findStringN1qlNone(@RequestParam(value = "name", defaultValue = "Couch") String name) {
        Iterable<Person> p=MyService.getRepository().none();
        String n1ql="";
        try {
            Method m = PersonRepository.class.getMethod("none", new Class[] { });
            n1ql=m.getAnnotation(Query.class).value();
        }catch(NoSuchMethodException nsme){}
        return toString("n1ql="+n1ql+"<br>name="+name,p,finderMethods);
    }
    @RequestMapping("/findStringN1qlOne")
    public @ResponseBody String findStringN1qlOne(@RequestParam(value = "name", defaultValue = "Couch") String name) {

        Iterable<Person> p=MyService.getRepository().one(name);
        String n1ql="";
        try {
            Method m = PersonRepository.class.getMethod("one", new Class[] { String.class });
            n1ql=m.getAnnotation(Query.class).value();
        }catch(NoSuchMethodException nsme){}
        return toString("n1ql="+n1ql+"<br>name="+name,p,finderMethods);
    }

    @RequestMapping("/findStringN1qlTwo")
    public @ResponseBody String findStringN1qlTwo(@RequestParam(value = "name", defaultValue = "Couch") String name,
            @RequestParam(value = "lastname", defaultValue = "Base") String lastname) {
        Iterable<Person> p=MyService.getRepository().two(name,lastname);
        String n1ql="";
        try {
            Method m = PersonRepository.class.getMethod("two", new Class[] { String.class, String.class });
            n1ql=m.getAnnotation(Query.class).value();
        }catch(NoSuchMethodException nsme){}
        return toString("n1ql="+n1ql+"<br>name="+name,p,finderMethods);
    }
    //////////////////////////////////////////////////////////////////////////////////

    /*
    <S extends Person> Iterable<S> saveAll(Iterable<S> var1);
    Iterable<Person> findAllById(Iterable<UUID> var1);
    long count();
     */

    @RequestMapping("/findExistsById")
    public @ResponseBody String findExistsById(
            @RequestParam(value = "name", defaultValue = "Reba" ) String name) {
        Iterable<Person> p=MyService.getRepository().findByFirstname(name);
        UUID id=p.iterator().next().getId();
        boolean found=MyService.getRepository().existsById(id.toString());
        return toString("name="+name+", id="+id,(found ? p : null),finderMethods);
    }

    @RequestMapping("/deleteById")
    public @ResponseBody String deleteById(
            @RequestParam(value = "name", defaultValue = "Reba" ) String name) {
        Iterable<Person> p=MyService.getRepository().findByFirstname(name);
        UUID id=p.iterator().next().getId();
        MyService.getRepository().deleteById(id.toString());
        return toString("name="+name+", id="+id,p,deleteMethods);
    }

    @RequestMapping("/delete")
    public @ResponseBody String delete(  @RequestParam(value = "name", defaultValue = "Reba" ) String name) {
        Iterable<Person> p=MyService.getRepository().findByFirstname(name);
        MyService.getRepository().delete(p.iterator().next());
        return toString("name="+name,p,deleteMethods);
    }

    @RequestMapping("/deleteAllIn")
    public @ResponseBody String deleteAllIn(
            @RequestParam(value = "name", defaultValue = "Couch,Kenny,Madonna" ) String names ) {
        Iterable<Person> p=MyService.getRepository().findByFirstnameIn(names.split(","));
        MyService.getRepository().deleteAll(p);
        return toString("names="+names,p,deleteMethods);
    }

    @RequestMapping("/deleteAll")
    public @ResponseBody String deleteAll() {
        MyService.getRepository().deleteAll();
        return toString("all",null,deleteMethods);
    }

    public static String toString(String args, Iterable<Person> persons, String methods){
        StringBuffer sb=new StringBuffer();

        sb.append("<table>");
        sb.append("<tr><td>"+args+"</td></tr>");

        sb.append("<tr><td>");
        if(persons == null ) {
            sb.append( "no results");
        }else {
            //String s=persons.stream().collect(StringBuilder::new,  StringBuilder::append, StringBuilder::append).toString();
            String s=persons.toString();
            sb.append(s.replaceAll("Person","</td></tr><tr><td>Person"));
        }
        sb.append("</td></tr>");
        if(methods == deleteMethods){
            sb.append("<tr><td>Remaining users</td></tr>");
            Iterable<Person> people = MyService.getRepository().findAll();
            String s=people.toString();
            sb.append(s.replaceAll("Person","</td></tr><tr><td>Person"));
        }
        sb.append("</table>");
        sb.append(methods);
        return sb.toString();
    }


    static private String finderMethods(){
        StringBuilder sb=new StringBuilder();
        sb.append("<table>");
        for(String s:getFinderMethods()){
            sb.append("<tr>\n");
            sb.append("  <td><a href="+s+">"+s+"</a></td>\n");
            sb.append("</tr>\n");
        }
        sb.append("<table>");
        return sb.toString();
    }

    private static List<String> getFinderMethods(){
        return Arrays.stream(HomeController.class.getMethods())
                .filter(m -> m.getName().startsWith("find"))
                .map(Method::getName)
                .collect(Collectors.toList());
    }

    static private String deleteMethods(){
        StringBuilder sb=new StringBuilder();
        sb.append("<table>");
        for(String s:getDeleteMethods()){
            sb.append("<tr>\n");
            sb.append("  <td><a href="+s+">"+s+"</a></td>\n");
            sb.append("</tr>\n");
        }
        sb.append("<table>");
        return sb.toString();
    }

    private static List<String> getDeleteMethods(){
        return Arrays.stream(HomeController.class.getMethods())
                .filter(m -> m.getName().startsWith("delete"))
                .map(Method::getName)
                .collect(Collectors.toList());
    }

//   public static Authenticator authenticator() {
//        return PasswordAuthenticator.create("Administrator", "password");
//    }
    /*
    public static String connectionString() {
        return "localhost:11210";
    }
    public static String bucketName() {
        return "travel-sample";
    }
     */
}