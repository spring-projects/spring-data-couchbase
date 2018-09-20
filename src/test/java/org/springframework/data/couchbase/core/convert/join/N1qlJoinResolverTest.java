package org.springframework.data.couchbase.core.convert.join;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import java.lang.annotation.Annotation;

import com.couchbase.client.java.CouchbaseBucket;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.query.FetchType;
import org.springframework.data.couchbase.core.query.HashSide;
import org.springframework.data.couchbase.core.query.N1qlJoin;
import org.springframework.data.util.TypeInformation;
import org.springframework.data.couchbase.core.convert.join.N1qlJoinResolver.N1qlJoinResolverParameters;

/**
 * Unit tests for {@link N1qlJoinResolver}
 */
public class N1qlJoinResolverTest {
    static CouchbaseTemplate template;
    static TypeInformation<Entity> entity;
    static TypeInformation<Entity> associatedEntity;
    static String entityClassName;

    @BeforeClass
    public static void setup() {
        template = mock(CouchbaseTemplate.class);
        CouchbaseBucket bucket = mock(CouchbaseBucket.class);
        when(bucket.name()).thenReturn("B");
        when(template.getCouchbaseBucket()).thenReturn(bucket);
        CouchbaseConverter converter = mock(CouchbaseConverter.class);
        when(converter.getTypeKey()).thenReturn("_class");
        when(template.getConverter()).thenReturn(converter);
        entity = mock(TypeInformation.class);
        doReturn(Entity.class).when(entity).getType();
        associatedEntity = mock(TypeInformation.class);
        doReturn(Entity.class).when(associatedEntity).getType();
        entityClassName = Entity.class.getName();
    }

    private static N1qlJoin createAnnotation(String on, String where, String index, String rightIndex, HashSide hashSide, String[] keys) {
        N1qlJoin joinDefinition = new N1qlJoin() {

            @Override
            public Class<? extends Annotation> annotationType() {
                return N1qlJoin.class;
            }

            @Override
            public String on() {
                return on;
            }

            @Override
            public FetchType fetchType() {
                return FetchType.IMMEDIATE;
            }

            @Override
            public String where() {
                return where;
            }

            @Override
            public String index() {
                return index;
            }

            @Override
            public String rightIndex() {
                return rightIndex;
            }

            @Override
            public HashSide hashside() {
                return hashSide;
            }

            @Override
            public String[] keys() {
                return keys;
            }
        };
        return joinDefinition;
    }

    static public class Entity {
    }

    @Test
    public void shouldBuildQueryWithIndex() {
        N1qlJoin joinDefinition = createAnnotation("A=B", "", "leftIndex", "", HashSide.NONE, new String[0]);
        N1qlJoinResolverParameters parameters = new N1qlJoinResolverParameters(joinDefinition, "mydoc", entity, associatedEntity);
        String statement = N1qlJoinResolver.buildQuery(template, parameters);
        String expected = "SELECT META(rks).id AS _ID, META(rks).cas AS _CAS, (rks).*  FROM `B` lks USE INDEX(leftIndex) JOIN B rks ON A=B" +
                " AND lks._class = \"" + entityClassName + "\"" + " AND " +
                "rks._class = \"" + entityClassName + "\" WHERE META(lks).id=\"mydoc\"";
        assertEquals(statement, expected);
    }

    @Test
    public void shouldBuildQueryWithRightIndex() {
        N1qlJoin joinDefinition = createAnnotation("A=B", "", "", "rightIndex", HashSide.NONE, new String[0]);
        N1qlJoinResolverParameters parameters = new N1qlJoinResolverParameters(joinDefinition, "mydoc", entity, associatedEntity);
        String statement = N1qlJoinResolver.buildQuery(template, parameters);
        String expected = "SELECT META(rks).id AS _ID, META(rks).cas AS _CAS, (rks).*  FROM `B` lks JOIN B rks USE INDEX(rightIndex) ON A=B" +
                " AND lks._class = \"" + entityClassName + "\"" + " AND " +
                "rks._class = \"" + entityClassName + "\" WHERE META(lks).id=\"mydoc\"";
        assertEquals(statement, expected);
    }

    @Test
    public void shouldBuildQueryWithHashProbe() {
        N1qlJoin joinDefinition = createAnnotation("A=B", "", "", "", HashSide.PROBE, new String[0]);
        N1qlJoinResolverParameters parameters = new N1qlJoinResolverParameters(joinDefinition, "mydoc", entity, associatedEntity);
        String statement = N1qlJoinResolver.buildQuery(template, parameters);
        String expected = "SELECT META(rks).id AS _ID, META(rks).cas AS _CAS, (rks).*  FROM `B` lks JOIN B rks USE HASH(probe) ON A=B" +
                " AND lks._class = \"" + entityClassName + "\"" + " AND " +
                "rks._class = \"" + entityClassName + "\" WHERE META(lks).id=\"mydoc\"";
        assertEquals(statement, expected);
    }

    @Test
    public void shouldBuildQueryWithHashBuild() {
        N1qlJoin joinDefinition = createAnnotation("A=B", "", "", "", HashSide.BUILD, new String[0]);
        N1qlJoinResolverParameters parameters = new N1qlJoinResolverParameters(joinDefinition, "mydoc", entity, associatedEntity);
        String statement = N1qlJoinResolver.buildQuery(template, parameters);
        String expected = "SELECT META(rks).id AS _ID, META(rks).cas AS _CAS, (rks).*  FROM `B` lks JOIN B rks USE HASH(build) ON A=B" +
                " AND lks._class = \"" + entityClassName + "\"" + " AND " +
                "rks._class = \"" + entityClassName + "\" WHERE META(lks).id=\"mydoc\"";
        assertEquals(statement, expected);
    }

    @Test
    public void shouldBuildQueryWithKeys() {
        N1qlJoin joinDefinition = createAnnotation("A=B", "", "", "", HashSide.NONE, new String[]{"x", "y"});
        N1qlJoinResolverParameters parameters = new N1qlJoinResolverParameters(joinDefinition, "mydoc", entity, associatedEntity);
        String statement = N1qlJoinResolver.buildQuery(template, parameters);
        String expected = "SELECT META(rks).id AS _ID, META(rks).cas AS _CAS, (rks).*  FROM `B` lks JOIN B rks USE KEYS [\"x\",\"y\"] ON A=B" +
                " AND lks._class = \"" + entityClassName + "\"" + " AND " +
                "rks._class = \"" + entityClassName + "\" WHERE META(lks).id=\"mydoc\"";
        assertEquals(statement, expected);
    }

    @Test
    public void shouldBuildQueryWithWhere() {
        N1qlJoin joinDefinition = createAnnotation("A=B", "C=D", "", "", HashSide.NONE, new String[0]);
        N1qlJoinResolverParameters parameters = new N1qlJoinResolverParameters(joinDefinition, "mydoc", entity, associatedEntity);
        String statement = N1qlJoinResolver.buildQuery(template, parameters);
        String expected = "SELECT META(rks).id AS _ID, META(rks).cas AS _CAS, (rks).*  FROM `B` lks JOIN B rks ON A=B" +
                " AND lks._class = \"" + entityClassName + "\"" + " AND " +
                "rks._class = \"" + entityClassName + "\" WHERE META(lks).id=\"mydoc\" AND C=D";
        assertEquals(statement, expected);
    }

    @Test
    public void shouldBuildQueryWithMultipleHints() {
        N1qlJoin joinDefinition = createAnnotation("A=B", "", "leftIndex", "rightIndex", HashSide.BUILD, new String[]{"x"});
        N1qlJoinResolverParameters parameters = new N1qlJoinResolverParameters(joinDefinition, "mydoc", entity, associatedEntity);
        String statement = N1qlJoinResolver.buildQuery(template, parameters);
        String expected = "SELECT META(rks).id AS _ID, META(rks).cas AS _CAS, (rks).*  FROM `B` lks USE INDEX(leftIndex) JOIN B rks USE INDEX(rightIndex)" +
                " HASH(build) KEYS [\"x\"] ON A=B AND lks._class = \"" + entityClassName + "\"" + " AND " +
                "rks._class = \"" + entityClassName + "\" WHERE META(lks).id=\"mydoc\"";
        assertEquals(statement, expected);
    }
}
