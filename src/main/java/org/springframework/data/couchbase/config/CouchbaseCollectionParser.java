package org.springframework.data.couchbase.config;

import com.couchbase.client.java.Collection;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.StringUtils;
import org.w3c.dom.Element;

public class CouchbaseCollectionParser extends AbstractSingleBeanDefinitionParser {
    /**
     * The <code>cluster-ref</code> attribute in a bucket definition defines the cluster to build from.
     */
    public static final String CLUSTER_REF_ATTR = "cluster-ref";

    /**
     * The <code>bucketName</code> attribute in a bucket definition defines the name of the bucket to open.
     */
    public static final String BUCKETNAME_ATTR = "bucketName";

    public static final String COLLECTIONNAME_ATTR = "collectionName";
    /**
     * Resolve the bean ID and assign a default if not set.
     *
     * @param element the XML element which contains the attributes.
     * @param definition the bean definition to work with.
     * @param parserContext encapsulates the parsing state and configuration.
     * @return the ID to work with.
     */
    @Override
    protected String resolveId(final Element element, final AbstractBeanDefinition definition, final ParserContext parserContext) {
        String id = super.resolveId(element, definition, parserContext);
        return StringUtils.hasText(id) ? id : BeanNames.COUCHBASE_CLUSTER;
    }

    /**
     * Defines the bean class that will be constructed.
     *
     * @param element the XML element which contains the attributes.
     * @return the class type to instantiate.
     */
    @Override
    protected Class getBeanClass(final Element element) {
        return Collection.class;
    }

    /**
     * Parse the bean definition and build up the bean.
     *
     * @param element the XML element which contains the attributes.
     * @param bean the builder which builds the bean.
     */
    @Override
    protected void doParse(final Element element, final BeanDefinitionBuilder bean) {
        String clusterRef = element.getAttribute(CLUSTER_REF_ATTR);
        if (!StringUtils.hasText(clusterRef)) {
            clusterRef = BeanNames.COUCHBASE_CLUSTER;
        }
        bean.addConstructorArgReference(clusterRef);

        String bucketName = element.getAttribute(BUCKETNAME_ATTR);
        if (StringUtils.hasText(bucketName)) {
            bean.addConstructorArgValue(bucketName);
        }

        String collectionName = element.getAttribute(COLLECTIONNAME_ATTR);
        if (StringUtils.hasText(collectionName)) {
            bean.addConstructorArgValue(collectionName);
        }
    }
}
