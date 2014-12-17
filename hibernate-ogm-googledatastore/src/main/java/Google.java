import org.hibernate.ogm.datastore.mongodb.options.navigation.impl.MongoDBEntityContextImpl;
import org.hibernate.ogm.datastore.mongodb.options.navigation.impl.MongoDBGlobalContextImpl;
import org.hibernate.ogm.datastore.mongodb.options.navigation.impl.MongoDBPropertyContextImpl;
import org.hibernate.ogm.datastore.spi.DatastoreConfiguration;
import org.hibernate.ogm.options.navigation.spi.ConfigurationContext;

/**
 * Created by ABhat on 14 Dec 2014.
 */
public class Google implements DatastoreConfiguration<MongoDBGlobalContextImpl> {
    @Override
    public MongoDBGlobalContextImpl getConfigurationBuilder(ConfigurationContext context) {
        return context.createGlobalContext( MongoDBGlobalContextImpl.class, MongoDBEntityContextImpl.class, MongoDBPropertyContextImpl.class );

    }
}
