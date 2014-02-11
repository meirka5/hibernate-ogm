package org.hibernate.ogm.test.utils;

import java.util.Map;

import org.hibernate.SessionFactory;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.ogm.cfg.OgmConfiguration;
import org.hibernate.ogm.datastore.redis.impl.RedisDatastoreProvider;
import org.hibernate.ogm.datastore.spi.DatastoreProvider;
import org.hibernate.ogm.grid.AssociationKey;
import org.hibernate.ogm.grid.EntityKey;
import org.hibernate.ogm.grid.RowKey;
import org.hibernate.ogm.options.generic.document.AssociationStorageType;
import org.hibernate.ogm.options.navigation.context.GlobalContext;

public class RedisTestHelper implements TestableGridDialect {

	@Override
	public Map<String, Object> extractEntityTuple(SessionFactory sessionFactory, EntityKey key) {
		return getEntityMap( sessionFactory ).get( key );
	}

	private static Map<EntityKey, Map<String, Object>> getEntityMap(SessionFactory sessionFactory) {
		RedisDatastoreProvider castProvider = getProvider( sessionFactory );
		return castProvider.getEntityMap();
	}

	private static RedisDatastoreProvider getProvider(SessionFactory sessionFactory) {
		DatastoreProvider provider = ( (SessionFactoryImplementor) sessionFactory ).getServiceRegistry().getService( DatastoreProvider.class );
		if ( !( RedisDatastoreProvider.class.isInstance( provider ) ) ) {
			throw new RuntimeException( "Not testing with RedisDatastoreProvider, cannot extract underlying map." );
		}
		return RedisDatastoreProvider.class.cast( provider );
	}

	private static Map<AssociationKey, Map<RowKey, Map<String, Object>>> getAssociationCache(SessionFactory sessionFactory) {
		RedisDatastoreProvider castProvider = getProvider( sessionFactory );
		return castProvider.getAssociationsMap();
	}

	@Override
	public boolean backendSupportsTransactions() {
		return false;
	}

	@Override
	public void dropSchemaAndDatabase(SessionFactory sessionFactory) {
		RedisDatastoreProvider castProvider = getProvider( sessionFactory );
		castProvider.removeAll();
	}

	@Override
	public Map<String, String> getEnvironmentProperties() {
		return null;
	}

	@Override
	public long getNumberOfEntities(SessionFactory sessionFactory) {
		return getEntityMap( sessionFactory ).size();
	}

	@Override
	public long getNumberOfAssociations(SessionFactory sessionFactory) {
		return getAssociationCache( sessionFactory ).size();
	}

	@Override
	public long getNumberOfAssociations(SessionFactory sessionFactory, AssociationStorageType type) {
		return 0;
	}

	@Override
	public long getNumberOEmbeddedCollections(SessionFactory sessionFactory) {
		return 0;
	}

	@Override
	public GlobalContext<?, ?> configureDatastore(OgmConfiguration configuration) {
		return null;
	}

}
