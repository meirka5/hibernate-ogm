package org.hibernate.ogm.test.utils;

import java.util.Map;
import java.util.Set;

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

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

public class RedisTestHelper implements TestableGridDialect {

	@Override
	@SuppressWarnings("unchecked")
	public Map<String, Object> extractEntityTuple(SessionFactory sessionFactory, EntityKey key) {
		RedisDatastoreProvider provider = getProvider( sessionFactory );
		JedisPool pool = provider.getPool();
		Jedis jedis = pool.getResource();
		Transaction tx = jedis.multi();
		try {
			Response<Map<String, String>> hgetAllResponse = tx.hgetAll( key.toString() );
			tx.exec();
			@SuppressWarnings("rawtypes")
			Map map = hgetAllResponse.get();
			return map;
		} finally {
			pool.returnResource( jedis );
		}
	}

	private static RedisDatastoreProvider getProvider(SessionFactory sessionFactory) {
		DatastoreProvider provider = ( (SessionFactoryImplementor) sessionFactory ).getServiceRegistry().getService( DatastoreProvider.class );
		if ( !( RedisDatastoreProvider.class.isInstance( provider ) ) ) {
			throw new RuntimeException( "Not testing with RedisDatastoreProvider, cannot extract underlying map." );
		}
		return RedisDatastoreProvider.class.cast( provider );
	}

	@Override
	public boolean backendSupportsTransactions() {
		return false;
	}

	@Override
	public void dropSchemaAndDatabase(SessionFactory sessionFactory) {
		RedisDatastoreProvider provider = getProvider( sessionFactory );
		JedisPool pool = provider.getPool();
		Jedis jedis = pool.getResource();
		Transaction tx = jedis.multi();
		try {
			tx.flushAll();
			tx.exec();
		} finally {
			pool.returnResource( jedis );
		}
	}

	@Override
	public Map<String, String> getEnvironmentProperties() {
		return null;
	}

	@Override
	public long getNumberOfEntities(SessionFactory sessionFactory) {
		RedisDatastoreProvider provider = getProvider( sessionFactory );
		JedisPool pool = provider.getPool();
		Jedis jedis = pool.getResource();
		Transaction tx = jedis.multi();
		try {
			Response<Set<String>> keys = tx.keys( "EntityKey*" );
			tx.exec();
			return keys.get().size();
		} finally {
			pool.returnResource( jedis );
		}
	}

	@Override
	public long getNumberOfAssociations(SessionFactory sessionFactory) {
		RedisDatastoreProvider provider = getProvider( sessionFactory );
		JedisPool pool = provider.getPool();
		Jedis jedis = pool.getResource();
		Transaction tx = jedis.multi();
		try {
			Response<Set<String>> keys = tx.keys( "AssociationKey*" );
			tx.exec();
			return keys.get().size();
		} finally {
			pool.returnResource( jedis );
		}
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
