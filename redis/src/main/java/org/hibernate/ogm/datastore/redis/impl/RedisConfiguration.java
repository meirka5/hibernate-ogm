/**
 * 
 */
package org.hibernate.ogm.datastore.redis.impl;

import java.util.Map;

import org.hibernate.ogm.cfg.OgmProperties;
import org.hibernate.ogm.cfg.impl.CommonStoreConfiguration;

import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

/**
 * @author Davide D'Alto <davide@hibernate.org>
 */
public class RedisConfiguration extends CommonStoreConfiguration {

	public RedisConfiguration(Map<?, ?> configurationValues) {
		super( addDefaults( configurationValues ), Protocol.DEFAULT_PORT );
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static Map addDefaults(Map configurationValues) {
		if ( configurationValues.get( OgmProperties.DATABASE ) == null ) {
			configurationValues.put( OgmProperties.DATABASE, Protocol.DEFAULT_DATABASE );
		}
		return configurationValues;
	}

	public JedisPoolConfig buildJedisPoolConfig() {
		return new JedisPoolConfig();
	}

	public int getDatabaseIndex() {
		return Integer.valueOf( super.getDatabaseName() );
	}

}
