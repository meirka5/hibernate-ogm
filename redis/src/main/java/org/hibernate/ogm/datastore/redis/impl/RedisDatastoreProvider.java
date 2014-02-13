/* 
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.hibernate.ogm.datastore.redis.impl;

import java.util.Map;

import org.hibernate.ogm.datastore.spi.DatastoreProvider;
import org.hibernate.ogm.dialect.GridDialect;
import org.hibernate.ogm.dialect.redis.RedisDialect;
import org.hibernate.ogm.logging.redis.impl.Log;
import org.hibernate.ogm.logging.redis.impl.LoggerFactory;
import org.hibernate.ogm.service.impl.LuceneBasedQueryParserService;
import org.hibernate.ogm.service.impl.QueryParserService;
import org.hibernate.service.spi.Configurable;
import org.hibernate.service.spi.Startable;
import org.hibernate.service.spi.Stoppable;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Protocol;

/**
 * @author Seiya Kawashima <skawashima@uchicago.edu>
 */
public class RedisDatastoreProvider implements DatastoreProvider, Startable, Stoppable, Configurable {

	private static final Log log = LoggerFactory.getLogger();

	private JedisPool pool;

	private RedisConfiguration configuration;

	@Override
	public void configure(Map configurationValues) {
		configuration = new RedisConfiguration( configurationValues );
	}

	@Override
	public void start() {
		try {
			log.redisStarting();
			pool = new JedisPool(
					configuration.buildJedisPoolConfig(),
					configuration.getHost(),
					configuration.getPort(),
					Protocol.DEFAULT_TIMEOUT,
					configuration.getPassword(),
					configuration.getDatabaseIndex()
					);
		}
		catch (Exception ex) {
			throw log.cannotStartRedis( ex );
		}
	}

	@Override
	public void stop() {
		log.redisStopping();
		if ( pool != null ) {
			pool.destroy();
		}
	}

	@Override
	public Class<? extends GridDialect> getDefaultDialect() {
		return RedisDialect.class;
	}

	public JedisPool getPool() {
		return pool;
	}

	@Override
	public Class<? extends QueryParserService> getDefaultQueryParserServiceType() {
		return LuceneBasedQueryParserService.class;
	}

}
