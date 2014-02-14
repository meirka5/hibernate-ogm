/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * JBoss, Home of Professional Open Source
 * Copyright 2013-2014 Red Hat Inc. and/or its affiliates and other contributors
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
package org.hibernate.ogm.test.integration.wildfly;

import org.hibernate.ogm.cfg.OgmProperties;
import org.hibernate.ogm.jpa.HibernateOgmPersistence;
import org.hibernate.ogm.test.integration.wildfly.model.Member;
import org.hibernate.ogm.test.integration.wildfly.util.ModuleMemberRegistrationDeployment;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.descriptor.api.Descriptors;
import org.jboss.shrinkwrap.descriptor.api.persistence20.PersistenceDescriptor;
import org.junit.runner.RunWith;

/**
 * Test the hibernate OGM module in JBoss AS using CouchDB
 *
 * @author Andrea Boriero <dreborier@gmail.com>
 */
@RunWith(Arquillian.class)
public class RedisModuleMemberRegistrationIT extends ModuleMemberRegistrationScenario {

	public static final String ENVIRONMENT_REDIS_HOSTNAME = "REDIS_HOSTNAME";
	public static final String ENVIRONMENT_REDIS_PORT = "REDIS_PORT";

	public static final String DEFAULT_HOSTNAME = "localhost";
	public static final String DEFAULT_PORT = "6379";

	private static String redisHostName;
	private static String redisPortNumber;

	static {
		setHostName();
		setPortNumber();
	}

	@Deployment
	public static Archive<?> createTestArchive() {
		return new ModuleMemberRegistrationDeployment.Builder( RedisModuleMemberRegistrationIT.class )
				.persistenceXml( persistenceXml() )
				.manifestDependencies( "org.hibernate:ogm services, org.hibernate.ogm.couchdb services" )
				.createDeployment();
	}

	private static void setHostName() {
		redisHostName = System.getenv( ENVIRONMENT_REDIS_HOSTNAME );
		if ( isNull( redisHostName ) ) {
			redisHostName = DEFAULT_HOSTNAME;
		}
	}

	private static void setPortNumber() {
		redisPortNumber = System.getenv( ENVIRONMENT_REDIS_PORT );
		if ( isNull( redisPortNumber ) ) {
			redisPortNumber = DEFAULT_PORT;
		}
	}

	private static boolean isNull(String value) {
		return value == null || value.length() == 0 || value.toLowerCase().equals( "null" );
	}

	private static PersistenceDescriptor persistenceXml() {
		return Descriptors.create( PersistenceDescriptor.class )
					.version( "2.0" )
					.createPersistenceUnit()
						.name( "primary" )
						.provider( HibernateOgmPersistence.class.getCanonicalName() )
						.clazz( Member.class.getName() )
						.getOrCreateProperties()
							.createProperty().name( OgmProperties.DATASTORE_PROVIDER ).value( "redis" ).up()
							.createProperty().name( OgmProperties.HOST ).value( redisHostName ).up()
							.createProperty().name( OgmProperties.PORT ).value( redisPortNumber ).up()
					.up().up();
	}

}
