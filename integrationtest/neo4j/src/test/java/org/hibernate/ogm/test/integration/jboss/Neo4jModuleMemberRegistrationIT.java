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
package org.hibernate.ogm.test.integration.jboss;

import org.hibernate.ogm.test.integration.jboss.model.Member;
import org.hibernate.ogm.test.integration.jboss.util.ModuleMemberRegistrationDeployment;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.descriptor.api.Descriptors;
import org.jboss.shrinkwrap.descriptor.api.persistence20.PersistenceDescriptor;
import org.jboss.shrinkwrap.descriptor.api.persistence20.PersistenceUnit;
import org.jboss.shrinkwrap.descriptor.api.persistence20.Properties;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Test for the Hibernate OGM module in WildFly using Neo4j
 *
 * @author Guillaume Scheibel <guillaume.scheibel@gmail.com>
 */
@RunWith(Arquillian.class)
public class Neo4jModuleMemberRegistrationIT extends ModuleMemberRegistrationScenario {

	@Deployment
	public static Archive<?> createTestArchive() {
		return new ModuleMemberRegistrationDeployment
				.Builder( Neo4jModuleMemberRegistrationIT.class )
				.persistenceXml( persistenceXml() )
				.manifestDependencies( "org.hibernate:ogm services, org.hibernate.ogm.neo4j services" )
				.createDeployment();
	}

	private static PersistenceDescriptor persistenceXml() {
		Properties<PersistenceUnit<PersistenceDescriptor>> propertiesContext = Descriptors.create( PersistenceDescriptor.class )
				.version( "2.0" )
				.createPersistenceUnit()
				.name( "primary" )
				.provider( "org.hibernate.ogm.jpa.HibernateOgmPersistence" )
				.clazz( Member.class.getName() )
				.getOrCreateProperties();
		return propertiesContext
					.createProperty().name( "hibernate.ogm.datastore.provider" ).value( "neo4j" ).up()
					.createProperty().name( "hibernate.ogm.datastore.database" ).value( "ogm_test_database" ).up()
					.createProperty().name( "hibernate.search.default.directory_provider" ).value( "ram" ).up()
				.up().up();
	}

	@Test
	public void shouldFindPersistedMemberByIdWithNativeQuery() throws Exception {
//		Member newMember = memberRegistration.getNewMember();
//		newMember.setName( "Peter O'Tall" );
//		memberRegistration.register();
//
//		String nativeQuery = "{ _id: " + newMember.getId() + " }";
//		Member found = memberRegistration.findWithNativeQuery( nativeQuery );
//
//		assertNotNull( "Expected at least one result using a native query", found );
//		assertEquals( "Native query hasn't found a new member", newMember.getName(), found.getName() );
	}

}
