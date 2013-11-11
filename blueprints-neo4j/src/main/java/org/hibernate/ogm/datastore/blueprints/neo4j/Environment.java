/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
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
package org.hibernate.ogm.datastore.blueprints.neo4j;

/**
 * Neo4j GridDialect configuration options.
 *
 * @author Davide D'Alto
 * @see org.hibernate.ogm.dialect.blueprints.neo4j.BlueprintsDialect
 */
public interface Environment {

	/**
	 * Location of the Neo4j embedded properties file. It can be an URL or an absolute file path.
	 */
	String BLUEPRINTS_CONFIGURATION_LOCATION = "hibernate.ogm.blueprints.properties.location";

	/**
	 * Name of the neo4j index containing the stored entities. Default to {@link #DEFAULT_BLUEPRINTS_ENTITY_INDEX_NAME}
	 */
	String BLUEPRINTS_ENTITY_INDEX_NAME = "hibernate.ogm.neo4j.index.entity";

	/**
	 * Name of the Neo4j index containing the stored associations. Default to
	 * {@link #DEFAULT_BLUEPRINTS_ASSOCIATION_INDEX_NAME}
	 */
	String BLUEPRINTS_ASSOCIATION_INDEX_NAME = "hibernate.ogm.neo4j.index.association";

	/**
	 * Name of the index that stores the next available value for a sequence. Default to
	 * {@link #DEFAULT_BLUEPRINTS_SEQUENCE_INDEX_NAME}
	 */
	String BLUEPRINTS_SEQUENCE_INDEX_NAME = "hibernate.ogm.neo4j.index.sequence";

	/**
	 * Qualified class name for the creation of a new {@link org.neo4j.graphdb.GraphDatabaseService}.
	 * <p>
	 * The class must implement the interface {@link org.hibernate.ogm.datastore.neo4j.spi.GraphDatabaseServiceFactory}.
	 */
	String NEO4J_GRAPHDB_FACTORYCLASS = "hibernate.ogm.neo4j.graphdb.factoryclass";

	/**
	 * Default name of the index that stores entities.
	 */
	String DEFAULT_BLUEPRINTS_ENTITY_INDEX_NAME = "_nodes_ogm_index";

	/**
	 * Default name of the index that stores associations.
	 */
	String DEFAULT_BLUEPRINTS_ASSOCIATION_INDEX_NAME = "_relationships_ogm_index";

	/**
	 * Default Name of the index that stores the next available value for sequences.
	 */
	String DEFAULT_BLUEPRINTS_SEQUENCE_INDEX_NAME = "_sequences_ogm_index";

	/**
	 * Default Location of the blueprints properties file.
	 */
	String DEFAULT_BLUEPRINTS_CONFIGURATION_LOCATION = "blueprints.properties";


}
