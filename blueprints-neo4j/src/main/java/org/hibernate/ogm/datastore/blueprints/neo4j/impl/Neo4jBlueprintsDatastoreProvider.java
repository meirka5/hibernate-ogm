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
package org.hibernate.ogm.datastore.blueprints.neo4j.impl;

import static org.hibernate.ogm.datastore.blueprints.neo4j.Environment.BLUEPRINTS_ASSOCIATION_INDEX_NAME;
import static org.hibernate.ogm.datastore.blueprints.neo4j.Environment.BLUEPRINTS_CONFIGURATION_LOCATION;
import static org.hibernate.ogm.datastore.blueprints.neo4j.Environment.BLUEPRINTS_ENTITY_INDEX_NAME;
import static org.hibernate.ogm.datastore.blueprints.neo4j.Environment.BLUEPRINTS_SEQUENCE_INDEX_NAME;
import static org.hibernate.ogm.datastore.blueprints.neo4j.Environment.DEFAULT_BLUEPRINTS_ASSOCIATION_INDEX_NAME;
import static org.hibernate.ogm.datastore.blueprints.neo4j.Environment.DEFAULT_BLUEPRINTS_ENTITY_INDEX_NAME;
import static org.hibernate.ogm.datastore.blueprints.neo4j.Environment.DEFAULT_BLUEPRINTS_SEQUENCE_INDEX_NAME;

import java.net.URL;
import java.util.Map;

import org.hibernate.ogm.datastore.spi.DatastoreProvider;
import org.hibernate.ogm.dialect.GridDialect;
import org.hibernate.ogm.dialect.blueprints.neo4j.BlueprintsDialect;
import org.hibernate.ogm.grid.RowKey;
import org.hibernate.ogm.options.navigation.context.GlobalContext;
import org.hibernate.ogm.options.navigation.impl.ConfigurationContext;
import org.hibernate.ogm.options.navigation.impl.GenericOptionModel;
import org.hibernate.ogm.service.impl.LuceneBasedQueryParserService;
import org.hibernate.ogm.service.impl.QueryParserService;
import org.hibernate.service.spi.Configurable;
import org.hibernate.service.spi.Startable;
import org.hibernate.service.spi.Stoppable;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.GraphFactory;
import com.tinkerpop.blueprints.Index;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.neo4j.Neo4jGraph;

/**
 * Provides access to the Neo4j system.
 *
 * @author Davide D'Alto
 */
public class Neo4jBlueprintsDatastoreProvider implements DatastoreProvider, Startable, Stoppable, Configurable {

	private String sequenceIndexName = DEFAULT_BLUEPRINTS_SEQUENCE_INDEX_NAME;

	private String vertexIndexName = DEFAULT_BLUEPRINTS_ENTITY_INDEX_NAME;

	private String edgeIndexName = DEFAULT_BLUEPRINTS_ASSOCIATION_INDEX_NAME;

	private Neo4jSequenceGenerator neo4jSequenceGenerator;

	private Neo4jGraph graph;

	private String blueprintsConfiguration;

	@Override
	public Class<? extends QueryParserService> getDefaultQueryParserServiceType() {
		return LuceneBasedQueryParserService.class;
	}

	@Override
	public void configure(Map cfg) {
		blueprintsConfiguration = configurationLocation( cfg );
		sequenceIndexName = defaultIfNull( cfg, BLUEPRINTS_SEQUENCE_INDEX_NAME, DEFAULT_BLUEPRINTS_SEQUENCE_INDEX_NAME );
		vertexIndexName = defaultIfNull( cfg, BLUEPRINTS_ENTITY_INDEX_NAME, DEFAULT_BLUEPRINTS_ENTITY_INDEX_NAME );
		edgeIndexName = defaultIfNull( cfg, BLUEPRINTS_ASSOCIATION_INDEX_NAME, DEFAULT_BLUEPRINTS_ASSOCIATION_INDEX_NAME );
	}

	private String configurationLocation(Map cfg) {
		String blueprintsConfiguration = (String) cfg.get( BLUEPRINTS_CONFIGURATION_LOCATION );
		if ( blueprintsConfiguration == null ) {
			URL resource = this.getClass().getClassLoader().getResource( "blueprints.properties" );
			blueprintsConfiguration = resource.getFile();
		}
		return blueprintsConfiguration;
	}

	private String defaultIfNull(Map<?, ?> cfg, String key, String defaultValue) {
		String indexName = (String) cfg.get( key );
		return indexName == null ? defaultValue : indexName;
	}

	@Override
	public void stop() {
		graph.shutdown();
	}

	@Override
	public void start() {
		this.graph = (Neo4jGraph) GraphFactory.open( blueprintsConfiguration );
		this.neo4jSequenceGenerator = new Neo4jSequenceGenerator( graph, sequenceIndexName );
	}

	public void commit() {
		this.graph.commit();
	}

	public void rollback() {
		this.graph.rollback();
	}

	@Override
	public GlobalContext<?, ?> getConfigurationBuilder(ConfigurationContext context) {
		return GenericOptionModel.createGlobalContext( context );
	}

	@Override
	public Class<? extends GridDialect> getDefaultDialect() {
		return BlueprintsDialect.class;
	}

	public Vertex createVertex() {
		return graph.addVertex( null );
	}

	public int nextValue(RowKey key, int increment, int initialValue) {
		return neo4jSequenceGenerator.nextValue( key, increment, initialValue );
	}

	public Index<Vertex> getVertexesIndex() {
		Index<Vertex> index = graph.getIndex( vertexIndexName, Vertex.class );
		if ( index == null ) {
			return graph.createIndex( vertexIndexName, Vertex.class );
		}
		return index;
	}

	public Index<Edge> getEdgesIndex() {
		Index<Edge> index = graph.getIndex( edgeIndexName, Edge.class );
		if ( index == null ) {
			return graph.createIndex( edgeIndexName, Edge.class );
		}
		return index;
	}

}
