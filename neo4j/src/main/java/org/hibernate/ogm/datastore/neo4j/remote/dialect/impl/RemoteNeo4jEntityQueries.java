/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.neo4j.remote.dialect.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.hibernate.HibernateException;
import org.hibernate.ogm.datastore.neo4j.dialect.impl.EntityQueries;
import org.hibernate.ogm.datastore.neo4j.remote.impl.Neo4jClient;
import org.hibernate.ogm.datastore.neo4j.remote.json.impl.ErrorResponse;
import org.hibernate.ogm.datastore.neo4j.remote.json.impl.Graph.Node;
import org.hibernate.ogm.datastore.neo4j.remote.json.impl.Row;
import org.hibernate.ogm.datastore.neo4j.remote.json.impl.Statement;
import org.hibernate.ogm.datastore.neo4j.remote.json.impl.StatementResult;
import org.hibernate.ogm.datastore.neo4j.remote.json.impl.Statements;
import org.hibernate.ogm.datastore.neo4j.remote.json.impl.StatementsResponse;
import org.hibernate.ogm.dialect.query.spi.ClosableIterator;
import org.hibernate.ogm.dialect.spi.TupleContext;
import org.hibernate.ogm.model.key.spi.EntityKeyMetadata;
import org.hibernate.ogm.util.impl.ArrayHelper;

/**
 * @author Davide D'Alto
 */
public class RemoteNeo4jEntityQueries extends EntityQueries {

	private static final ClosableIteratorAdapter<AssociationPropertiesRow> EMPTY_RELATIONSHIPS = new ClosableIteratorAdapter<>(Collections.<AssociationPropertiesRow>emptyList().iterator() );

	public RemoteNeo4jEntityQueries(EntityKeyMetadata entityKeyMetadata) {
		this( entityKeyMetadata, null );
	}

	public RemoteNeo4jEntityQueries(EntityKeyMetadata entityKeyMetadata, TupleContext tupleContext) {
		super( entityKeyMetadata, tupleContext );
	}

	public Node findEntity(Neo4jClient executionEngine, Object[] columnValues) {
		Map<String, Object> params = params( columnValues );
		String query = getFindEntityQuery();
		List<StatementResult> results = executeQuery( executionEngine, query, params );
		if ( results != null ) {
			Row row = results.get( 0 ).getData().get( 0 );
			if ( row.getGraph().getNodes().size() > 0 ) {
				return row.getGraph().getNodes().get( 0 );
			}
		}
		return null;
	}

	public Node findAssociatedEntity(Neo4jClient neo4jClient, Object[] keyValues, String associationrole) {
		Map<String, Object> params = params( keyValues );
		String query = getFindAssociatedEntityQuery( associationrole );
		if ( query != null ) {
			List<StatementResult> results = executeQuery( neo4jClient, query, params );
			if ( results != null ) {
				Row row = results.get( 0 ).getData().get( 0 );
				if ( row.getGraph().getNodes().size() > 0 ) {
					return row.getGraph().getNodes().get( 0 );
				}
			}
		}
		return null;
	}

	public Statement getCreateEntityWithPropertiesQueryStatement(Object[] columnValues, Map<String, Object> properties) {
		String query = getCreateEntityWithPropertiesQuery();
		Map<String, Object> params = Collections.singletonMap( "props", (Object) properties );
		return new Statement( query, params );
	}

	public Statement removeColumnStatement(Object[] columnValues, String column) {
		String query = getRemoveColumnQuery( column );
		Map<String, Object> params = params( columnValues );
		return new Statement( query, params );
	}

	public Statement getUpdateEntityPropertiesStatement(Object[] columnvalues, Map<String, Object> properties) {
		String query = getUpdateEntityPropertiesQuery( properties );

		Object[] paramsValues = ArrayHelper.concat( Arrays.asList( columnvalues, new Object[properties.size()] ) );
		int index = columnvalues.length;
		for ( Map.Entry<String, Object> entry : properties.entrySet() ) {
			paramsValues[index++] = entry.getValue();
		}
		return new Statement( query, params( paramsValues ) );
	}

	public void removeEntity(Neo4jClient executionEngine, Object[] columnValues) {
		executeQuery( executionEngine, getRemoveEntityQuery(), params( columnValues ) );
	}

	public ClosableIterator<Node> findEntities(Neo4jClient executionEngine) {
		String query = getFindEntitiesQuery();
		List<StatementResult> results = executeQuery( executionEngine, query, null );
		if ( results != null ) {
			Row row = results.get( 0 ).getData().get( 0 );
			if ( row.getGraph().getNodes().size() > 0 ) {
				List<Node> nodes = row.getGraph().getNodes();
				return new ClosableIteratorAdapter<>( nodes.iterator() );
			}
		}
		return null;
	}

	public Statement getUpdateOneToOneAssociationStatement(String associationRole, Object[] ownerKeyValues, Object[] targetKeyValues) {
		String query = getUpdateToOneQuery( associationRole );
		Map<String, Object> params = params( ownerKeyValues );
		params.putAll( params( targetKeyValues, ownerKeyValues.length ) );
		return new Statement( query, params );
	}

	private List<StatementResult> executeQuery(Neo4jClient executionEngine, String query, Map<String, Object> properties, String... dataContents) {
		Statements statements = new Statements();
		statements.addStatement( query, properties, dataContents );
		return executeQuery( executionEngine, statements );
	}

	private List<StatementResult> executeQuery(Neo4jClient executionEngine, Statements statements) {
		StatementsResponse statementsResponse = executionEngine.executeQueriesInOpenTransaction( statements );
		validate( statementsResponse );
		List<StatementResult> results = statementsResponse.getResults();
		if ( results == null || results.isEmpty() ) {
			return null;
		}
		if ( results.get( 0 ).getData().isEmpty() ) {
			return null;
		}
		return results;
	}

	private void validate(StatementsResponse statementsResponse) {
		if ( !statementsResponse.getErrors().isEmpty() ) {
			ErrorResponse errorResponse = statementsResponse.getErrors().get( 0 );
			throw new HibernateException( String.valueOf( errorResponse ) );
		}
	}

	public Statement updateEmbeddedColumnStatement(Object[] keyValues, String column, Object value) {
		String query = getUpdateEmbeddedColumnQuery( keyValues, column );
		Map<String, Object> params = params( ArrayHelper.concat( keyValues, value, value ) );
		return new Statement( query, params );
	}

	public Statement removeEmbeddedColumnStatement(Object[] keyValues, String embeddedColumn) {
		String query = getRemoveEmbeddedPropertyQuery().get( embeddedColumn );
		Map<String, Object> params = params( keyValues );
		return new Statement( query, params );
	}

	public Statement removeEmptyEmbeddedNodesStatement(Object[] keyValues, String embeddedColumn) {
		String query = getRemoveEmbeddedPropertyQuery().get( embeddedColumn );
		Map<String, Object> params = params( keyValues );
		return new Statement( query, params );
	}

	@SuppressWarnings("unchecked")
	public ClosableIterator<AssociationPropertiesRow> findAssociation(Neo4jClient executionEngine, Object[] columnValues, String role) {
		// Find the target node
		String queryForAssociation = getFindAssociationQuery( role );

		// Find the embedded properties of the target node
		String queryForEmbedded = getFindAssociationTargetEmbeddedValues( role );

		// Execute the queries
		Map<String, Object> params = params( columnValues );
		Statements statements = new Statements();
		statements.addStatement( queryForAssociation, params, Statement.AS_ROW );
		statements.addStatement( queryForEmbedded, params, Statement.AS_ROW );
		List<StatementResult> response = executeQuery( executionEngine, statements );

		if ( response != null ) {
			List<Row> data = response.get( 0 ).getData();
			List<Row> embeddedNodes = response.get( 1 ).getData();
			int embeddedNodesIndex = 0;
			List<AssociationPropertiesRow> responseRows = new ArrayList<>( data.size() );
			for ( int i = 0; i < data.size(); i++ ) {
				String idTarget = String.valueOf( data.get( i ).getRow().get( 0 ) );

				// Read the properties of the owner, the target and the relationship that joins them
				Map<String, Object> rel = (Map<String, Object>) data.get( i ).getRow().get( 1 );
				Map<String, Object> ownerNode = (Map<String, Object>) data.get( i ).getRow().get( 2 );
				Map<String, Object> targetNode = (Map<String, Object>) data.get( i ).getRow().get( 3 );

				// Read the embedded column and add them to the target node
				while ( embeddedNodesIndex < embeddedNodes.size() ) {
					Row row = embeddedNodes.get( embeddedNodesIndex );
					String embeddedOwnerId = row.getRow().get( 0 ).toString();
					if ( embeddedOwnerId.equals( idTarget ) ) {
						addTargetEmbeddedProperties( targetNode, row );
						embeddedNodesIndex++;
					}
					else {
						break;
					}
				}
				AssociationPropertiesRow associationPropertiesRow = new AssociationPropertiesRow( rel, ownerNode, targetNode );
				responseRows.add( associationPropertiesRow );
			}
			if ( responseRows.isEmpty() ) {
				return EMPTY_RELATIONSHIPS;
			}
			return new ClosableIteratorAdapter<>( responseRows.iterator() );
		}
		return EMPTY_RELATIONSHIPS;
	}

	@SuppressWarnings("unchecked")
	private void addTargetEmbeddedProperties(Map<String, Object> targetNode, Row row) {
		List<String> pathToNode = (List<String>) row.getRow().get( 1 );
		if ( pathToNode != null ) {
			Map<String, Object> embeddedNodeProperties = (Map<String, Object>) row.getRow().get( 3 );
			String path = concat( pathToNode );
			for ( Map.Entry<String, Object> entry : embeddedNodeProperties.entrySet() ) {
				targetNode.put( path + "." + entry.getKey(), entry.getValue() );
			}
		}
	}

	private String concat(List<String> pathToNode) {
		StringBuilder path = new StringBuilder();
		for ( String entry : pathToNode ) {
			path.append( "." );
			path.append( entry );
		}
		return path.substring( 1 );
	}

	public Node findEmbeddedNode(Neo4jClient neo4jClient, Object[] keyValues, String embeddedPath) {
		List<StatementResult> results = executeQuery( neo4jClient, getFindEmbeddedNodeQueries().get( embeddedPath ), params( keyValues ) );
		if ( results == null ) {
			return null;
		}
		return results.get( 0 ).getData().get( 0 ).getGraph().getNodes().get( 0 );
	}

	public void removeToOneAssociation(Neo4jClient executionEngine, Object[] columnValues, String associationRole) {
		Map<String, Object> params = params( ArrayHelper.concat( columnValues, associationRole ) );
		executeQuery( executionEngine, getRemoveToOneAssociation(), params );
	}

	private static class ClosableIteratorAdapter<T> implements ClosableIterator<T> {

		private final Iterator<T> iterator;

		public ClosableIteratorAdapter(Iterator<T> iterator) {
			this.iterator = iterator;
		}

		@Override
		public boolean hasNext() {
			return iterator.hasNext();
		}

		@Override
		public T next() {
			return iterator.next();
		}

		@Override
		public void close() {
		}

		@Override
		public void remove() {
		}
	}
}
