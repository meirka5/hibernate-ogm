/**
 * 
 */
package org.hibernate.ogm.dialect.redis;

import org.hibernate.ogm.grid.AssociationKey;
import org.hibernate.ogm.grid.EntityKey;
import org.hibernate.ogm.grid.RowKey;



/**
 * @author Davide D'Alto <davide@hibernate.org>
 */
public final class IdGenerator {

	public static String generateId(DomainSpace domain, EntityKey entityKey) {
		return generateId( domain, entityKey.getTable(), entityKey.getColumnNames(), entityKey.getColumnValues() );
	}

	public static String generateId(DomainSpace domain, AssociationKey asssociationKey) {
		return generateId( domain, asssociationKey.getTable(), asssociationKey.getColumnNames(), asssociationKey.getColumnValues() );
	}

	public static String generateId(DomainSpace domain, RowKey rowKey) {
		return generateId( domain, rowKey.getTable(), rowKey.getColumnNames(), rowKey.getColumnValues() );
	}

	public static String generatePrefix(DomainSpace domain, String table) {
		StringBuilder builder = new StringBuilder();
		appendPrefix( domain, table, builder );
		return builder.toString();
	}

	private static String generateId(DomainSpace domain, String table, String[] columnNames, Object[] columnValues) {
		StringBuilder builder = new StringBuilder();
		appendPrefix( domain, table, builder );
		for ( int i = 0; i < columnNames.length; i++ ) {
			builder.append( columnNames[i] );
			builder.append( "='" );
			builder.append( String.valueOf( columnValues[i] ) );
			builder.append( "', " );
		}
		String id = builder.substring( 0, builder.length() - 2 );
		return id;
	}

	private static void appendPrefix(DomainSpace domain, String table, StringBuilder builder) {
		builder.append( domain.getPrefix() );
		builder.append( table );
		builder.append( "::" );
	}
}
