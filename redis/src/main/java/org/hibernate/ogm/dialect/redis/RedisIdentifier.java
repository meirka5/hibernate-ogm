/**
 * 
 */
package org.hibernate.ogm.dialect.redis;

import org.hibernate.ogm.grid.AssociationKey;
import org.hibernate.ogm.grid.EntityKey;
import org.hibernate.ogm.grid.RowKey;
import org.hibernate.ogm.util.impl.Identifier;

/**
 * @author Davide D'Alto <davide@hibernate.org>
 */
public final class RedisIdentifier {

	public static String createId(DomainSpace domain, EntityKey entityKey) {
		return domain.getPrefix() + Identifier.createEntityId( entityKey );
	}

	public static String createId(DomainSpace domain, AssociationKey asssociationKey) {
		return domain.getPrefix() + Identifier.createAssociationId( asssociationKey );
	}

	public static String createId(DomainSpace domain, RowKey rowKey) {
		return domain.getPrefix() + Identifier.createRowId( rowKey );
	}

	public static String createPrefix(DomainSpace domain, String table) {
		return domain.getPrefix() + Identifier.createPrefix( table );
	}

}
