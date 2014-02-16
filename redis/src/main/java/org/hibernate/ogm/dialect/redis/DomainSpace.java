/**
 * 
 */
package org.hibernate.ogm.dialect.redis;


/**
 * @author Davide D'Alto <davide@hibernate.org>
 */
public enum DomainSpace {
	ENTITY("E_"),
	ASSOCIATION("A_"),
	ASSOCIATION_ROW("R_"),
	SEQUENCE("S_");

	private final String prefix;

	private DomainSpace(String prefix) {
		this.prefix = prefix;
	}

	public String getPrefix() {
		return prefix;
	}

}
