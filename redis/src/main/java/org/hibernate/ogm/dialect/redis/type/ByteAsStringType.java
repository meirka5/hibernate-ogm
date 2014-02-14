/**
 * 
 */
package org.hibernate.ogm.dialect.redis.type;

import org.hibernate.MappingException;
import org.hibernate.engine.spi.Mapping;
import org.hibernate.ogm.type.AbstractGenericBasicType;
import org.hibernate.ogm.type.descriptor.StringMappedGridTypeDescriptor;
import org.hibernate.type.descriptor.java.ByteTypeDescriptor;


/**
 * @author Davide D'Alto <davide@hibernate.org>
 *
 */
public class ByteAsStringType extends AbstractGenericBasicType<Byte> {

	public static final ByteAsStringType INSTANCE = new ByteAsStringType();

	public ByteAsStringType() {
		super( StringMappedGridTypeDescriptor.INSTANCE, ByteTypeDescriptor.INSTANCE );
	}

	@Override
	public String getName() {
		return "redis_Byte";
	}

	@Override
	public int getColumnSpan(Mapping mapping) throws MappingException {
		return 1;
	}

}