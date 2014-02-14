package org.hibernate.ogm.dialect.redis.type;

import org.hibernate.MappingException;
import org.hibernate.engine.spi.Mapping;
import org.hibernate.ogm.type.AbstractGenericBasicType;
import org.hibernate.ogm.type.descriptor.StringMappedGridTypeDescriptor;
import org.hibernate.type.descriptor.java.BooleanTypeDescriptor;

/**
 * @author Andrea Boriero <dreborier@gmail.com/>
 */
public class BooleanAsStringType extends AbstractGenericBasicType<Boolean> {

	public static final BooleanAsStringType INSTANCE = new BooleanAsStringType();

	public BooleanAsStringType() {
		super( StringMappedGridTypeDescriptor.INSTANCE, BooleanTypeDescriptor.INSTANCE );
	}

	@Override
	public String getName() {
		return "redis_boolean";
	}

	@Override
	public int getColumnSpan(Mapping mapping) throws MappingException {
		return 1;
	}
}