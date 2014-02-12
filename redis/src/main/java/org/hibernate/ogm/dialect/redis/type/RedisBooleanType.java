package org.hibernate.ogm.dialect.redis.type;

import org.hibernate.MappingException;
import org.hibernate.engine.spi.Mapping;
import org.hibernate.ogm.type.AbstractGenericBasicType;
import org.hibernate.ogm.type.descriptor.StringMappedGridTypeDescriptor;
import org.hibernate.type.descriptor.java.BooleanTypeDescriptor;

/**
 * @author Andrea Boriero <dreborier@gmail.com/>
 */
public class RedisBooleanType extends AbstractGenericBasicType<Boolean> {

	public static final RedisBooleanType INSTANCE = new RedisBooleanType();

	public RedisBooleanType() {
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