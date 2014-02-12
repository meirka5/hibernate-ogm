package org.hibernate.ogm.dialect.redis.type;

import org.hibernate.MappingException;
import org.hibernate.engine.spi.Mapping;
import org.hibernate.ogm.type.AbstractGenericBasicType;
import org.hibernate.ogm.type.descriptor.StringMappedGridTypeDescriptor;
import org.hibernate.type.descriptor.java.DoubleTypeDescriptor;

/**
 * @author Andrea Boriero <dreborier@gmail.com/>
 */
public class RedisDoubleType extends AbstractGenericBasicType<Double> {

	public static final RedisDoubleType INSTANCE = new RedisDoubleType();

	public RedisDoubleType() {
		super( StringMappedGridTypeDescriptor.INSTANCE, DoubleTypeDescriptor.INSTANCE );
	}

	@Override
	public String getName() {
		return "redis_double";
	}

	@Override
	public int getColumnSpan(Mapping mapping) throws MappingException {
		return 1;
	}
}