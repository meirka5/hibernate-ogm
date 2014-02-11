package org.hibernate.ogm.util.redis.annotation;

import java.lang.annotation.Annotation;

public interface Finder {

	String findAnnotation(Annotation[] annotations, Object obj);

	String findAnnotationBy(Annotation[] annotations, String ann, Object obj);
}
