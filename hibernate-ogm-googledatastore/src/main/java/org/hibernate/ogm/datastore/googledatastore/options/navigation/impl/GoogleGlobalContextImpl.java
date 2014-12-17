package org.hibernate.ogm.datastore.googledatastore.options.navigation.impl;

import org.hibernate.ogm.datastore.googledatastore.options.navigation.GoogleEntityContext;
import org.hibernate.ogm.datastore.googledatastore.options.navigation.GoogleGlobalContext;
import org.hibernate.ogm.datastore.keyvalue.options.CacheMappingType;

/**
 * Created by ABhat on 14 Dec 2014.
 */
public class GoogleGlobalContextImpl implements GoogleGlobalContext {
    @Override
    public GoogleGlobalContext cacheMapping(CacheMappingType cacheMapping) {
        return null;
    }

    @Override
    public GoogleEntityContext entity(Class<?> type) {
        return null;
    }
}
