/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.dialect.eventstate.impl;

import org.hibernate.engine.spi.SessionImplementor;

/**
 * Callback for event cycle scoped state objects.
 * <p>
 * State is solely to be kept in the produced state objects, lifecycle implementations themselves should preferably be
 * stateless and need to be thread-safe.
 *
 * @author Gunnar Morling
 */
public interface EventStateLifecycle<T> {

	/**
	 * Creates a new instance of the represented event state type. Invoked by {@link EventContextManager} in case a
	 * event state type is accessed for the first time during a given event cycle.
	 */
	T create(SessionImplementor session);

	/**
	 * Invoked by {@link EventContextManager} if an event cycle is finished.
	 */
	void onFinish(T state, SessionImplementor session);
}
