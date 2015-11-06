/*
 Licensed to Diennea S.r.l. under one
 or more contributor license agreements. See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership. Diennea S.r.l. licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.

 */
package majordodo.utils;

import java.util.PriorityQueue;

/**
 * Priority queue <b>bonded</b>.
 * 
 * This queue will not grow more than configured max size: if added more than
 * max size elements only <i>bigger</i> ones will be retained, other data will
 * be discarded silently.
 * 
 * @author diego.salvi
 */
public final class DiscardingBoundedPriorityQueue<E extends Comparable<E>> extends PriorityQueue<E>
{

	/** Default Serial Version UID */
	private static final long serialVersionUID = 1L;

	/** Bounding size */
	private final int maxSize;

	/**
	 * Create a new {@link DiscardingBoundedPriorityQueue}.
	 * 
	 * @param size maximum queue size
	 */
	public DiscardingBoundedPriorityQueue(int size)
	{
		super(size);

		this.maxSize = size;
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * Will discard smalled elements if needed (invoked from
	 * {@link #add(Object)} too).
	 * </p>
	 */
	@Override
	public boolean offer(E e)
	{

		if ( this.size() < maxSize )
			return super.offer(e);
		
		if ( e.compareTo(this.peek()) > 0 )
		{
			this.remove();

			return super.offer(e);
		}

		return false;

	}

}