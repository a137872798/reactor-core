/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.core.publisher;

import org.reactivestreams.Publisher;
import reactor.core.CoreSubscriber;

/**
 * Wraps multiple Publishers into a ParallelFlux which runs them
 * in parallel.
 * 
 * @param <T> the value type
 */
final class ParallelArraySource<T> extends ParallelFlux<T> implements SourceProducer<T>  {
	final Publisher<T>[] sources;
	
	ParallelArraySource(Publisher<T>[] sources) {
		//noinspection ConstantConditions
		if (sources == null || sources.length == 0) {
			throw new IllegalArgumentException("Zero publishers not supported");
		}
		this.sources = sources;
	}

	@Override
	public int parallelism() {
		return sources.length;
	}

	/**
	 * 因为 source 和 subscribe 的数量是一样的 这里一一对应 直接进行订阅就好
	 * @param subscribers the subscribers array to run in parallel, the number of items
	 * must be equal to the parallelism level of this ParallelFlux
	 *                    parallelFlux 本身是一个并行对象 当被订阅时可以设置一组订阅者 数量与并行度一致
	 */
	@Override
	public void subscribe(CoreSubscriber<? super T>[] subscribers) {
		if (!validate(subscribers)) {
			return;
		}
		
		int n = subscribers.length;
		
		for (int i = 0; i < n; i++) {
			Flux.from(sources[i]).subscribe(subscribers[i]);
		}
	}
}
