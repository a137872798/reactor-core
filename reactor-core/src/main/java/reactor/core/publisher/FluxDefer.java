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

import java.util.Objects;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import reactor.core.CoreSubscriber;

/**
 * Defers the creation of the actual Publisher the Subscriber will be subscribed to.
 *
 * @param <T> the value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 * 该对象内部没有直接存放数据流 而是通过一个可以创建数据流的 supplier 来初始化
 */
final class FluxDefer<T> extends Flux<T> implements SourceProducer<T> {

	/**
	 * 用于创建数据流的 函数
	 */
	final Supplier<? extends Publisher<? extends T>> supplier;

	FluxDefer(Supplier<? extends Publisher<? extends T>> supplier) {
		this.supplier = Objects.requireNonNull(supplier, "supplier");
	}

	/**
	 * 当为该对象设置订阅者时
	 * @param actual the {@link Subscriber} interested into the published sequence
	 */
	@Override
	@SuppressWarnings("unchecked")
	public void subscribe(CoreSubscriber<? super T> actual) {
		Publisher<? extends T> p;

		try {
			p = Objects.requireNonNull(supplier.get(),
					"The Publisher returned by the supplier is null");
		}
		catch (Throwable e) {
			Operators.error(actual, Operators.onOperatorError(e, actual.currentContext()));
			return;
		}

		// 将 pub 包装成 flux 对象
		from(p).subscribe(actual);
	}


	@Override
	public Object scanUnsafe(Attr key) {
		return null; //no particular key to be represented, still useful in hooks
	}
}
