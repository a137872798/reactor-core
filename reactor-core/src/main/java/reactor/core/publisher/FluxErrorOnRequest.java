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
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.reactivestreams.Subscriber;
import reactor.core.CoreSubscriber;
import reactor.util.annotation.Nullable;

/**
 * Emits a constant or generated Throwable instance to Subscribers.
 *
 * @param <T> the value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 * 将异常包装成 Flux 对象  调用 request() 时 抛出异常   (相当于异常是延迟触发的)
 */
final class FluxErrorOnRequest<T> extends Flux<T> implements SourceProducer<T> {

	final Throwable error;

	FluxErrorOnRequest(Throwable error) {
		this.error = Objects.requireNonNull(error);
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		actual.onSubscribe(new ErrorSubscription(actual, error));
	}

	@Override
	public Object scanUnsafe(Attr key) {
		return null; //no particular key to be represented, still useful in hooks
	}

	static final class ErrorSubscription implements InnerProducer {

		final CoreSubscriber<?> actual;

		final Throwable error;

		// 该变量使用 volatile 修饰
		volatile int once;
		static final AtomicIntegerFieldUpdater<ErrorSubscription> ONCE =
				AtomicIntegerFieldUpdater.newUpdater(ErrorSubscription.class, "once");

		ErrorSubscription(CoreSubscriber<?> actual, Throwable error) {
			this.actual = actual;
			this.error = error;
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				if (ONCE.compareAndSet(this, 0, 1)) {
					actual.onError(error);
				}
			}
		}

		@Override
		public void cancel() {
			once = 1;
		}

		@Override
		public CoreSubscriber actual() {
			return actual;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.ERROR) return error;
			if (key == Attr.CANCELLED || key == Attr.TERMINATED)
				return once == 1;

			return InnerProducer.super.scanUnsafe(key);
		}
	}

}
