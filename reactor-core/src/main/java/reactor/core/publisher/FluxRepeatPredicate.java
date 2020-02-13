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
import java.util.function.BooleanSupplier;

import reactor.core.CorePublisher;
import reactor.core.CoreSubscriber;

/**
 * Repeatedly subscribes to the source if the predicate returns true after
 * completion of the previous subscription.
 *
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxRepeatPredicate<T> extends InternalFluxOperator<T, T> {

	final BooleanSupplier predicate;

	FluxRepeatPredicate(Flux<? extends T> source, BooleanSupplier predicate) {
		super(source);
		this.predicate = Objects.requireNonNull(predicate, "predicate");
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		RepeatPredicateSubscriber<T> parent = new RepeatPredicateSubscriber<>(source,
				actual, predicate);

		actual.onSubscribe(parent);

		if (!parent.isCancelled()) {
			// 向上游拉取数据
			parent.resubscribe();
		}
		return null;
	}

	/**
	 * 订阅者包装对象
	 * @param <T>
	 */
	static final class RepeatPredicateSubscriber<T>
			extends Operators.MultiSubscriptionSubscriber<T, T> {

		final CorePublisher<? extends T> source;

		final BooleanSupplier predicate;

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<RepeatPredicateSubscriber> WIP =
		  AtomicIntegerFieldUpdater.newUpdater(RepeatPredicateSubscriber.class, "wip");

		long produced;

		RepeatPredicateSubscriber(CorePublisher<? extends T> source,
				CoreSubscriber<? super T> actual, BooleanSupplier predicate) {
			super(actual);
			this.source = source;
			this.predicate = predicate;
		}

		@Override
		public void onNext(T t) {
			produced++;

			actual.onNext(t);
		}

		/**
		 * 当数据发射结束后 如果谓语判断为true  重新订阅一次source
		 */
		@Override
		public void onComplete() {
			boolean b;
			
			try {
				b = predicate.getAsBoolean();
			} catch (Throwable e) {
				actual.onError(Operators.onOperatorError(e, actual.currentContext()));
				return;
			}
			
			if (b) {
				resubscribe();
			} else {
				actual.onComplete();
			}
		}

		/**
		 * 向上游拉取数据
		 */
		void resubscribe() {
			if (WIP.getAndIncrement(this) == 0) {
				do {
					if (isCancelled()) {
						return;
					}

					long c = produced;
					if (c != 0L) {
						produced = 0L;
						produced(c);
					}

					source.subscribe(this);

				} while (WIP.decrementAndGet(this) != 0);
			}
		}
	}
}
