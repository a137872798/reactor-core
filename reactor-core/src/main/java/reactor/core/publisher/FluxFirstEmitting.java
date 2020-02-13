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

import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.stream.Stream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;

/**
 * Given a set of source Publishers the values of that Publisher is forwarded to the
 * subscriber which responds first with any signal.
 *
 * @param <T> the value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 * 内部包含一组 pub 对象
 */
final class FluxFirstEmitting<T> extends Flux<T> implements SourceProducer<T> {

	final Publisher<? extends T>[] array;

	final Iterable<? extends Publisher<? extends T>> iterable;

	@SafeVarargs
	FluxFirstEmitting(Publisher<? extends T>... array) {
		this.array = Objects.requireNonNull(array, "array");
		this.iterable = null;
	}

	FluxFirstEmitting(Iterable<? extends Publisher<? extends T>> iterable) {
		this.array = null;
		this.iterable = Objects.requireNonNull(iterable);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		Publisher<? extends T>[] a = array;
		int n;
		if (a == null) {
			n = 0;
			a = new Publisher[8];

			Iterator<? extends Publisher<? extends T>> it;

			try {
				it = Objects.requireNonNull(iterable.iterator(),
						"The iterator returned is null");
			}
			catch (Throwable e) {
				Operators.error(actual, Operators.onOperatorError(e,
						actual.currentContext()));
				return;
			}

			for (; ; ) {

				boolean b;

				try {
					b = it.hasNext();
				}
				catch (Throwable e) {
					Operators.error(actual, Operators.onOperatorError(e,
							actual.currentContext()));
					return;
				}

				if (!b) {
					break;
				}

				Publisher<? extends T> p;

				try {
					p = Objects.requireNonNull(it.next(),
							"The Publisher returned by the iterator is null");
				}
				catch (Throwable e) {
					Operators.error(actual, Operators.onOperatorError(e,
							actual.currentContext()));
					return;
				}

				if (n == a.length) {
					Publisher<? extends T>[] c = new Publisher[n + (n >> 2)];
					System.arraycopy(a, 0, c, 0, n);
					a = c;
				}
				a[n++] = p;
			}

		}
		else {
			n = a.length;
		}

		if (n == 0) {
			Operators.complete(actual);
			return;
		}
		if (n == 1) {
			Publisher<? extends T> p = a[0];

			if (p == null) {
				Operators.error(actual,
						new NullPointerException("The single source Publisher is null"));
			}
			else {
				p.subscribe(actual);
			}
			return;
		}

		// 跟 combineLaster 类似的套路 创建一个 协调对象
		RaceCoordinator<T> coordinator = new RaceCoordinator<>(n);

		coordinator.subscribe(a, n, actual);
	}

	/**
	 * Returns a new instance which has the additional source to be amb'd together with
	 * the current array of sources.
	 * <p>
	 * This operation doesn't change the current FluxFirstEmitting instance.
	 *
	 * @param source the new source to merge with the others
	 *
	 * @return the new FluxFirstEmitting instance or null if the Amb runs with an Iterable
	 */
	@Nullable
	FluxFirstEmitting<T> ambAdditionalSource(Publisher<? extends T> source) {
		if (array != null) {
			int n = array.length;
			@SuppressWarnings("unchecked") Publisher<? extends T>[] newArray =
					new Publisher[n + 1];
			System.arraycopy(array, 0, newArray, 0, n);
			newArray[n] = source;

			return new FluxFirstEmitting<>(newArray);
		}
		return null;
	}

	@Override
	public Object scanUnsafe(Attr key) {
		return null; //no particular key to be represented, still useful in hooks
	}

	/**
	 * 竞争协调对象
	 * @param <T>
	 */
	static final class RaceCoordinator<T>
			implements Subscription, Scannable {

		/**
		 * 上游每个 pub 会对应下面一个 sub
		 */
		final FirstEmittingSubscriber<T>[] subscribers;

		/**
		 * 该对象是否已经被关闭
		 */
		volatile boolean cancelled;

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<RaceCoordinator> WIP =
				AtomicIntegerFieldUpdater.newUpdater(RaceCoordinator.class, "wip");

		/**
		 * 根据 pub 数量来创建 subscriber
		 * @param n
		 */
		@SuppressWarnings("unchecked")
		RaceCoordinator(int n) {
			subscribers = new FirstEmittingSubscriber[n];
			wip = Integer.MIN_VALUE;
		}

		@Override
		public Stream<? extends Scannable> inners() {
			return Stream.of(subscribers);
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.CANCELLED) return cancelled;

			return null;
		}

		/**
		 * 将 上游的一组 pub 数据 发往下游
		 * @param sources
		 * @param n
		 * @param actual
		 */
		void subscribe(Publisher<? extends T>[] sources,
				int n,
				CoreSubscriber<? super T> actual) {
			FirstEmittingSubscriber<T>[] a = subscribers;

			// 将订阅者包装  onNext onComplete 等 会转发到 协调对象上
			for (int i = 0; i < n; i++) {
				a[i] = new FirstEmittingSubscriber<>(actual, this, i);
			}

			// 触发本对象的 request
			actual.onSubscribe(this);

			for (int i = 0; i < n; i++) {
				if (cancelled || wip != Integer.MIN_VALUE) {
					return;
				}

				Publisher<? extends T> p = sources[i];

				if (p == null) {
					if (WIP.compareAndSet(this, Integer.MIN_VALUE, -1)) {
						actual.onError(new NullPointerException("The " + i + " th Publisher source is null"));
					}
					return;
				}

				p.subscribe(a[i]);
			}

		}

		/**
		 * 代表 订阅者尝试从该对象中拉取数据
		 * @param n
		 */
		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				int w = wip;
				if (w >= 0) {
					subscribers[w].request(n);
				}
				else {
					// 挨个触发 request 竞争成功的 对象将允许向下游发送数据
					for (FirstEmittingSubscriber<T> s : subscribers) {
						s.request(n);
					}
				}
			}
		}

		@Override
		public void cancel() {
			if (cancelled) {
				return;
			}
			cancelled = true;

			int w = wip;
			if (w >= 0) {
				subscribers[w].cancel();
			}
			else {
				for (FirstEmittingSubscriber<T> s : subscribers) {
					s.cancel();
				}
			}
		}

		/**
		 * 竞争
		 * @param index
		 * @return
		 */
		boolean tryWin(int index) {
			if (wip == Integer.MIN_VALUE) {
				if (WIP.compareAndSet(this, Integer.MIN_VALUE, index)) {

					FirstEmittingSubscriber<T>[] a = subscribers;
					int n = a.length;

					// 竞争成功情况下 关闭其他subscription
					for (int i = 0; i < n; i++) {
						if (i != index) {
							a[i].cancel();
						}
					}

					return true;
				}
			}
			return false;
		}
	}

	/**
	 * 该对象用于将请求转发到协调对象上
	 * @param <T>
	 */
	static final class FirstEmittingSubscriber<T> extends Operators.DeferredSubscription
			implements InnerOperator<T, T> {

		/**
		 * 协调对象
		 */
		final RaceCoordinator<T> parent;

		final CoreSubscriber<? super T> actual;

		final int index;

		boolean won;

		FirstEmittingSubscriber(CoreSubscriber<? super T> actual,
				RaceCoordinator<T> parent,
				int index) {
			this.actual = actual;
			this.parent = parent;
			this.index = index;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.CANCELLED) return parent.cancelled;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public void onSubscribe(Subscription s) {
			set(s);
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		/**
		 * 竞争成功的 情况下 允许触发真正的 sub(actual)
		 * @param t
		 */
		@Override
		public void onNext(T t) {
			if (won) {
				actual.onNext(t);
			}
			else if (parent.tryWin(index)) {
				won = true;
				actual.onNext(t);
			}
		}

		@Override
		public void onError(Throwable t) {
			if (won) {
				actual.onError(t);
			}
			else if (parent.tryWin(index)) {
				won = true;
				actual.onError(t);
			}
		}

		@Override
		public void onComplete() {
			if (won) {
				actual.onComplete();
			}
			else if (parent.tryWin(index)) {
				won = true;
				actual.onComplete();
			}
		}
	}
}