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
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Signals a timeout (or switches to another sequence) in case a per-item
 * generated Publisher source fires an item or completes before the next item
 * arrives from the main source.
 *
 * @param <T> the main source type
 * @param <U> the value type for the timeout for the very first item
 * @param <V> the value type for the timeout for the subsequent items
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxTimeout<T, U, V> extends InternalFluxOperator<T, T> {

	final Publisher<U> firstTimeout;

	final Function<? super T, ? extends Publisher<V>> itemTimeout;

	final Publisher<? extends T> other;

	final String timeoutDescription; //only useful when no `other`

	FluxTimeout(Flux<? extends T> source,
			Publisher<U> firstTimeout,
			Function<? super T, ? extends Publisher<V>> itemTimeout,
			String timeoutDescription) {
		super(source);
		this.firstTimeout = Objects.requireNonNull(firstTimeout, "firstTimeout");
		this.itemTimeout = Objects.requireNonNull(itemTimeout, "itemTimeout");
		this.other = null;

		/**
		 * 生成描述信息
		 */
		this.timeoutDescription = addNameToTimeoutDescription(source,
				Objects.requireNonNull(timeoutDescription, "timeoutDescription is needed when no fallback"));
	}

	FluxTimeout(Flux<? extends T> source,
			Publisher<U> firstTimeout,
			Function<? super T, ? extends Publisher<V>> itemTimeout,
			Publisher<? extends T> other) {
		super(source);
		this.firstTimeout = Objects.requireNonNull(firstTimeout, "firstTimeout");
		this.itemTimeout = Objects.requireNonNull(itemTimeout, "itemTimeout");
		this.other = Objects.requireNonNull(other, "other");
		this.timeoutDescription = null;
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		// 确保订阅者 以串行形式触发 onNext()
		CoreSubscriber<T> serial = Operators.serialize(actual);

		TimeoutMainSubscriber<T, V> main =
				new TimeoutMainSubscriber<>(serial, itemTimeout, other, timeoutDescription);

		// 触发main 对象的 request
		serial.onSubscribe(main);

		// 包装一个超时对象
		TimeoutTimeoutSubscriber ts = new TimeoutTimeoutSubscriber(main, 0L);

		// 设置超时对象
		main.setTimeout(ts);

		// 向第一个超时数据源拉取数据
		firstTimeout.subscribe(ts);

		// 数据源之后会向main 下发数据
		return main;
	}

	@Nullable
	static String addNameToTimeoutDescription(Publisher<?> source,
			@Nullable  String timeoutDescription) {
		if (timeoutDescription == null) {
			return null;
		}

		Scannable s = Scannable.from(source);
		if (s.isScanAvailable()) {
			return timeoutDescription + " in '" + s.name() + "'";
		}
		else {
			return timeoutDescription;
		}
	}

	/**
	 * 被包装的订阅者对象
	 * @param <T>
	 * @param <V>
	 */
	static final class TimeoutMainSubscriber<T, V>
			extends Operators.MultiSubscriptionSubscriber<T, T> {

		final Function<? super T, ? extends Publisher<V>> itemTimeout;

		final Publisher<? extends T> other;
		final String timeoutDescription; //only useful/non-null when no `other`

		Subscription s;

		volatile IndexedCancellable timeout;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<TimeoutMainSubscriber, IndexedCancellable>
				TIMEOUT =
				AtomicReferenceFieldUpdater.newUpdater(TimeoutMainSubscriber.class,
						IndexedCancellable.class,
						"timeout");

		volatile long index;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<TimeoutMainSubscriber> INDEX =
				AtomicLongFieldUpdater.newUpdater(TimeoutMainSubscriber.class, "index");

		TimeoutMainSubscriber(CoreSubscriber<? super T> actual,
				Function<? super T, ? extends Publisher<V>> itemTimeout,
				@Nullable Publisher<? extends T> other,
				@Nullable String timeoutDescription) {
			super(actual);
			this.itemTimeout = itemTimeout;
			this.other = other;
			this.timeoutDescription = timeoutDescription;
		}

		/**
		 * 接收 source 的数据
		 * @param s
		 */
		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				set(s);
			}
		}

		@Override
		protected boolean shouldCancelCurrent() {
			return true;
		}

		/**
		 * 接收上游发射的数据
		 * @param t
		 */
		@Override
		public void onNext(T t) {
			timeout.cancel();

			long idx = index;
			// 代表已经超时了 现在不做处理
			if (idx == Long.MIN_VALUE) {
				s.cancel();
				Operators.onNextDropped(t, actual.currentContext());
				return;
			}
			// 代表已经发射了下一个数据 每次发射一个 index 就会递增 同时存在另一个用于检测 本对象是否超时未发送数据
			if (!INDEX.compareAndSet(this, idx, idx + 1)) {
				s.cancel();
				Operators.onNextDropped(t, actual.currentContext());
				return;
			}

			// 正常将数据发送到下游
			actual.onNext(t);

			producedOne();

			Publisher<? extends V> p;

			try {
				// 接收到的数据 又作为一个新的超时检测对象 在一定延时后会检测本对象是否接收到新数据
				p = Objects.requireNonNull(itemTimeout.apply(t),
						"The itemTimeout returned a null Publisher");
			}
			catch (Throwable e) {
				actual.onError(Operators.onOperatorError(this, e, t,
						actual.currentContext()));
				return;
			}

			TimeoutTimeoutSubscriber ts = new TimeoutTimeoutSubscriber(this, idx + 1);

			if (!setTimeout(ts)) {
				return;
			}

			p.subscribe(ts);
		}

		@Override
		public void onError(Throwable t) {
			long idx = index;
			if (idx == Long.MIN_VALUE) {
				Operators.onErrorDropped(t, actual.currentContext());
				return;
			}
			if (!INDEX.compareAndSet(this, idx, Long.MIN_VALUE)) {
				Operators.onErrorDropped(t, actual.currentContext());
				return;
			}

			cancelTimeout();

			actual.onError(t);
		}

		@Override
		public void onComplete() {
			long idx = index;
			if (idx == Long.MIN_VALUE) {
				return;
			}
			if (!INDEX.compareAndSet(this, idx, Long.MIN_VALUE)) {
				return;
			}

			cancelTimeout();

			actual.onComplete();
		}

		void cancelTimeout() {
			IndexedCancellable s = timeout;
			if (s != CancelledIndexedCancellable.INSTANCE) {
				s = TIMEOUT.getAndSet(this, CancelledIndexedCancellable.INSTANCE);
				if (s != null && s != CancelledIndexedCancellable.INSTANCE) {
					s.cancel();
				}
			}
		}

		@Override
		public void cancel() {
			index = Long.MIN_VALUE;
			cancelTimeout();
			super.cancel();
		}

		/**
		 * 设置超时回调对象
		 * @param newTimeout
		 * @return
		 */
		boolean setTimeout(IndexedCancellable newTimeout) {

			for (; ; ) {
				IndexedCancellable currentTimeout = timeout;

				if (currentTimeout == CancelledIndexedCancellable.INSTANCE) {
					newTimeout.cancel();
					return false;
				}

				if (currentTimeout != null && currentTimeout.index() >= newTimeout.index()) {
					newTimeout.cancel();
					return false;
				}

				if (TIMEOUT.compareAndSet(this, currentTimeout, newTimeout)) {
					if (currentTimeout != null) {
						currentTimeout.cancel();
					}
					return true;
				}
			}
		}

		/**
		 * 检测 index 为 i 的数据是否已经发射
		 * @param i
		 */
		void doTimeout(long i) {
			// 匹配的情况下才有检测的必要
			if (index == i && INDEX.compareAndSet(this, i, Long.MIN_VALUE)) {
				handleTimeout();
			}
		}

		void doError(long i, Throwable e) {
			if (index == i && INDEX.compareAndSet(this, i, Long.MIN_VALUE)) {
				super.cancel();

				actual.onError(e);
			}
		}

		/**
		 * 当定时检测器已经接收到数据(代表经过了一个检测的周期)  检测当前pub 是否正常下发数据
		 */
		void handleTimeout() {
			// 查看是否有降级措施
			if (other == null) {
				super.cancel();
				actual.onError(new TimeoutException("Did not observe any item or terminal signal within "
						+ timeoutDescription + " (and no fallback has been configured)"));
			}
			else {
				set(Operators.emptySubscription());

				// 使用降级对象作为数据源
				other.subscribe(new TimeoutOtherSubscriber<>(actual, this));
			}
		}
	}

	static final class TimeoutOtherSubscriber<T> implements CoreSubscriber<T> {

		final CoreSubscriber<? super T> actual;

		final Operators.MultiSubscriptionSubscriber<T, T> arbiter;

		TimeoutOtherSubscriber(CoreSubscriber<? super T> actual,
				Operators.MultiSubscriptionSubscriber<T, T> arbiter) {
			this.actual = actual;
			this.arbiter = arbiter;
		}

		@Override
		public Context currentContext() {
			return actual.currentContext();
		}

		@Override
		public void onSubscribe(Subscription s) {
			arbiter.set(s);
		}

		@Override
		public void onNext(T t) {
			actual.onNext(t);
		}

		@Override
		public void onError(Throwable t) {
			actual.onError(t);
		}

		@Override
		public void onComplete() {
			actual.onComplete();
		}
	}

	interface IndexedCancellable {

		long index();

		void cancel();
	}

	enum CancelledIndexedCancellable implements IndexedCancellable {
		INSTANCE;

		@Override
		public long index() {
			return Long.MAX_VALUE;
		}

		@Override
		public void cancel() {

		}

	}

	/**
	 * 超时对象
	 */
	static final class TimeoutTimeoutSubscriber
			implements Subscriber<Object>, IndexedCancellable {

		final TimeoutMainSubscriber<?, ?> main;

		final long index;

		volatile Subscription s;

		static final AtomicReferenceFieldUpdater<TimeoutTimeoutSubscriber, Subscription>
				S = AtomicReferenceFieldUpdater.newUpdater(TimeoutTimeoutSubscriber.class,
				Subscription.class,
				"s");

		/**
		 *
		 * @param main
		 * @param index  默认为0
		 */
		TimeoutTimeoutSubscriber(TimeoutMainSubscriber<?, ?> main, long index) {
			this.main = main;
			this.index = index;
		}

		/**
		 * 连接到 firstTimeout
		 * @param s
		 */
		@Override
		public void onSubscribe(Subscription s) {
			if (!S.compareAndSet(this, null, s)) {
				s.cancel();
				if (this.s != Operators.cancelledSubscription()) {
					Operators.reportSubscriptionSet();
				}
			}
			else {
				s.request(Long.MAX_VALUE);
			}
		}

		/**
		 * 接收数据
		 * @param t
		 */
		@Override
		public void onNext(Object t) {
			s.cancel();

			main.doTimeout(index);
		}

		@Override
		public void onError(Throwable t) {
			main.doError(index, t);
		}

		@Override
		public void onComplete() {
			main.doTimeout(index);
		}

		@Override
		public void cancel() {
			Subscription a = s;
			if (a != Operators.cancelledSubscription()) {
				a = S.getAndSet(this, Operators.cancelledSubscription());
				if (a != null && a != Operators.cancelledSubscription()) {
					a.cancel();
				}
			}
		}

		@Override
		public long index() {
			return index;
		}
	}
}
