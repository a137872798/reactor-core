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
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Switches to a new Publisher generated via a function whenever the upstream produces an
 * item.
 *
 * @param <T> the source value type
 * @param <R> the output value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 * 上游每下发一个元素都会被转换成 pub 之后 再消费 pub 的数据 如果此时上游发射了一个新的元素 那么终止之前的pub
 */
final class FluxSwitchMap<T, R> extends InternalFluxOperator<T, R> {

	final Function<? super T, ? extends Publisher<? extends R>> mapper;

	final Supplier<? extends Queue<Object>> queueSupplier;

	final int prefetch;

	@SuppressWarnings("ConstantConditions")
	static final SwitchMapInner<Object> CANCELLED_INNER =
			new SwitchMapInner<>(null, 0, Long.MAX_VALUE);

	FluxSwitchMap(Flux<? extends T> source,
			Function<? super T, ? extends Publisher<? extends R>> mapper,
			Supplier<? extends Queue<Object>> queueSupplier,
			int prefetch) {
		super(source);
		if (prefetch <= 0) {
			throw new IllegalArgumentException("prefetch > 0 required but it was " +
					prefetch);
		}
		this.mapper = Objects.requireNonNull(mapper, "mapper");
		this.queueSupplier = Objects.requireNonNull(queueSupplier, "queueSupplier");
		this.prefetch = prefetch;
	}

	@Override
	public int getPrefetch() {
		return Integer.MAX_VALUE;
	}

	/**
	 * 为该对象设置订阅者
	 * @param actual
	 * @return
	 */
	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super R> actual) {
		//for now switchMap doesn't support onErrorContinue, so the scalar version shouldn't either
		if (FluxFlatMap.trySubscribeScalarMap(source, actual, mapper, false, false)) {
			return null;
		}

		return new SwitchMapMain<T, R>(actual,
				mapper,
				queueSupplier.get(), prefetch);
	}

	/**
	 * 切换对象
	 * @param <T>
	 * @param <R>
	 */
	static final class SwitchMapMain<T, R> implements InnerOperator<T, R> {

		final Function<? super T, ? extends Publisher<? extends R>> mapper;

		final Queue<Object>               queue;
		final BiPredicate<Object, Object> queueBiAtomic;
		final int                         prefetch;
		final CoreSubscriber<? super R>   actual;

		Subscription s;

		volatile boolean done;

		volatile Throwable error;

		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<SwitchMapMain, Throwable> ERROR =
				AtomicReferenceFieldUpdater.newUpdater(SwitchMapMain.class,
						Throwable.class,
						"error");

		volatile boolean cancelled;

		volatile int once;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<SwitchMapMain> ONCE =
				AtomicIntegerFieldUpdater.newUpdater(SwitchMapMain.class, "once");

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<SwitchMapMain> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(SwitchMapMain.class, "requested");

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<SwitchMapMain> WIP =
				AtomicIntegerFieldUpdater.newUpdater(SwitchMapMain.class, "wip");

		volatile SwitchMapInner<R> inner;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<SwitchMapMain, SwitchMapInner> INNER =
				AtomicReferenceFieldUpdater.newUpdater(SwitchMapMain.class,
						SwitchMapInner.class,
						"inner");

		volatile long index;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<SwitchMapMain> INDEX =
				AtomicLongFieldUpdater.newUpdater(SwitchMapMain.class, "index");

		volatile int active;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<SwitchMapMain> ACTIVE =
				AtomicIntegerFieldUpdater.newUpdater(SwitchMapMain.class, "active");

		@SuppressWarnings("unchecked")
		SwitchMapMain(CoreSubscriber<? super R> actual,
				Function<? super T, ? extends Publisher<? extends R>> mapper,
				Queue<Object> queue,
				int prefetch) {
			this.actual = actual;
			this.mapper = mapper;
			this.queue = queue;
			this.prefetch = prefetch;
			this.active = 1;
			if(queue instanceof BiPredicate){
				this.queueBiAtomic = (BiPredicate<Object, Object>) queue;
			}
			else {
				this.queueBiAtomic = null;
			}
		}

		@Override
		public final CoreSubscriber<? super R> actual() {
			return actual;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.CANCELLED) return cancelled;
			if (key == Attr.PARENT) return s;
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.ERROR) return error;
			if (key == Attr.PREFETCH) return prefetch;
			if (key == Attr.BUFFERED) return queue.size();
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return requested;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public Stream<? extends Scannable> inners() {
			return Stream.of(inner);
		}

		/**
		 * 入口
		 * @param s
		 */
		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				// 触发本对象的 request 方法  主要就是增加 requested
				actual.onSubscribe(this);

				// 向上游拉取数据 会转发到 onNext
				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onNext(T t) {
			if (done) {
				Operators.onNextDropped(t, actual.currentContext());
				return;
			}

			// 每当上游发射一个数据时 修改偏移量
			long idx = INDEX.incrementAndGet(this);

			// 关闭之前的 对象
			SwitchMapInner<R> si = inner;
			if (si != null) {
				si.deactivate();
				si.cancel();
			}

			Publisher<? extends R> p;

			try {
				p = Objects.requireNonNull(mapper.apply(t),
				"The mapper returned a null publisher");
			}
			catch (Throwable e) {
				onError(Operators.onOperatorError(s, e, t, actual.currentContext()));
				return;
			}

			SwitchMapInner<R> innerSubscriber =
					new SwitchMapInner<>(this, prefetch, idx);

			// 标记当前 inner 对象
			if (INNER.compareAndSet(this, si, innerSubscriber)) {
				ACTIVE.getAndIncrement(this);
				p.subscribe(innerSubscriber);
			}
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, actual.currentContext());
				return;
			}

			if (Exceptions.addThrowable(ERROR, this, t)) {

				if (ONCE.compareAndSet(this, 0, 1)) {
					deactivate();
				}

				cancelInner();
				done = true;
				drain();
			}
			else {
				Operators.onErrorDropped(t, actual.currentContext());
			}
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}

			if (ONCE.compareAndSet(this, 0, 1)) {
				deactivate();
			}

			done = true;
			drain();
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Operators.addCap(REQUESTED, this, n);
				drain();
			}
		}

		@Override
		public void cancel() {
			if (!cancelled) {
				cancelled = true;

				if (WIP.getAndIncrement(this) == 0) {
					cancelAndCleanup(queue);
				}
			}
		}

		void deactivate() {
			ACTIVE.decrementAndGet(this);
		}

		void cancelInner() {
			SwitchMapInner<?> si = INNER.getAndSet(this, CANCELLED_INNER);
			if (si != null && si != CANCELLED_INNER) {
				si.cancel();
				si.deactivate();
			}
		}

		void cancelAndCleanup(Queue<?> q) {
			s.cancel();

			cancelInner();

			q.clear();
		}

		/**
		 * 处理数据
		 */
		void drain() {
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}

			Subscriber<? super R> a = actual;
			Queue<Object> q = queue;

			int missed = 1;

			for (; ; ) {

				long r = requested;
				long e = 0L;

				while (r != e) {
					boolean d = active == 0;

					// 拉取数据对应的 子对象
					@SuppressWarnings("unchecked") SwitchMapInner<R> si =
							(SwitchMapInner<R>) q.poll();

					boolean empty = si == null;

					if (checkTerminated(d, empty, a, q)) {
						return;
					}

					if (empty) {
						break;
					}

					// 拉取上游传过来的实际数据
					Object second;

					while ((second = q.poll()) == null) {
					}

					// 匹配成功 往下游传播数据
					if (index == si.index) {

						@SuppressWarnings("unchecked") R v = (R) second;

						a.onNext(v);

						si.requestOne();

						e++;
					}
				}

				if (r == e) {
					if (checkTerminated(active == 0, q.isEmpty(), a, q)) {
						return;
					}
				}

				if (e != 0 && r != Long.MAX_VALUE) {
					REQUESTED.addAndGet(this, -e);
				}

				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}

		boolean checkTerminated(boolean d, boolean empty, Subscriber<?> a, Queue<?> q) {
			if (cancelled) {
				cancelAndCleanup(q);
				return true;
			}

			if (d) {
				Throwable e = Exceptions.terminate(ERROR, this);
				if (e != null && e != Exceptions.TERMINATED) {
					cancelAndCleanup(q);

					a.onError(e);
					return true;
				}
				else if (empty) {
					a.onComplete();
					return true;
				}

			}
			return false;
		}

		/**
		 * 接收从上游发来的数据
		 * @param inner
		 * @param value
		 */
		void innerNext(SwitchMapInner<R> inner, R value) {
			// 插入队列的函数  reactor 内部有一些队列的插入可以串一个谓语参数 就比如当前对象
			if (queueBiAtomic != null) {
				//the queue is a "BiQueue" from Reactor, test(A, B) actually does double insertion
				queueBiAtomic.test(inner, value);
			}
			else {
				//the queue is a regular queue, a bit more overhead to do double insertion
				queue.offer(inner);
				queue.offer(value);
			}
			// 插入后尝试拉取数据
			drain();
		}

		void innerError(SwitchMapInner<R> inner, Throwable e) {
			if (Exceptions.addThrowable(ERROR, this, e)) {
				s.cancel();

				if (ONCE.compareAndSet(this, 0, 1)) {
					deactivate();
				}
				inner.deactivate();
				drain();
			}
			else {
				Operators.onErrorDropped(e, actual.currentContext());
			}
		}

		void innerComplete(SwitchMapInner<R> inner) {
			inner.deactivate();
			drain();
		}
	}

	/**
	 * Main 对应的子对象
	 * @param <R>
	 */
	static final class SwitchMapInner<R> implements InnerConsumer<R>, Subscription {

		final SwitchMapMain<?, R> parent;

		final int prefetch;

		final int limit;

		final long index;

		volatile int once;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<SwitchMapInner> ONCE =
				AtomicIntegerFieldUpdater.newUpdater(SwitchMapInner.class, "once");

		volatile Subscription s;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<SwitchMapInner, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(SwitchMapInner.class,
						Subscription.class,
						"s");

		int produced;

		SwitchMapInner(SwitchMapMain<?, R> parent, int prefetch, long index) {
			this.parent = parent;
			this.prefetch = prefetch;
			this.limit = Operators.unboundedOrLimit(prefetch);
			this.index = index;
		}

		@Override
		public Context currentContext() {
			return parent.currentContext();
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.CANCELLED) return s == Operators.cancelledSubscription();
			if (key == Attr.PARENT) return s;
			if (key == Attr.ACTUAL) return parent;
			if (key == Attr.PREFETCH) return prefetch;

			return null;
		}

		/**
		 * 指定 subscription 并拉取数据
		 * @param s
		 */
		@Override
		public void onSubscribe(Subscription s) {
			Subscription a = this.s;
			if (a == Operators.cancelledSubscription()) {
				s.cancel();
			}
			if (a != null) {
				s.cancel();

				Operators.reportSubscriptionSet();
				return;
			}

			if (S.compareAndSet(this, null, s)) {
				s.request(Operators.unboundedOrPrefetch(prefetch));
				return;
			}
			a = this.s;
			if (a != Operators.cancelledSubscription()) {
				s.cancel();

				Operators.reportSubscriptionSet();
			}
		}

		@Override
		public void onNext(R t) {
			parent.innerNext(this, t);
		}

		@Override
		public void onError(Throwable t) {
			parent.innerError(this, t);
		}

		@Override
		public void onComplete() {
			parent.innerComplete(this);
		}

		void deactivate() {
			if (ONCE.compareAndSet(this, 0, 1)) {
				parent.deactivate();
			}
		}

		void requestOne() {
			int p = produced + 1;
			if (p == limit) {
				produced = 0;
				s.request(p);
			}
			else {
				produced = p;
			}
		}

		@Override
		public void request(long n) {
			long p = produced + n;
			if (p >= limit) {
				produced = 0;
				s.request(p);
			}
			else {
				produced = (int) p;
			}
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
	}
}
