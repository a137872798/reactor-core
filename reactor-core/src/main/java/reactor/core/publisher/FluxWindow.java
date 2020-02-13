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

import java.util.ArrayDeque;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Splits the source sequence into possibly overlapping publishers.
 *
 * @param <T> the value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">https://github.com/reactor/reactive-streams-commons</a>
 * 每当接收到数据时 回向下游发射一个 包含 maxSize元素的 flux对象
 */
final class FluxWindow<T> extends InternalFluxOperator<T, Flux<T>> {

	final int size;

	final int skip;

	final Supplier<? extends Queue<T>> processorQueueSupplier;

	final Supplier<? extends Queue<UnicastProcessor<T>>> overflowQueueSupplier;

	FluxWindow(Flux<? extends T> source,
			int size,
			Supplier<? extends Queue<T>> processorQueueSupplier) {
		super(source);
		if (size <= 0) {
			throw new IllegalArgumentException("size > 0 required but it was " + size);
		}
		this.size = size;
		this.skip = size;
		this.processorQueueSupplier =
				Objects.requireNonNull(processorQueueSupplier, "processorQueueSupplier");
		this.overflowQueueSupplier = null; // won't be needed here
	}

	FluxWindow(Flux<? extends T> source,
			int size,   // 代表每个 window 会发射多少数据
			int skip,   // 代表每发射多少个元素 会创建一个新的 window
			Supplier<? extends Queue<T>> processorQueueSupplier,
			Supplier<? extends Queue<UnicastProcessor<T>>> overflowQueueSupplier) {
		super(source);
		if (size <= 0) {
			throw new IllegalArgumentException("size > 0 required but it was " + size);
		}
		if (skip <= 0) {
			throw new IllegalArgumentException("skip > 0 required but it was " + skip);
		}
		this.size = size;
		this.skip = skip;
		this.processorQueueSupplier =
				Objects.requireNonNull(processorQueueSupplier, "processorQueueSupplier");
		this.overflowQueueSupplier =
				Objects.requireNonNull(overflowQueueSupplier, "overflowQueueSupplier");
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super Flux<T>> actual) {
		// 默认情况返回这个
		if (skip == size) {
			return new WindowExactSubscriber<>(actual,
					size,
					processorQueueSupplier);
		}
		else if (skip > size) {
			return new WindowSkipSubscriber<>(actual,
					size, skip, processorQueueSupplier);
		}
		else {
			return new WindowOverlapSubscriber<>(actual,
					size,
					skip, processorQueueSupplier, overflowQueueSupplier.get());
		}
	}

	/**
	 * 代表skip 和size一致
	 * @param <T>
	 */
	static final class WindowExactSubscriber<T>
			implements Disposable, InnerOperator<T, Flux<T>> {

		final CoreSubscriber<? super Flux<T>> actual;

		final Supplier<? extends Queue<T>> processorQueueSupplier;

        /**
         * 代表每个 window 会发射多少数据 同时每发射多少个元素就生成一个新的 window
         */
		final int size;

		volatile int cancelled;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<WindowExactSubscriber> CANCELLED =
				AtomicIntegerFieldUpdater.newUpdater(WindowExactSubscriber.class, "cancelled");

		volatile int windowCount;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<WindowExactSubscriber> WINDOW_COUNT =
				AtomicIntegerFieldUpdater.newUpdater(WindowExactSubscriber.class, "windowCount");

		int index;

		Subscription s;

        /**
         * 该对象可以看作下游只有一个订阅者的  中转站对象
         */
		UnicastProcessor<T> window;

		boolean done;

		WindowExactSubscriber(CoreSubscriber<? super Flux<T>> actual,
				int size,
				Supplier<? extends Queue<T>> processorQueueSupplier) {
			this.actual = actual;
			this.size = size;
			this.processorQueueSupplier = processorQueueSupplier;
			WINDOW_COUNT.lazySet(this, 1);
		}

		/**
		 * 设置上游对象
		 * @param s
		 */
		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;
				actual.onSubscribe(this);
			}
		}

		/**
		 * 接收上游发射的数据
		 * @param t
		 */
		@Override
		public void onNext(T t) {
			if (done) {
				Operators.onNextDropped(t, actual.currentContext());
				return;
			}

			int i = index;

			UnicastProcessor<T> w = window;
			// 代表第一次收到数据 此时增加窗口数
			if (cancelled == 0 && i == 0) {
				WINDOW_COUNT.getAndIncrement(this);

				// 注意下发的对象是一个 pub 对象
				w = new UnicastProcessor<>(processorQueueSupplier.get(), this);
				window = w;

				actual.onNext(w);
			}

			i++;

			// 往pub 中下发数据  这样从下游看来 就是订阅者先收到 pub 然后针对该pub的订阅者 收到了一个数据
			w.onNext(t);

			// 代表发送的数据填满了一个窗口 这时就要关闭该窗口  之后下发的数据会生成一个新的窗口对象
			if (i == size) {
				index = 0;
				window = null;
				w.onComplete();
			}
			else {
				index = i;
			}
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, actual.currentContext());
				return;
			}
			done = true;
			Processor<T, T> w = window;
			if (w != null) {
				window = null;
				w.onError(t);
			}

			actual.onError(t);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			Processor<T, T> w = window;
			if (w != null) {
				window = null;
				w.onComplete();
			}

			actual.onComplete();
		}

		/**
		 * 向上游拉取数据
		 * @param n
		 */
		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				long u = Operators.multiplyCap(size, n);
				s.request(u);
			}
		}

		@Override
		public void cancel() {
			if (CANCELLED.compareAndSet(this, 0, 1)) {
				dispose();
			}
		}

		@Override
		public void dispose() {
			if (WINDOW_COUNT.decrementAndGet(this) == 0) {
				s.cancel();
			}
		}

		@Override
		public CoreSubscriber<? super Flux<T>> actual() {
			return actual;
		}

		@Override
		public boolean isDisposed() {
			return cancelled == 1 || done;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.CANCELLED) return cancelled == 1;
			if (key == Attr.CAPACITY) return size;
			if (key == Attr.TERMINATED) return done;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public Stream<? extends Scannable> inners() {
			return Stream.of(window);
		}
	}

	static final class WindowSkipSubscriber<T>
			implements Disposable, InnerOperator<T, Flux<T>> {

		final CoreSubscriber<? super Flux<T>> actual;
		final Context                         ctx;

		final Supplier<? extends Queue<T>> processorQueueSupplier;

		final int size;

		final int skip;

		volatile int cancelled;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<WindowSkipSubscriber> CANCELLED =
				AtomicIntegerFieldUpdater.newUpdater(WindowSkipSubscriber.class, "cancelled");

		volatile int windowCount;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<WindowSkipSubscriber> WINDOW_COUNT =
				AtomicIntegerFieldUpdater.newUpdater(WindowSkipSubscriber.class, "windowCount");

		volatile int firstRequest;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<WindowSkipSubscriber> FIRST_REQUEST =
				AtomicIntegerFieldUpdater.newUpdater(WindowSkipSubscriber.class,
						"firstRequest");

		int index;

		Subscription s;

		UnicastProcessor<T> window;

		boolean done;

		WindowSkipSubscriber(CoreSubscriber<? super Flux<T>> actual,
				int size,
				int skip,
				Supplier<? extends Queue<T>> processorQueueSupplier) {
			this.actual = actual;
			this.ctx = actual.currentContext();
			this.size = size;
			this.skip = skip;
			this.processorQueueSupplier = processorQueueSupplier;
			WINDOW_COUNT.lazySet(this, 1);
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;
				actual.onSubscribe(this);
			}
		}

		@Override
		public void onNext(T t) {
			if (done) {
				Operators.onNextDropped(t, ctx);
				return;
			}

			int i = index;

			UnicastProcessor<T> w = window;
			if (i == 0) {
				WINDOW_COUNT.getAndIncrement(this);

				w = new UnicastProcessor<>(processorQueueSupplier.get(), this);
				window = w;

				actual.onNext(w);
			}

			i++;

			if (w != null) {
				w.onNext(t);
			}
			else {
				Operators.onDiscard(t, ctx);
			}

			if (i == size) {
				window = null;
				if (w != null) {
					w.onComplete();
				}
			}

			if (i == skip) {
				index = 0;
			}
			else {
				index = i;
			}
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, ctx);
				return;
			}
			done = true;

			Processor<T, T> w = window;
			if (w != null) {
				window = null;
				w.onError(t);
			}

			actual.onError(t);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;

			Processor<T, T> w = window;
			if (w != null) {
				window = null;
				w.onComplete();
			}

			actual.onComplete();
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				if (firstRequest == 0 && FIRST_REQUEST.compareAndSet(this, 0, 1)) {
					long u = Operators.multiplyCap(size, n);
					long v = Operators.multiplyCap(skip - size, n - 1);
					long w = Operators.addCap(u, v);
					s.request(w);
				}
				else {
					long u = Operators.multiplyCap(skip, n);
					s.request(u);
				}
			}
		}

		@Override
		public void cancel() {
			if (CANCELLED.compareAndSet(this, 0, 1)) {
				dispose();
			}
		}

		@Override
		public boolean isDisposed() {
			return cancelled == 1 || done;
		}

		@Override
		public void dispose() {
			if (WINDOW_COUNT.decrementAndGet(this) == 0) {
				s.cancel();
			}
		}

		@Override
		public CoreSubscriber<? super Flux<T>> actual() {
			return actual;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.CANCELLED) return cancelled == 1;
			if (key == Attr.CAPACITY) return size;
			if (key == Attr.TERMINATED) return done;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public Stream<? extends Scannable> inners() {
			return Stream.of(window);
		}
	}

	static final class WindowOverlapSubscriber<T> extends ArrayDeque<UnicastProcessor<T>>
			implements Disposable, InnerOperator<T, Flux<T>> {

		final CoreSubscriber<? super Flux<T>> actual;

		final Supplier<? extends Queue<T>> processorQueueSupplier;

		final Queue<UnicastProcessor<T>> queue;

		final int size;

		final int skip;

		volatile int cancelled;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<WindowOverlapSubscriber> CANCELLED =
				AtomicIntegerFieldUpdater.newUpdater(WindowOverlapSubscriber.class,
						"cancelled");

		volatile int windowCount;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<WindowOverlapSubscriber> WINDOW_COUNT =
				AtomicIntegerFieldUpdater.newUpdater(WindowOverlapSubscriber.class,
						"windowCount");

		volatile int firstRequest;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<WindowOverlapSubscriber> FIRST_REQUEST =
				AtomicIntegerFieldUpdater.newUpdater(WindowOverlapSubscriber.class,
						"firstRequest");

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<WindowOverlapSubscriber> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(WindowOverlapSubscriber.class,
						"requested");

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<WindowOverlapSubscriber> WIP =
				AtomicIntegerFieldUpdater.newUpdater(WindowOverlapSubscriber.class, "wip");

		int index;

		int produced;

		Subscription s;

		volatile boolean done;
		Throwable error;

		WindowOverlapSubscriber(CoreSubscriber<? super Flux<T>> actual,
				int size,
				int skip,
				Supplier<? extends Queue<T>> processorQueueSupplier,
				Queue<UnicastProcessor<T>> overflowQueue) {
			this.actual = actual;
			this.size = size;
			this.skip = skip;
			this.processorQueueSupplier = processorQueueSupplier;
			WINDOW_COUNT.lazySet(this, 1);
			this.queue = overflowQueue;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;
				actual.onSubscribe(this);
			}
		}

		@Override
		public void onNext(T t) {
			if (done) {
				Operators.onNextDropped(t, actual.currentContext());
				return;
			}

			int i = index;

			if (i == 0) {
				if (cancelled == 0) {
					WINDOW_COUNT.getAndIncrement(this);

					UnicastProcessor<T> w = new UnicastProcessor<>(processorQueueSupplier.get(), this);

					offer(w);

					queue.offer(w);
					drain();
				}
			}

			i++;

			for (Processor<T, T> w : this) {
				w.onNext(t);
			}

			int p = produced + 1;
			if (p == size) {
				produced = p - skip;

				Processor<T, T> w = poll();
				if (w != null) {
					w.onComplete();
				}
			}
			else {
				produced = p;
			}

			if (i == skip) {
				index = 0;
			}
			else {
				index = i;
			}
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, actual.currentContext());
				return;
			}
			done = true;

			for (Processor<T, T> w : this) {
				w.onError(t);
			}
			clear();

			error = t;
			drain();
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;

			for (Processor<T, T> w : this) {
				w.onComplete();
			}
			clear();

			drain();
		}

		void drain() {
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}

			final Subscriber<? super Flux<T>> a = actual;
			final Queue<UnicastProcessor<T>> q = queue;
			int missed = 1;

			for (; ; ) {

				long r = requested;
				long e = 0;

				while (e != r) {
					boolean d = done;

					UnicastProcessor<T> t = q.poll();

					boolean empty = t == null;

					if (checkTerminated(d, empty, a, q)) {
						return;
					}

					if (empty) {
						break;
					}

					a.onNext(t);

					e++;
				}

				if (e == r) {
					if (checkTerminated(done, q.isEmpty(), a, q)) {
						return;
					}
				}

				if (e != 0L && r != Long.MAX_VALUE) {
					REQUESTED.addAndGet(this, -e);
				}

				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}

		boolean checkTerminated(boolean d, boolean empty, Subscriber<?> a, Queue<?> q) {
			if (cancelled == 1) {
				q.clear();
				return true;
			}

			if (d) {
				Throwable e = error;

				if (e != null) {
					q.clear();
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

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {

				Operators.addCap(REQUESTED, this, n);

				if (firstRequest == 0 && FIRST_REQUEST.compareAndSet(this, 0, 1)) {
					long u = Operators.multiplyCap(skip, n - 1);
					long v = Operators.addCap(size, u);
					s.request(v);
				}
				else {
					long u = Operators.multiplyCap(skip, n);
					s.request(u);
				}

				drain();
			}
		}

		@Override
		public void cancel() {
			if (CANCELLED.compareAndSet(this, 0, 1)) {
				dispose();
			}
		}

		@Override
		public void dispose() {
			if (WINDOW_COUNT.decrementAndGet(this) == 0) {
				s.cancel();
			}
		}

		@Override
		public CoreSubscriber<? super Flux<T>> actual() {
			return actual;
		}

		@Override
		public boolean isDisposed() {
			return cancelled == 1 || done;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.CANCELLED) return cancelled == 1;
			if (key == Attr.CAPACITY) return size;
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.LARGE_BUFFERED) return (long) queue.size() + size();
			if (key == Attr.BUFFERED) {
				long realBuffered = (long) queue.size() + size();
				if (realBuffered < Integer.MAX_VALUE) return (int) realBuffered;
				return Integer.MIN_VALUE;
			}
			if (key == Attr.ERROR) return error;
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return requested;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public Stream<? extends Scannable> inners() {
			return Stream.of(toArray())
			             .map(Scannable::from);
		}
	}

}
