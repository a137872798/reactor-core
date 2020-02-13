/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
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
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Combines the latest values from multiple sources through a function.
 *
 * @param <T> the value type of the sources
 * @param <R> the result type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 * 一组 flux 的聚合对象
 */
final class FluxCombineLatest<T, R> extends Flux<R> implements Fuseable, SourceProducer<R> {

	/**
	 * 内部包含的一组数据源对象
	 */
	final Publisher<? extends T>[] array;

	/**
	 * 数组对应的迭代器
	 */
	final Iterable<? extends Publisher<? extends T>> iterable;

	/**
	 * 将 array 组合成单个对象的函数
	 */
	final Function<Object[], R> combiner;

	/**
	 * 生成并发队列的函数
	 */
	final Supplier<? extends Queue<SourceAndArray>> queueSupplier;

	final int prefetch;

	/**
	 *
	 * @param array  一组数据源
	 * @param combiner  聚合函数
	 * @param queueSupplier  用于提供并发队列的函数
	 * @param prefetch
	 */
	FluxCombineLatest(Publisher<? extends T>[] array,
			Function<Object[], R> combiner,
			Supplier<? extends Queue<SourceAndArray>> queueSupplier, int prefetch) {
		if (prefetch <= 0) {
			throw new IllegalArgumentException("prefetch > 0 required but it was " + prefetch);
		}

		this.array = Objects.requireNonNull(array, "array");
		this.iterable = null;
		this.combiner = Objects.requireNonNull(combiner, "combiner");
		this.queueSupplier = Objects.requireNonNull(queueSupplier, "queueSupplier");
		this.prefetch = prefetch;
	}

	/**
	 * 可通过 array 或者迭代器进行初始化
	 * @param iterable
	 * @param combiner
	 * @param queueSupplier
	 * @param prefetch
	 */
	FluxCombineLatest(Iterable<? extends Publisher<? extends T>> iterable,
			Function<Object[], R> combiner,
			Supplier<? extends Queue<SourceAndArray>> queueSupplier, int prefetch) {
		if (prefetch < 0) {
			throw new IllegalArgumentException("prefetch > 0 required but it was " + prefetch);
		}

		this.array = null;
		this.iterable = Objects.requireNonNull(iterable, "iterable");
		this.combiner = Objects.requireNonNull(combiner, "combiner");
		this.queueSupplier = Objects.requireNonNull(queueSupplier, "queueSupplier");
		this.prefetch = prefetch;
	}

	@Override
	public int getPrefetch() {
		return prefetch;
	}

	/**
	 * 为 flux 对象设置订阅者
	 * @param actual the {@link Subscriber} interested into the published sequence
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void subscribe(CoreSubscriber<? super R> actual) {
		Publisher<? extends T>[] a = array;
		int n;
		// 该flux 内部 实际上组合了多个pub
		if (a == null) {
			n = 0;
			a = new Publisher[8];

			Iterator<? extends Publisher<? extends T>> it;

			// 代表通过迭代器 初始化
			try {
				it = Objects.requireNonNull(iterable.iterator(), "The iterator returned is null");
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
				// 将迭代器内的元素转移到 数组中
				a[n++] = p;
			}

		}
		else {
			n = a.length;
		}


		// 代表 内部没有元素 使用 emptySub触发函数
		if (n == 0) {
			Operators.complete(actual);
			return;
		}
		// 代表单个pub
		if (n == 1) {
			Function<T, R> f = t -> combiner.apply(new Object[]{t});
			if (a[0] instanceof Fuseable) {
				new FluxMapFuseable<>(from(a[0]), f).subscribe(actual);
				return;
			}
			else if (!(actual instanceof QueueSubscription)) {
				new FluxMap<>(from(a[0]), f).subscribe(actual);
				return;
			}
		}

		Queue<SourceAndArray> queue = queueSupplier.get();

		// 将数据包装成 subscription 并触发 onSubscribe
		CombineLatestCoordinator<T, R> coordinator =
				new CombineLatestCoordinator<>(actual, combiner, n, queue, prefetch);

		actual.onSubscribe(coordinator);

		// 将 数据填充到 subscription 中
		coordinator.subscribe(a, n);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.PREFETCH) return prefetch;
		return null;
	}

	/**
	 * 该对象内部包含一组 pub  该对象同时实现 pub sub 接口
	 * @param <T>
	 * @param <R>
	 */
	static final class CombineLatestCoordinator<T, R>
			implements QueueSubscription<R>, InnerProducer<R> {

		final Function<Object[], R>     combiner;
		final CombineLatestInner<T>[]   subscribers;
		/**
		 * 存储元素的队列
		 */
		final Queue<SourceAndArray>     queue;

		/**
		 * 每个 下标 对应的 pub 最后一个传播到下游的元素 每个 pub 可能都会传播多个元素到下游
		 */
		final Object[]                  latest;
		/**
		 * 当该flux 对象 实际被订阅时的订阅者
		 */
		final CoreSubscriber<? super R> actual;

		/**
		 * false 代表开启异步模式
		 */
		boolean outputFused;

		/**
		 * 标记 多个上游pub 中 已经有多少往下游发送过数据了
		 */
		int nonEmptySources;

		int completedSources;

		volatile boolean cancelled;

		/**
		 * 被请求拉取的数量
		 */
		volatile long requested;

		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<CombineLatestCoordinator> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(CombineLatestCoordinator.class,
						"requested");

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<CombineLatestCoordinator> WIP =
				AtomicIntegerFieldUpdater.newUpdater(CombineLatestCoordinator.class,
						"wip");

		volatile boolean done;

		volatile Throwable error;

		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<CombineLatestCoordinator, Throwable>
				ERROR =
				AtomicReferenceFieldUpdater.newUpdater(CombineLatestCoordinator.class,
						Throwable.class,
						"error");

		CombineLatestCoordinator(CoreSubscriber<? super R> actual,
				Function<Object[], R> combiner,
				int n,
				Queue<SourceAndArray> queue, int prefetch) {
		 	this.actual = actual;
			this.combiner = combiner;
			@SuppressWarnings("unchecked") CombineLatestInner<T>[] a =
					new CombineLatestInner[n];
			for (int i = 0; i < n; i++) {
				a[i] = new CombineLatestInner<>(this, i, prefetch);
			}
			this.subscribers = a;
			this.latest = new Object[n];
			this.queue = queue;
		}

		@Override
		public final CoreSubscriber<? super R> actual() {
			return actual;
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				// 更新已经请求的数量
				Operators.addCap(REQUESTED, this, n);
				// 从 flux 中拉取元素  首次触发 queue 还是空的 也就是没有元素传播到下游
				drain();
			}
		}

		/**
		 * 关闭该 subscription
		 */
		@Override
		public void cancel() {
			if (cancelled) {
				return;
			}
			cancelled = true;
			cancelAll();

			if (WIP.getAndIncrement(this) == 0) {
				clear();
			}
		}

		@Override
		public Stream<? extends Scannable> inners() {
			return Stream.of(subscribers);
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.CANCELLED) return cancelled;
			if (key == Attr.ERROR) return error;
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return requested;

			return InnerProducer.super.scanUnsafe(key);
		}

		/**
		 * 将pub 填充到 subscription 中
		 * 这里是一一对应的关系 也就是 一开始 flux 有多少个 pub 就会生成 内部包含多少 sub的 coordinator
		 * @param sources
		 * @param n
		 */
		void subscribe(Publisher<? extends T>[] sources, int n) {
			CombineLatestInner<T>[] a = subscribers;

			for (int i = 0; i < n; i++) {
				if (done || cancelled) {
					return;
				}
				// 该方法会将 a 包装成 subscription 并触发 request 从sub 拉取数据 这时触发了 onNext onComplete 等
				// 不是直接交由 CombineLatestInner 处理 而是委托给 coordinator
				sources[i].subscribe(a[i]);
			}
		}

		/**
		 * 代表下面的第几个 sub 触发了 onNext 方法
		 * @param index
		 * @param value
		 */
		void innerValue(int index, T value) {

			boolean replenishInsteadOfDrain;

			synchronized (this) {
				Object[] os = latest;

				int localNonEmptySources = nonEmptySources;

				if (os[index] == null) {
					localNonEmptySources++;
					nonEmptySources = localNonEmptySources;
				}

				os[index] = value;

				// 代表所有 pub 都已经传播过至少一次数据
				if (os.length == localNonEmptySources) {
					SourceAndArray sa =
							new SourceAndArray(subscribers[index], os.clone());

					// 代表 每个 pub 都已经成功 传播过数据后 接下去才会往队列中添加数据
					if (!queue.offer(sa)) {
						innerError(Operators.onOperatorError(this, Exceptions.failWithOverflow(Exceptions.BACKPRESSURE_ERROR_QUEUE_FULL), actual.currentContext()));
						return;
					}

					replenishInsteadOfDrain = false;
				}
				else {
					// 代表还处在填充阶段
					replenishInsteadOfDrain = true;
				}
			}

			if (replenishInsteadOfDrain) {
				subscribers[index].requestOne();
			}
			else {
				// 已经填充完毕 可以进行消费了
				drain();
			}
		}

		/**
		 * 当某个 pub 发送完所有的数据后
		 * @param index
		 */
		void innerComplete(int index) {
			synchronized (this) {
				Object[] os = latest;

				if (os[index] != null) {
					int localCompletedSources = completedSources + 1;

					// 所有元素都处理完时  done 变成 true
					if (localCompletedSources == os.length) {
						done = true;
					}
					else {
						completedSources = localCompletedSources;
						return;
					}
				}
				else {
					done = true;
				}
			}
			// 处理队列中的元素
			drain();
		}

		/**
		 * 当尝试继续添加元素 失败时触发
		 * @param e
		 */
		void innerError(Throwable e) {

			// 将异常信息 追加到  error 字段中
			if (Exceptions.addThrowable(ERROR, this, e)) {
				done = true;
				drain();
			}
			// 代表 该对象已经被 终止了
			else {
				// 丢弃队列中尚未处理的元素
				discardQueue(queue);
				Operators.onErrorDropped(e, actual.currentContext());
			}
		}

		/**
		 * 总结就是从 queue 中拉取数据 并将数据发往下游
		 */
		void drainOutput() {
			final CoreSubscriber<? super R> a = actual;
			final Queue<SourceAndArray> q = queue;

			int missed = 1;

			for (; ; ) {

				// 当前元素都处理完  丢弃队列中元素
				if (cancelled) {
					discardQueue(q);
					return;
				}

				// 产生了异常
				Throwable ex = error;
				if (ex != null) {
					discardQueue(q);
					a.onError(ex);
					return;
				}

				boolean d = done;

				boolean empty = q.isEmpty();

				// TODO 以异步模式 触发
				if (!empty) {
					a.onNext(null);
				}

				// 代表处理完所有元素了
				if (d && empty) {
					a.onComplete();
					return;
				}

				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}

		/**
		 * 以 异步模式拉取数据并发往下游  上面的同步方法 检测到queue中有元素 却触发 onNext(null)
		 * 而异步模式下 从队列中尽可能拉取数据
		 */
		void drainAsync() {
			final Queue<SourceAndArray> q = queue;

			int missed = 1;

			for (; ; ) {

				long r = requested;
				long e = 0L;

				while (e != r) {
					boolean d = done;

					// 从队列中获取单个元素
					SourceAndArray v = q.poll();

					boolean empty = v == null;

					// 检查是否处理完所有数据
					if (checkTerminated(d, empty, q)) {
						return;
					}

					if (empty) {
						break;
					}

					R w;

					// 将 queue 中的数据 也就是 array 通过结合函数 转换
					try {
						w = Objects.requireNonNull(combiner.apply(v.array), "Combiner returned null");
					}
					catch (Throwable ex) {
						Context ctx = actual.currentContext();
						Operators.onDiscardMultiple(Stream.of(v.array), ctx);

						ex = Operators.onOperatorError(this, ex,	v.array, ctx);
						Exceptions.addThrowable(ERROR, this, ex);
						//noinspection ConstantConditions
						ex = Exceptions.terminate(ERROR, this);
						actual.onError(ex);
						return;
					}

					// 将数据传播到下游
					actual.onNext(w);

					// 该对象连接到 flux的 pub 上 所以通过source 可以从 flux 拉取数据
					v.source.requestOne();

					e++;
				}

				if (e == r) {
					if (checkTerminated(done, q.isEmpty(), q)) {
						return;
					}
				}

				// 处理掉的请求数会减掉
				if (e != 0L && r != Long.MAX_VALUE) {
					REQUESTED.addAndGet(this, -e);
				}

				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}

		/**
		 * 拉取元素 并 转发到下游
		 */
		void drain() {
			// 必须确保 wip == 0 才能正常走流程
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}

			if (outputFused) {
				// 从队列中拉取元素 并发往下游
				drainOutput();
			}
			else {
				drainAsync();
			}
		}

		/**
		 * 检查是否处理完所有数据
		 * @param d
		 * @param empty
		 * @param q
		 * @return
		 */
		boolean checkTerminated(boolean d, boolean empty, Queue<SourceAndArray> q) {
			// 检测到 coordinator 已经被关闭
			if (cancelled) {
				cancelAll();
				discardQueue(q);
				return true;
			}

			// 代表处理完所有数据
			if (d) {
				Throwable e = Exceptions.terminate(ERROR, this);

				// 代表遇到了异常
				if (e != null && e != Exceptions.TERMINATED) {
					cancelAll();
					discardQueue(q);
					actual.onError(e);
					return true;
				}
				// 代表队列中没有要处理的数据了 允许关闭该对象
				else if (empty) {
					cancelAll();

					actual.onComplete();
					return true;
				}
			}
			return false;
		}

		/**
		 * 挨个关闭所有的 sub
		 */
		void cancelAll() {
			for (CombineLatestInner<T> inner : subscribers) {
				inner.cancel();
			}
		}

		/**
		 * 返回支持的聚合模式
		 * @param requestedMode the mode requested by the intermediate operator
		 * @return
		 */
		@Override
		public int requestFusion(int requestedMode) {
			if ((requestedMode & THREAD_BARRIER) != 0) {
				return NONE;
			}
			int m = requestedMode & ASYNC;
			outputFused = m != 0;
			return m;
		}

		/**
		 * 代表该 subscription 内部的数据 可以通过 poll 进行拉取
		 * @return
		 */
		@Override
		@Nullable
		public R poll() {
			SourceAndArray e = queue.poll();
			if (e == null) {
				return null;
			}
			R r = combiner.apply(e.array);
			e.source.requestOne();
			return r;
		}

		/**
		 * 触发丢弃队列元素的 模板
		 * @param q
		 */
		private void discardQueue(Queue<SourceAndArray> q) {
			Operators.onDiscardQueueWithClear(q, actual.currentContext(), SourceAndArray::toStream);
		}

		@Override
		public void clear() {
			discardQueue(queue);
		}

		@Override
		public boolean isEmpty() {
			return queue.isEmpty();
		}

		@Override
		public int size() {
			return queue.size();
		}
	}

	/**
	 * 对应一个 sub 对象
	 * @param <T>
	 */
	static final class CombineLatestInner<T>
			implements InnerConsumer<T> {

		/**
		 * 该对象 属于哪个 coordinator (协调员)
		 */
		final CombineLatestCoordinator<T, ?> parent;

		final int index;

		final int prefetch;

		final int limit;

		volatile Subscription s;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<CombineLatestInner, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(CombineLatestInner.class,
						Subscription.class,
						"s");

		int produced;

		 CombineLatestInner(CombineLatestCoordinator<T, ?> parent,
				int index,
				int prefetch) {
			this.parent = parent;
			this.index = index;
			this.prefetch = prefetch;
			this.limit = Operators.unboundedOrLimit(prefetch);
		}
		
		@Override
		public Context currentContext() {
			return parent.actual.currentContext();
		}


		/**
		 * 同 普通的 subscriber 一样
		 * @param s
		 */
		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.setOnce(S, this, s)) {
				s.request(Operators.unboundedOrPrefetch(prefetch));
			}
		}

		// 核心方法委托给 coordinator

		@Override
		public void onNext(T t) {
			parent.innerValue(index, t);
		}

		@Override
		public void onError(Throwable t) {
			parent.innerError(t);
		}

		@Override
		public void onComplete() {
			parent.innerComplete(index);
		}

		public void cancel() {
			Operators.terminate(S, this);
		}

		/**
		 * 继续从 sub 中拉取数据
		 */
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
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return  s;
			if (key == Attr.ACTUAL) return parent;
			if (key == Attr.CANCELLED) return s == Operators.cancelledSubscription();
			if (key == Attr.PREFETCH) return prefetch;

			return null;
		}
	}

	/**
	 * The queue element type for internal use with FluxCombineLatest.
	 */
	static final class SourceAndArray {

		/**
		 * 本次触发onNext 的 订阅者 (该对象会匹配到上游的一个 pub)
		 */
		final CombineLatestInner<?> source;
		/**
		 * 记录当前 所有 pub 传播数据的快照
		 */
		final Object[]              array;

		SourceAndArray(CombineLatestInner<?> source, Object[] array) {
			this.source = source;
			this.array = array;
		}

		final Stream<?> toStream() {
			return Stream.of(this.array);
		}
	}
}
