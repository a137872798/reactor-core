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
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiFunction;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Reduces all 'rails' into a single value which then gets reduced into a single
 * Publisher sequence.
 *
 * @param <T> the value type
 *           将上游并行结果 结合成一个数据 并下发
 */
final class ParallelMergeReduce<T> extends Mono<T> implements Scannable, Fuseable {

	final ParallelFlux<? extends T> source;

	final BiFunction<T, T, T> reducer;

	ParallelMergeReduce(ParallelFlux<? extends T> source,
			BiFunction<T, T, T> reducer) {
		this.source = source;
		this.reducer = reducer;
	}

	@Override
	@Nullable
	public Object scanUnsafe(Attr key) {
		if (key == Attr.PARENT) return source;

		return null;
	}

	/**
	 * 针对 mono 拉取数据
	 * @param actual the {@link Subscriber} interested into the published sequence
	 */
	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		MergeReduceMain<T> parent =
				new MergeReduceMain<>(actual, source.parallelism(), reducer);
		// 这里会打上一个 触发过request 的标记 但是数据不是通过该对象拉取的
		actual.onSubscribe(parent);

		// 将数据发到子对象上
		source.subscribe(parent.subscribers);
	}

	/**
	 * 转发对象
	 * @param <T>
	 */
	static final class MergeReduceMain<T>
			extends Operators.MonoSubscriber<T, T> {

		/**
		 * 模拟的订阅者 用于从parallelFlux 拉取数据
		 */
		final MergeReduceInner<T>[] subscribers;

		final BiFunction<T, T, T> reducer;

		volatile SlotPair<T> current;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<MergeReduceMain, SlotPair>
				CURRENT = AtomicReferenceFieldUpdater.newUpdater(
				MergeReduceMain.class,
				SlotPair.class,
				"current");

		volatile int remaining;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<MergeReduceMain>
				REMAINING = AtomicIntegerFieldUpdater.newUpdater(
				MergeReduceMain.class,
				"remaining");

		volatile Throwable error;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<MergeReduceMain, Throwable>
				ERROR = AtomicReferenceFieldUpdater.newUpdater(
				MergeReduceMain.class,
				Throwable.class,
				"error");

		/**
		 * 根据上游并行度创建对应长度数组
		 * @param subscriber
		 * @param n
		 * @param reducer
		 */
		MergeReduceMain(CoreSubscriber<? super T> subscriber,
				int n,
				BiFunction<T, T, T> reducer) {
			super(subscriber);
			@SuppressWarnings("unchecked") MergeReduceInner<T>[] a =
					new MergeReduceInner[n];
			for (int i = 0; i < n; i++) {
				a[i] = new MergeReduceInner<>(this, reducer);
			}
			this.subscribers = a;
			this.reducer = reducer;
			REMAINING.lazySet(this, n);
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.ERROR) return error;
			if (key == Attr.TERMINATED) return REMAINING.get(this) == 0;

			return super.scanUnsafe(key);
		}

		/**
		 * 代表某一分流的累加结果已经完成
		 * @param value
		 * @return
		 */
		@Nullable
		SlotPair<T> addValue(T value) {
			for (; ; ) {
				SlotPair<T> curr = current;

				if (curr == null) {
					// 设置当前 pair
					curr = new SlotPair<>();
					if (!CURRENT.compareAndSet(this, null, curr)) {
						continue;
					}
				}

				// 增加 acquire 的值
				int c = curr.tryAcquireSlot();
				// 代表超出负荷了  tryAcquireSlot()该方法只能执行2次
				if (c < 0) {
					CURRENT.compareAndSet(this, curr, null);
					continue;
				}
				// 代表第一次设置
				if (c == 0) {
					curr.first = value;
				}
				else {
					// 代表第二次设置
					curr.second = value;
				}

				// 代表填充2个值后 会返回该对象
				if (curr.releaseSlot()) {
					CURRENT.compareAndSet(this, curr, null);
					return curr;
				}
				return null;
			}
		}

		@Override
		public void cancel() {
			for (MergeReduceInner<T> inner : subscribers) {
				inner.cancel();
			}
			super.cancel();
		}

		void innerError(Throwable ex) {
			if(ERROR.compareAndSet(this, null, ex)){
				cancel();
				actual.onError(ex);
			}
			else if(error != ex) {
				Operators.onErrorDropped(ex, actual.currentContext());
			}
		}

		/**
		 * 接收上游分流的数据结果
		 * @param value
		 */
		void innerComplete(@Nullable T value) {
			if (value != null) {
				for (; ; ) {
					// 设置current 为一个slot 对象 内部可以存放2个结果
					SlotPair<T> sp = addValue(value);

					if (sp != null) {

						try {
							value = Objects.requireNonNull(reducer.apply(sp.first,
									sp.second), "The reducer returned a null value");
						}
						catch (Throwable ex) {
							innerError(Operators.onOperatorError(this, ex,
									actual.currentContext()));
							return;
						}
					}
					else {
						// 代表内部还是只有一个值 就不需要累加了 直接忽略
						break;
					}
				}
			}

			// 代表数据被处理完了
			if (REMAINING.decrementAndGet(this) == 0) {
				SlotPair<T> sp = current;
				CURRENT.lazySet(this, null);

				if (sp != null) {
					complete(sp.first);
				}
				else {
					actual.onComplete();
				}
			}
		}
	}

	/**
	 * 用于接收上游分流后的数据
	 * @param <T>
	 */
	static final class MergeReduceInner<T> implements InnerConsumer<T> {

		final MergeReduceMain<T> parent;

		final BiFunction<T, T, T> reducer;

		volatile Subscription s;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<MergeReduceInner, Subscription>
				S = AtomicReferenceFieldUpdater.newUpdater(
				MergeReduceInner.class,
				Subscription.class,
				"s");

		T value;

		boolean done;

		/**
		 * 子对象 负责接收上游分流后的数据
		 * @param parent
		 * @param reducer
		 */
		MergeReduceInner(MergeReduceMain<T> parent,
				BiFunction<T, T, T> reducer) {
			this.parent = parent;
			this.reducer = reducer;
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
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.ACTUAL) return parent;
			if (key == Attr.BUFFERED) return value != null ? 1 : 0;
			if (key == Attr.PREFETCH) return Integer.MAX_VALUE;

			return null;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.setOnce(S, this, s)) {
				s.request(Long.MAX_VALUE);
			}
		}

		/**
		 * 接收上游发送的数据  这里不断通过 reducer 累加数据
		 * @param t
		 */
		@Override
		public void onNext(T t) {
			if (done) {
				Operators.onNextDropped(t, currentContext());
				return;
			}
			T v = value;

			if (v == null) {
				value = t;
			}
			else {

				try {
					v = Objects.requireNonNull(reducer.apply(v, t), "The reducer returned a null value");
				}
				catch (Throwable ex) {
					onError(Operators.onOperatorError(s, ex, t, currentContext()));
					return;
				}

				value = v;
			}
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, parent.currentContext());
				return;
			}
			done = true;
			parent.innerError(t);
		}

		/**
		 * 代表某一分流处理完数据了
		 */
		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			// 将结果发送到 main
			parent.innerComplete(value);
		}

		void cancel() {
			Operators.terminate(S, this);
		}
	}

	static final class SlotPair<T> {

		T first;

		T second;

		volatile int acquireIndex;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<SlotPair> ACQ =
				AtomicIntegerFieldUpdater.newUpdater(SlotPair.class, "acquireIndex");

		volatile int releaseIndex;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<SlotPair> REL =
				AtomicIntegerFieldUpdater.newUpdater(SlotPair.class, "releaseIndex");

		int tryAcquireSlot() {
			for (; ; ) {
				int acquired = acquireIndex;
				if (acquired >= 2) {
					return -1;
				}

				if (ACQ.compareAndSet(this, acquired, acquired + 1)) {
					return acquired;
				}
			}
		}

		boolean releaseSlot() {
			return REL.incrementAndGet(this) == 2;
		}
	}
}
