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

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Consumer;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.scheduler.Scheduler;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Buffers values if the subscriber doesn't request fast enough, bounding the buffer to a
 * chosen size and applying a TTL (time-to-live) to the elements. If the buffer overflows,
 * drop the oldest element.
 *
 * @author Stephane Maldini
 * @author Simon Baslé
 * @author David Karnok
 */
//see https://github.com/akarnokd/RxJava2Extensions/blob/master/src/main/java/hu/akarnokd/rxjava2/operators/FlowableOnBackpressureTimeout.java
final class FluxOnBackpressureBufferTimeout<O> extends InternalFluxOperator<O, O> {

	private static final Logger LOGGER =
			Loggers.getLogger(FluxOnBackpressureBufferTimeout.class);

	final Duration            ttl;
	final Scheduler           ttlScheduler;
	final int                 bufferSize;
	/**
	 * 当出现背压超负荷时的处理
	 */
	final Consumer<? super O> onBufferEviction;

	FluxOnBackpressureBufferTimeout(Flux<? extends O> source,
			Duration ttl,
			Scheduler ttlScheduler,
			int bufferSize,
			Consumer<? super O> onBufferEviction) {
		super(source);
		this.ttl = ttl;
		this.ttlScheduler = ttlScheduler;
		this.bufferSize = bufferSize;
		this.onBufferEviction = onBufferEviction;
	}

	@Override
	public CoreSubscriber<? super O> subscribeOrReturn(CoreSubscriber<? super O> actual) {
		return new BackpressureBufferTimeoutSubscriber<>(actual,
				ttl,
				ttlScheduler,
				bufferSize,
				onBufferEviction);
	}

	@Override
	public int getPrefetch() {
		return Integer.MAX_VALUE;
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_ON) return ttlScheduler;

		return super.scanUnsafe(key);
	}

	/**
	 * 加工传入的订阅者
	 * @param <T>
	 */
	static final class BackpressureBufferTimeoutSubscriber<T> extends ArrayDeque<Object>
			implements InnerOperator<T, T>, Runnable {

		final CoreSubscriber<? super T> actual;
		final Context                   ctx;
		final Duration                  ttl;
		final Scheduler                 ttlScheduler;
		final Scheduler.Worker          worker;
		final int                       bufferSizeDouble;
		final Consumer<? super T>       onBufferEviction;

		Subscription s;

		volatile boolean cancelled;

		volatile boolean done;
		Throwable error;

		volatile int wip;
		static final AtomicIntegerFieldUpdater<BackpressureBufferTimeoutSubscriber> WIP =
				AtomicIntegerFieldUpdater.newUpdater(BackpressureBufferTimeoutSubscriber.class,
						"wip");

		volatile long requested;
		static final AtomicLongFieldUpdater<BackpressureBufferTimeoutSubscriber>
				REQUESTED = AtomicLongFieldUpdater.newUpdater(
				BackpressureBufferTimeoutSubscriber.class,
				"requested");

		BackpressureBufferTimeoutSubscriber(CoreSubscriber<? super T> actual,
				Duration ttl,
				Scheduler ttlScheduler,
				int bufferSize,
				Consumer<? super T> onBufferEviction) {
			this.actual = actual;
			this.ctx = actual.currentContext();
			this.onBufferEviction = Objects.requireNonNull(onBufferEviction,
					"buffer eviction callback must not be null");
			this.bufferSizeDouble = bufferSize << 1;
			this.ttl = ttl;
			this.ttlScheduler = Objects.requireNonNull(ttlScheduler,
					"ttl Scheduler must not be null");
			// 获取到某个线程池
			this.worker = ttlScheduler.createWorker();
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) {
				return s;
			}
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) {
				return requested;
			}
			if (key == Attr.TERMINATED) {
				return done && isEmpty();
			}
			if (key == Attr.CANCELLED) {
				return cancelled;
			}
			if (key == Attr.BUFFERED) {
				return size();
			}
			if (key == Attr.ERROR) {
				return error;
			}
			if (key == Attr.PREFETCH) {
				return Integer.MAX_VALUE;
			}
			if (key == Attr.DELAY_ERROR) {
				return false;
			}
			if (key == Attr.RUN_ON) return ttlScheduler;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		/**
		 * 当下游对象向本对象拉取数据时触发
		 * @param n
		 */
		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Operators.addCap(REQUESTED, this, n);
				drain();
			}
		}

		@Override
		public void cancel() {
			cancelled = true;
			s.cancel();
			worker.dispose();

			if (WIP.getAndIncrement(this) == 0) {
				clearQueue();
			}
		}

		/**
		 * 清空队列中的元素
		 */
		@SuppressWarnings("unchecked")
		void clearQueue() {
			for (; ; ) {
				T evicted;
				synchronized (this) {
					if (this.isEmpty()) {
						break;
					}

					// 每次都会弹出2个元素 也就是 插入应该也是2个元素
					this.poll();
					evicted = (T) this.poll();
				}

				evict(evicted);
			}
		}

		/**
		 * 首先触发该方法 将上游数据发送到该对象
		 * @param s
		 */
		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				// 让下游对象向本对象拉取数据
				actual.onSubscribe(this);

				// 拉取所有上游数据
				s.request(Long.MAX_VALUE);
			}
		}

		/**
		 * 接收上游下发的数据
		 * @param t
		 */
		@SuppressWarnings("unchecked")
		@Override
		public void onNext(T t) {
			T evicted = null;
			synchronized (this) {
				// 因为每次往队列中都是一次插入2个元素  所以这里用 双倍长度来评估 超过的情况会先弹出最前面的数据
				if (this.size() == bufferSizeDouble) {
					this.poll();
					evicted = (T) this.poll();
				}
				this.offer(ttlScheduler.now(TimeUnit.MILLISECONDS));
				this.offer(t);
			}
			evict(evicted);
			try {
				// 定时执行任务
				worker.schedule(this, ttl.toMillis(), TimeUnit.MILLISECONDS);
			}
			catch (RejectedExecutionException re) {
				done = true;
				error = Operators.onRejectedExecution(re, this, null, t,
						actual.currentContext());
			}
			drain();
		}

		@Override
		public void onError(Throwable t) {
			error = t;
			done = true;
			drain();
		}

		@Override
		public void onComplete() {
			done = true;
			drain();
		}

		/**
		 * 当上游的数据传播到 下游时 在一定延时后会触发该方法
		 */
		@SuppressWarnings("unchecked")
		@Override
		public void run() {
			for (; ; ) {
				if (cancelled) {
					break;
				}

				boolean d = done;
				boolean empty;
				T evicted = null;

				synchronized (this) {
					// 查看最上面一个元素 不弹出
					Long ts = (Long) this.peek();
					empty = ts == null;
					if (!empty) {
						// 定期清理过时的元素  正常情况下 是不需要这个任务的 看来下游又需要切线程了
						if (ts <= ttlScheduler.now(TimeUnit.MILLISECONDS) - ttl.toMillis()) {
							this.poll();
							evicted = (T) this.poll();
						}
						else {
							break;
						}
					}
				}

				evict(evicted);

				if (empty) {
					if (d) {
						drain();
					}
					break;
				}
			}
		}

		/**
		 * 驱逐掉某个元素
		 * @param evicted
		 */
		void evict(@Nullable T evicted) {
			if (evicted != null) {
				try {
					onBufferEviction.accept(evicted);
				}
				catch (Throwable ex) {
					if (LOGGER.isDebugEnabled()) {
						LOGGER.debug(
								"value [{}] couldn't be evicted due to a callback error. This error will be dropped: {}",
								evicted,
								ex);
					}
					Operators.onErrorDropped(ex, actual.currentContext());
				}
				Operators.onDiscard(evicted, actual.currentContext());
			}
		}

		/**
		 * 将上游数据通过中转器 转发到下游
		 */
		@SuppressWarnings("unchecked")
		void drain() {
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}

			int missed = 1;

			for (; ; ) {
				long r = requested;
				long e = 0;

				while (e != r) {
					if (cancelled) {
						// 如果本对象已经被关闭了 清空队列  在evict 时 会触发消费者钩子
						clearQueue();
						return;
					}

					boolean d = done;
					T v;

					synchronized (this) {
						if (this.poll() != null) {
							v = (T) this.poll();
						}
						else {
							v = null;
						}
					}

					boolean empty = v == null;

					// 代表已经没有数据了 触发 onComplete
					if (d && empty) {
						Throwable ex = error;
						if (ex != null) {
							actual.onError(ex);
						}
						else {
							actual.onComplete();
						}

						worker.dispose();
						return;
					}

					if (empty) {
						break;
					}

					// 将数据传播到下游
					actual.onNext(v);

					e++;
				}

				if (e == r) {
					if (cancelled) {
						clearQueue();
						return;
					}

					boolean d = done;
					boolean empty;
					synchronized (this) {
						empty = this.isEmpty();
					}

					if (d && empty) {
						Throwable ex = error;
						if (ex != null) {
							actual.onError(ex);
						}
						else {
							actual.onComplete();
						}

						worker.dispose();
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
	}
}
