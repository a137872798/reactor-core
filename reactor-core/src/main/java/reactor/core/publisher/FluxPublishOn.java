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
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Supplier;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Scheduler.Worker;
import reactor.util.annotation.Nullable;

/**
 * Emits events on a different thread specified by a scheduler callback.
 *
 * @param <T> the value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 * 该对象向下游发射数据的动作是异步的  或者说是并发的 这样下游的onNext 方法就有可能是并发执行
 */
final class FluxPublishOn<T> extends InternalFluxOperator<T, T> implements Fuseable {

	/**
	 * 调度器
	 * 存在一个 ImmediateScheduler 代表直接在本线程执行任务
	 */
	final Scheduler scheduler;

	final boolean delayError;

	final Supplier<? extends Queue<T>> queueSupplier;

	final int prefetch;

	/**
	 * 低潮???
	 */
	final int lowTide;

	FluxPublishOn(Flux<? extends T> source,
			Scheduler scheduler,
			boolean delayError,
			int prefetch,
			int lowTide,
			Supplier<? extends Queue<T>> queueSupplier) {
		super(source);
		if (prefetch <= 0) {
			throw new IllegalArgumentException("prefetch > 0 required but it was " + prefetch);
		}
		this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
		this.delayError = delayError;
		this.prefetch = prefetch;
		this.lowTide = lowTide;
		this.queueSupplier = Objects.requireNonNull(queueSupplier, "queueSupplier");
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_ON) return scheduler;

		return super.scanUnsafe(key);
	}

	@Override
	public int getPrefetch() {
		return prefetch;
	}

	/**
	 * 加工订阅者对象
	 * @param actual
	 * @return
	 */
	@Override
	@SuppressWarnings("unchecked")
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		Worker worker;

		try {
			worker = Objects.requireNonNull(scheduler.createWorker(),
					"The scheduler returned a null worker");
		}
		catch (Throwable e) {
			Operators.error(actual, Operators.onOperatorError(e, actual.currentContext()));
			return null;
		}

		if (actual instanceof ConditionalSubscriber) {
			ConditionalSubscriber<? super T> cs = (ConditionalSubscriber<? super T>) actual;
			source.subscribe(new PublishOnConditionalSubscriber<>(cs,
					scheduler,
					worker,
					delayError,
					prefetch,
					lowTide,
					queueSupplier));
			return null;
		}
		// 这里使用 worker 来执行下发的任务
		return new PublishOnSubscriber<>(actual,
				scheduler,
				worker,
				delayError,
				prefetch,
				lowTide,
				queueSupplier);
	}

	/**
	 * 该对象会使用调用器 来下发数据
	 * @param <T>
	 */
	static final class PublishOnSubscriber<T>
			implements QueueSubscription<T>, Runnable, InnerOperator<T, T> {

		final CoreSubscriber<? super T> actual;

		final Scheduler scheduler;

		/**
		 * worker 相比 scheduler 更轻量级 scheduler 内部为了提供并发度 可能会设置多个线程池
		 * 而 worker 对应执行任务的一个线程池
		 */
		final Worker worker;

		final boolean delayError;

		final int prefetch;

		final int limit;

		final Supplier<? extends Queue<T>> queueSupplier;

		Subscription s;

		Queue<T> queue;

		volatile boolean cancelled;

		volatile boolean done;

		Throwable error;

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<PublishOnSubscriber> WIP =
				AtomicIntegerFieldUpdater.newUpdater(PublishOnSubscriber.class, "wip");

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<PublishOnSubscriber> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(PublishOnSubscriber.class, "requested");

		int sourceMode;

		long produced;

		boolean outputFused;

		PublishOnSubscriber(CoreSubscriber<? super T> actual,
				Scheduler scheduler,
				Worker worker,
				boolean delayError,
				int prefetch,
				int lowTide,   //  限制上游数据发射的速度的2个指标  这个是下限
				Supplier<? extends Queue<T>> queueSupplier) {
			this.actual = actual;
			this.worker = worker;
			this.scheduler = scheduler;
			this.delayError = delayError;
			this.prefetch = prefetch;
			this.queueSupplier = queueSupplier;
			this.limit = Operators.unboundedOrLimit(prefetch, lowTide);
		}

		/**
		 * 将上游数据 与该对象结合生成 subscription 并下发
		 * @param s
		 */
		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				if (s instanceof QueueSubscription) {
					@SuppressWarnings("unchecked") QueueSubscription<T> f =
							(QueueSubscription<T>) s;

					int m = f.requestFusion(Fuseable.ANY | Fuseable.THREAD_BARRIER);

					if (m == Fuseable.SYNC) {
						sourceMode = Fuseable.SYNC;
						queue = f;
						done = true;

						actual.onSubscribe(this);
						return;
					}
					if (m == Fuseable.ASYNC) {
						sourceMode = Fuseable.ASYNC;
						queue = f;

						actual.onSubscribe(this);

						s.request(Operators.unboundedOrPrefetch(prefetch));

						return;
					}
				}

				queue = queueSupplier.get();

				// 触发本对象的 request
				actual.onSubscribe(this);

				// 拉取上游数据  请求的数量实际上比 limit 大一点
				s.request(Operators.unboundedOrPrefetch(prefetch));
			}
		}

		// 当上游的数据下发
		@Override
		public void onNext(T t) {
			if (sourceMode == ASYNC) {
				trySchedule(this, null, null /* t always null */);
				return;
			}

			if (done) {
				Operators.onNextDropped(t, actual.currentContext());
				return;
			}
			if (!queue.offer(t)) {
				error = Operators.onOperatorError(s,
						Exceptions.failWithOverflow(Exceptions.BACKPRESSURE_ERROR_QUEUE_FULL),
						t, actual.currentContext());
				done = true;
			}
			// 接收到的数据没有直接发送到下游 而是尝试使用 worker 进行发送
			trySchedule(this, null, t);
		}

		/**
		 * 当出现异常时 往队列中插入一个 null 同时在 异步任务中 如果从队列中拉取到了null 会直接触发下游的 onComplete
		 * 因为 在上游线程没有与下游线程做协调的时候 不能直接停止下游的任务 这里null 就作为一个特殊标识
		 * @param t
		 */
		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, actual.currentContext());
				return;
			}
			error = t;
			done = true;
			trySchedule(null, t, null);
		}

		/**
		 * 思路同上
		 */
		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			//WIP also guards, no competing onNext
			trySchedule(null, null, null);
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Operators.addCap(REQUESTED, this, n);
				//WIP also guards during request and onError is possible
				trySchedule(this, null, null);
			}
		}

		/**
		 * 这里会设置 cancelled 标识
		 */
		@Override
		public void cancel() {
			if (cancelled) {
				return;
			}

			cancelled = true;
			s.cancel();
			worker.dispose();

			if (WIP.getAndIncrement(this) == 0) {
				Operators.onDiscardQueueWithClear(queue, actual.currentContext(), null);
			}
		}

		/**
		 * 尝试异步将某个数据下发
		 * @param subscription
		 * @param suppressed
		 * @param dataSignal
		 */
		void trySchedule(
				@Nullable Subscription subscription, // 本对象
				@Nullable Throwable suppressed,
				@Nullable Object dataSignal  // 下发的数据
		) {
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}

			// 通过调度器 执行异步任务  不过 immediateScheduler 返回的对象 只会在当前线程执行任务
			try {
				worker.schedule(this);
			}
			catch (RejectedExecutionException ree) {
				Operators.onDiscardQueueWithClear(queue, actual.currentContext(), null);
				actual.onError(Operators.onRejectedExecution(ree, subscription, suppressed, dataSignal,
						actual.currentContext()));
			}
		}

		/**
		 * 同步模式 拉取数据到下游
		 */
		void runSync() {
			int missed = 1;

			final Subscriber<? super T> a = actual;
			final Queue<T> q = queue;

			long e = produced;

			// 这里就是个 loop 直到 上游跟不上下游的消费速度 或者 上游数据发送完
			for (; ; ) {

				long r = requested;

				while (e != r) {
					T v;

					try {
						v = q.poll();
					}
					catch (Throwable ex) {
						doError(a, Operators.onOperatorError(s, ex,
								actual.currentContext()));
						return;
					}

					// 关闭的话 连 onComplete 也不会触发
					if (cancelled) {
						Operators.onDiscardQueueWithClear(q, actual.currentContext(), null);
						return;
					}
					if (v == null) {
						doComplete(a);
						return;
					}

					a.onNext(v);

					e++;
				}

				if (cancelled) {
					Operators.onDiscardQueueWithClear(q, actual.currentContext(), null);
					return;
				}

				if (q.isEmpty()) {
					doComplete(a);
					return;
				}

				int w = wip;
				if (missed == w) {
					produced = e;
					missed = WIP.addAndGet(this, -missed);
					if (missed == 0) {
						break;
					}
				}
				else {
					missed = w;
				}
			}
		}

		/**
		 * 异步执行该任务
		 */
		void runAsync() {
			int missed = 1;

			final Subscriber<? super T> a = actual;
			final Queue<T> q = queue;

			long e = produced;

			for (; ; ) {

				long r = requested;

				while (e != r) {
					boolean d = done;
					T v;

					try {
						v = q.poll();
					}
					catch (Throwable ex) {
						Exceptions.throwIfFatal(ex);
						s.cancel();
						Operators.onDiscardQueueWithClear(q, actual.currentContext(), null);

						doError(a, Operators.onOperatorError(ex, actual.currentContext()));
						return;
					}

					boolean empty = v == null;

					if (checkTerminated(d, empty, a)) {
						return;
					}

					// 拉取不到数据 跳过本次loop
					if (empty) {
						break;
					}

					// 正常拉取到数据的情况 触发下游的 onNext()
					a.onNext(v);

					e++;
					if (e == limit) {
						if (r != Long.MAX_VALUE) {
							r = REQUESTED.addAndGet(this, -e);
						}
						s.request(e);
						e = 0L;
					}
				}

				if (e == r && checkTerminated(done, q.isEmpty(), a)) {
					return;
				}

				int w = wip;
				if (missed == w) {
					produced = e;
					missed = WIP.addAndGet(this, -missed);
					if (missed == 0) {
						break;
					}
				}
				else {
					missed = w;
				}
			}
		}

		void runBackfused() {
			int missed = 1;

			for (; ; ) {

				if (cancelled) {
					Operators.onDiscardQueueWithClear(queue, actual.currentContext(), null);

					return;
				}

				boolean d = done;

				actual.onNext(null);

				if (d) {
					Throwable e = error;
					if (e != null) {
						doError(actual, e);
					}
					else {
						doComplete(actual);
					}
					return;
				}

				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}

		void doComplete(Subscriber<?> a) {
			a.onComplete();
			worker.dispose();
		}

		void doError(Subscriber<?> a, Throwable e) {
			try {
				a.onError(e);
			}
			finally {
				worker.dispose();
			}
		}

		@Override
		public void run() {
			if (outputFused) {
				runBackfused();
			}
			else if (sourceMode == Fuseable.SYNC) {
				runSync();
			}
			else {
				// 默认情况走这里
				runAsync();
			}
		}

		boolean checkTerminated(boolean d, boolean empty, Subscriber<?> a) {
			if (cancelled) {
				Operators.onDiscardQueueWithClear(queue, actual.currentContext(), null);
				return true;
			}
			if (d) {
				if (delayError) {
					if (empty) {
						Throwable e = error;
						if (e != null) {
							doError(a, e);
						}
						else {
							doComplete(a);
						}
						return true;
					}
				}
				else {
					Throwable e = error;
					if (e != null) {
						Operators.onDiscardQueueWithClear(queue, actual.currentContext(), null);
						doError(a, e);
						return true;
					}
					else if (empty) {
						doComplete(a);
						return true;
					}
				}
			}

			return false;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM ) return requested;
			if (key == Attr.PARENT ) return s;
			if (key == Attr.CANCELLED) return cancelled;
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.BUFFERED) return queue != null ? queue.size() : 0;
			if (key == Attr.ERROR) return error;
			if (key == Attr.DELAY_ERROR) return delayError;
			if (key == Attr.PREFETCH) return prefetch;
			if (key == Attr.RUN_ON) return worker;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		public void clear() {
			Operators.onDiscardQueueWithClear(queue, actual.currentContext(), null);
		}

		@Override
		public boolean isEmpty() {
			return queue.isEmpty();
		}

		@Override
		@Nullable
		public T poll() {
			T v = queue.poll();
			if (v != null && sourceMode != SYNC) {
				long p = produced + 1;
				if (p == limit) {
					produced = 0;
					s.request(p);
				}
				else {
					produced = p;
				}
			}
			return v;
		}

		@Override
		public int requestFusion(int requestedMode) {
			if ((requestedMode & ASYNC) != 0) {
				outputFused = true;
				return ASYNC;
			}
			return NONE;
		}

		@Override
		public int size() {
			return queue.size();
		}
	}

	static final class PublishOnConditionalSubscriber<T>
			implements QueueSubscription<T>, Runnable, InnerOperator<T, T> {

		final ConditionalSubscriber<? super T> actual;

		final Worker worker;

		final Scheduler scheduler;

		final boolean delayError;

		final int prefetch;

		final int limit;

		final Supplier<? extends Queue<T>> queueSupplier;

		Subscription s;

		Queue<T> queue;

		volatile boolean cancelled;

		volatile boolean done;

		Throwable error;

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<PublishOnConditionalSubscriber> WIP =
				AtomicIntegerFieldUpdater.newUpdater(PublishOnConditionalSubscriber.class,
						"wip");

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<PublishOnConditionalSubscriber> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(PublishOnConditionalSubscriber.class,
						"requested");

		int sourceMode;

		long produced;

		long consumed;

		boolean outputFused;

		PublishOnConditionalSubscriber(ConditionalSubscriber<? super T> actual,
				Scheduler scheduler,
				Worker worker,
				boolean delayError,
				int prefetch,
				int lowTide,
				Supplier<? extends Queue<T>> queueSupplier) {
			this.actual = actual;
			this.worker = worker;
			this.scheduler = scheduler;
			this.delayError = delayError;
			this.prefetch = prefetch;
			this.queueSupplier = queueSupplier;
			this.limit = Operators.unboundedOrLimit(prefetch, lowTide);
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				if (s instanceof QueueSubscription) {
					@SuppressWarnings("unchecked") QueueSubscription<T> f =
							(QueueSubscription<T>) s;

					int m = f.requestFusion(Fuseable.ANY | Fuseable.THREAD_BARRIER);

					if (m == Fuseable.SYNC) {
						sourceMode = Fuseable.SYNC;
						queue = f;
						done = true;

						actual.onSubscribe(this);
						return;
					}
					if (m == Fuseable.ASYNC) {
						sourceMode = Fuseable.ASYNC;
						queue = f;

						actual.onSubscribe(this);

						s.request(Operators.unboundedOrPrefetch(prefetch));

						return;
					}
				}

				queue = queueSupplier.get();

				actual.onSubscribe(this);

				s.request(Operators.unboundedOrPrefetch(prefetch));
			}
		}

		@Override
		public void onNext(T t) {
			if (sourceMode == ASYNC) {
				trySchedule(this, null, null);
				return;
			}

			if (done) {
				Operators.onNextDropped(t, actual.currentContext());
				return;
			}
			if (!queue.offer(t)) {
				error = Operators.onOperatorError(s, Exceptions.failWithOverflow(Exceptions.BACKPRESSURE_ERROR_QUEUE_FULL), t,
						actual.currentContext());
				done = true;
			}
			trySchedule(this, null, t);
		}

		@Override
		public void onError(Throwable t) {
			if(done){
				Operators.onErrorDropped(t, actual.currentContext());
				return;
			}
			error = t;
			done = true;
			trySchedule(null, t, null);
		}

		@Override
		public void onComplete() {
			if(done){
				return;
			}
			done = true;
			trySchedule(null, null, null);
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Operators.addCap(REQUESTED, this, n);
				trySchedule(this, null, null);
			}
		}

		@Override
		public void cancel() {
			if (cancelled) {
				return;
			}

			cancelled = true;
			s.cancel();
			worker.dispose();

			if (WIP.getAndIncrement(this) == 0) {
				Operators.onDiscardQueueWithClear(queue, actual.currentContext(), null);
			}
		}

		void trySchedule(
				@Nullable Subscription subscription,
				@Nullable Throwable suppressed,
				@Nullable Object dataSignal) {
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}

			try {
				worker.schedule(this);
			}
			catch (RejectedExecutionException ree) {
				Operators.onDiscardQueueWithClear(queue, actual.currentContext(), null);
				actual.onError(Operators.onRejectedExecution(ree, subscription, suppressed, dataSignal,
						actual.currentContext()));
			}
		}

		/**
		 * 在同步模式下 下发数据
		 */
		void runSync() {
			int missed = 1;

			final ConditionalSubscriber<? super T> a = actual;
			// onNext 的数据都存放在 queue 中
			final Queue<T> q = queue;

			long e = produced;

			for (; ; ) {

				long r = requested;

				while (e != r) {
					T v;
					try {
						v = q.poll();
					}
					catch (Throwable ex) {
						doError(a, Operators.onOperatorError(s, ex,
								actual.currentContext()));
						return;
					}

					if (cancelled) {
						Operators.onDiscardQueueWithClear(q, actual.currentContext(), null);
						return;
					}
					// 因为 一次onNext 对应一次runSync 如果 queue 没有拉到数据 就是上游数据处理完了
					if (v == null) {
						doComplete(a);
						return;
					}

					// 将数据发射到下游
					if (a.tryOnNext(v)) {
						e++;
					}
				}

				if (cancelled) {
					Operators.onDiscardQueueWithClear(q, actual.currentContext(), null);
					return;
				}

				if (q.isEmpty()) {
					doComplete(a);
					return;
				}

				int w = wip;
				if (missed == w) {
					produced = e;
					missed = WIP.addAndGet(this, -missed);
					if (missed == 0) {
						break;
					}
				}
				else {
					missed = w;
				}
			}
		}

		/**
		 * 代表在异步环境下发射数据
		 */
		void runAsync() {
			int missed = 1;

			final ConditionalSubscriber<? super T> a = actual;
			final Queue<T> q = queue;

			long emitted = produced;
			long polled = consumed;

			for (; ; ) {

				long r = requested;

				while (emitted != r) {
					boolean d = done;
					T v;
					try {
						v = q.poll();
					}
					catch (Throwable ex) {
						Exceptions.throwIfFatal(ex);
						s.cancel();
						Operators.onDiscardQueueWithClear(q, actual.currentContext(), null);

						doError(a, Operators.onOperatorError(ex, actual.currentContext()));
						return;
					}
					boolean empty = v == null;

					if (checkTerminated(d, empty, a)) {
						return;
					}

					if (empty) {
						break;
					}

					if (a.tryOnNext(v)) {
						emitted++;
					}

					polled++;

					// 好像唯一的区别就是 它会自动拉取下一批数据
					if (polled == limit) {
						s.request(polled);
						polled = 0L;
					}
				}

				if (emitted == r && checkTerminated(done, q.isEmpty(), a)) {
					return;
				}

				int w = wip;
				if (missed == w) {
					produced = emitted;
					consumed = polled;
					missed = WIP.addAndGet(this, -missed);
					if (missed == 0) {
						break;
					}
				}
				else {
					missed = w;
				}
			}

		}

		void runBackfused() {
			int missed = 1;

			for (; ; ) {

				if (cancelled) {
					Operators.onDiscardQueueWithClear(queue, actual.currentContext(), null);
					return;
				}

				boolean d = done;

				actual.onNext(null);

				if (d) {
					Throwable e = error;
					if (e != null) {
						doError(actual, e);
					}
					else {
						doComplete(actual);
					}
					return;
				}

				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}

		/**
		 * 当上游发射数据 并被接收到时 可以在这层做异步解耦
		 */
		@Override
		public void run() {
			if (outputFused) {
				runBackfused();
			}
			// 如果是在同步模式下
			else if (sourceMode == Fuseable.SYNC) {
				runSync();
			}
			// 在异步模式下
			else {
				runAsync();
			}
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return requested;
			if (key == Attr.PARENT) return s;
			if (key == Attr.CANCELLED) return cancelled;
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.BUFFERED) return queue != null ? queue.size() : 0;
			if (key == Attr.ERROR) return error;
			if (key == Attr.DELAY_ERROR) return delayError;
			if (key == Attr.PREFETCH) return prefetch;
			if (key == Attr.RUN_ON) return worker;

			return InnerOperator.super.scanUnsafe(key);
		}

		/**
		 * 丢弃 worker 对象
		 * @param a
		 */
		void doComplete(Subscriber<?> a) {
			a.onComplete();
			worker.dispose();
		}

		void doError(Subscriber<?> a, Throwable e) {
			try {
				a.onError(e);
			}
			finally {
				worker.dispose();
			}
		}

		boolean checkTerminated(boolean d, boolean empty, Subscriber<?> a) {
			if (cancelled) {
				Operators.onDiscardQueueWithClear(queue, actual.currentContext(), null);
				return true;
			}
			if (d) {
				if (delayError) {
					if (empty) {
						Throwable e = error;
						if (e != null) {
							doError(a, e);
						}
						else {
							doComplete(a);
						}
						return true;
					}
				}
				else {
					Throwable e = error;
					if (e != null) {
						Operators.onDiscardQueueWithClear(queue, actual.currentContext(), null);
						doError(a, e);
						return true;
					}
					else if (empty) {
						doComplete(a);
						return true;
					}
				}
			}

			return false;
		}

		@Override
		public void clear() {
			Operators.onDiscardQueueWithClear(queue, actual.currentContext(), null);
		}

		@Override
		public boolean isEmpty() {
			return queue.isEmpty();
		}

		@Override
		@Nullable
		public T poll() {
			T v = queue.poll();
			if (v != null && sourceMode != SYNC) {
				long p = consumed + 1;
				if (p == limit) {
					consumed = 0;
					s.request(p);
				}
				else {
					consumed = p;
				}
			}
			return v;
		}

		@Override
		public int requestFusion(int requestedMode) {
			if ((requestedMode & ASYNC) != 0) {
				outputFused = true;
				return ASYNC;
			}
			return NONE;
		}

		@Override
		public int size() {
			return queue.size();
		}
	}
}
