/*
 * Copyright (c) 2011-Present Pivotal Software Inc, All Rights Reserved.
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

import org.reactivestreams.Subscriber;

import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.publisher.FluxSink.OverflowStrategy;
import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;
import reactor.util.context.Context;

/**
 * Provides a multi-valued sink API for a callback that is called for each individual
 * Subscriber.
 *
 * @param <T> the value type
 */
final class FluxCreate<T> extends Flux<T> implements SourceProducer<T> {

	/**
	 * 创建模式 包含 仅仅允许push 和 push/pull
	 */
	enum CreateMode {
		PUSH_ONLY, PUSH_PULL
	}

	/**
	 * 该flux 是基于 consumer 来创建的
	 */
	final Consumer<? super FluxSink<T>> source;

	final OverflowStrategy backpressure;

	final CreateMode createMode;

	FluxCreate(Consumer<? super FluxSink<T>> source,
			FluxSink.OverflowStrategy backpressure,
			CreateMode createMode) {
		this.source = Objects.requireNonNull(source, "source");
		this.backpressure = Objects.requireNonNull(backpressure, "backpressure");
		this.createMode = createMode;
	}

	/**
	 * 根据 背压策略和 订阅者对象 来创建 sink
	 * @param t
	 * @param backpressure
	 * @param <T>
	 * @return
	 */
	static <T> BaseSink<T> createSink(CoreSubscriber<? super T> t,
			OverflowStrategy backpressure) {
		switch (backpressure) {
			case IGNORE: {
				// 忽略背压
				return new IgnoreSink<>(t);
			}
			case ERROR: {
				// 提示异常
				return new ErrorAsyncSink<>(t);
			}
			case DROP: {
				return new DropAsyncSink<>(t);
			}
			case LATEST: {
				return new LatestAsyncSink<>(t);
			}
			default: {
				return new BufferAsyncSink<>(t, Queues.SMALL_BUFFER_SIZE);
			}
		}
	}

	/**
	 * 为该对象设置订阅者
	 * @param actual the {@link Subscriber} interested into the published sequence
	 */
	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		// 创建一个 sink 对象
		BaseSink<T> sink = createSink(actual, backpressure);

		// 该方法用于设置请求数  (也就是调用 request)
		actual.onSubscribe(sink);
		try {
			source.accept(
					// SerializedSink 该对象内部可以存储数据 (通过一个 mpsc 队列)
					createMode == CreateMode.PUSH_PULL ? new SerializedSink<>(sink) :
							sink);
		}
		catch (Throwable ex) {
			Exceptions.throwIfFatal(ex);
			sink.error(Operators.onOperatorError(ex, actual.currentContext()));
		}
	}

	@Override
	public Object scanUnsafe(Attr key) {
		return null; //no particular key to be represented, still useful in hooks
	}

	/**
	 * Serializes calls to onNext, onError and onComplete.
	 *
	 * @param <T> the value type
	 *           串行化执行
	 */
	static final class SerializedSink<T> implements FluxSink<T>, Scannable {

		/**
		 * 该对象包装了 sink
		 */
		final BaseSink<T> sink;

		/**
		 * 记录处理过程中出现的异常
		 */
		volatile Throwable error;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<SerializedSink, Throwable> ERROR =
				AtomicReferenceFieldUpdater.newUpdater(SerializedSink.class,
						Throwable.class,
						"error");

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<SerializedSink> WIP =
				AtomicIntegerFieldUpdater.newUpdater(SerializedSink.class, "wip");

		final Queue<T> mpscQueue;

		volatile boolean done;

		SerializedSink(BaseSink<T> sink) {
			this.sink = sink;
			// 这里指定生成了 mpsc 队列
			this.mpscQueue = Queues.<T>unboundedMultiproducer().get();
		}

		@Override
		public Context currentContext() {
			return sink.currentContext();
		}

		/**
		 * 处理上游传过来的数据
		 * @param t the value to emit, not null
		 * @return
		 */
		@Override
		public FluxSink<T> next(T t) {
			Objects.requireNonNull(t, "t is null in sink.next(t)");
			if (sink.isTerminated() || done) {
				Operators.onNextDropped(t, sink.currentContext());
				return this;
			}
			if (WIP.get(this) == 0 && WIP.compareAndSet(this, 0, 1)) {
				try {
					// 代理给内部真正的sink 来处理
					sink.next(t);
				}
				catch (Throwable ex) {
					Operators.onOperatorError(sink, ex, t, sink.currentContext());
				}
				if (WIP.decrementAndGet(this) == 0) {
					return this;
				}
			}
			else {
				// 数据先存放在 队列中 之后再处理
				this.mpscQueue.offer(t);
				if (WIP.getAndIncrement(this) != 0) {
					return this;
				}
			}
			// 循环并尝试消费数据
			drainLoop();
			return this;
		}

		@Override
		public void error(Throwable t) {
			Objects.requireNonNull(t, "t is null in sink.error(t)");
			if (sink.isTerminated() || done) {
				Operators.onOperatorError(t, sink.currentContext());
				return;
			}
			if (Exceptions.addThrowable(ERROR, this, t)) {
				done = true;
				drain();
			}
			else {
				Context ctx = sink.currentContext();
				Operators.onDiscardQueueWithClear(mpscQueue, ctx, null);
				Operators.onOperatorError(t, ctx);
			}
		}

		@Override
		public void complete() {
			if (sink.isTerminated() || done) {
				return;
			}
			done = true;
			drain();
		}

		//impl note: don't use sink.isTerminated() in the drain loop,
		//it needs to separately check its own `done` status before calling the base sink
		//complete()/error() methods (which do flip the isTerminated), otherwise it could
		//bypass the terminate handler (in buffer and latest variants notably).
		void drain() {
			if (WIP.getAndIncrement(this) == 0) {
				drainLoop();
			}
		}

		/**
		 * 循环消费数据
		 */
		void drainLoop() {
			Context ctx = sink.currentContext();
			BaseSink<T> e = sink;
			Queue<T> q = mpscQueue;
			for (; ; ) {

				for (; ; ) {
					if (e.isCancelled()) {
						Operators.onDiscardQueueWithClear(q, ctx, null);
						if (WIP.decrementAndGet(this) == 0) {
							return;
						}
						else {
							continue;
						}
					}

					if (ERROR.get(this) != null) {
						Operators.onDiscardQueueWithClear(q, ctx, null);
						//noinspection ConstantConditions
						e.error(Exceptions.terminate(ERROR, this));
						return;
					}

					boolean d = done;
					T v = q.poll();

					boolean empty = v == null;

					if (d && empty) {
						e.complete();
						return;
					}

					if (empty) {
						break;
					}

					try {
						// 委托给真正的 sink 来处理请求
						e.next(v);
					}
					catch (Throwable ex) {
						Operators.onOperatorError(sink, ex, v, sink.currentContext());
					}
				}

				if (WIP.decrementAndGet(this) == 0) {
					break;
				}
			}
		}

		@Override
		public FluxSink<T> onRequest(LongConsumer consumer) {
			sink.onRequest(consumer, consumer, sink.requested);
			return this;
		}

		@Override
		public FluxSink<T> onCancel(Disposable d) {
			sink.onCancel(d);
			return this;
		}

		@Override
		public FluxSink<T> onDispose(Disposable d) {
			sink.onDispose(d);
			return this;
		}

		@Override
		public long requestedFromDownstream() {
			return sink.requestedFromDownstream();
		}

		@Override
		public boolean isCancelled() {
			return sink.isCancelled();
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.BUFFERED) {
				return mpscQueue.size();
			}
			if (key == Attr.ERROR) {
				return error;
			}
			if (key == Attr.TERMINATED) {
				return done;
			}

			return sink.scanUnsafe(key);
		}

		@Override
		public String toString() {
			return sink.toString();
		}
	}

	/**
	 * Serializes calls to onNext, onError and onComplete if onRequest is invoked.
	 * Otherwise, non-serialized base sink is used.
	 *
	 * @param <T> the value type
	 */
	static class SerializeOnRequestSink<T> implements FluxSink<T>, Scannable {

		final BaseSink<T> baseSink;
		SerializedSink<T> serializedSink;
		FluxSink<T>       sink;

		SerializeOnRequestSink(BaseSink<T> sink) {
			this.baseSink = sink;
			this.sink = sink;
		}

		@Override
		public Context currentContext() {
			return sink.currentContext();
		}

		@Override
		public Object scanUnsafe(Attr key) {
			return serializedSink != null ? serializedSink.scanUnsafe(key) :
					baseSink.scanUnsafe(key);
		}

		@Override
		public void complete() {
			sink.complete();
		}

		@Override
		public void error(Throwable e) {
			sink.error(e);
		}

		@Override
		public FluxSink<T> next(T t) {
			sink.next(t);
			return serializedSink == null ? this : serializedSink;
		}

		@Override
		public long requestedFromDownstream() {
			return sink.requestedFromDownstream();
		}

		@Override
		public boolean isCancelled() {
			return sink.isCancelled();
		}

		@Override
		public FluxSink<T> onRequest(LongConsumer consumer) {
			if (serializedSink == null) {
				serializedSink = new SerializedSink<>(baseSink);
				sink = serializedSink;
			}
			return sink.onRequest(consumer);
		}

		@Override
		public FluxSink<T> onCancel(Disposable d) {
			sink.onCancel(d);
			return sink;
		}

		@Override
		public FluxSink<T> onDispose(Disposable d) {
			sink.onDispose(d);
			return this;
		}

		@Override
		public String toString() {
			return baseSink.toString();
		}
	}


	/**
	 * 下沉对象基类
	 * @param <T>
	 */
	static abstract class BaseSink<T> extends AtomicBoolean
			implements FluxSink<T>, InnerProducer<T> {

		/**
		 * 常量 代表总是被丢弃
		 */
		static final Disposable TERMINATED = OperatorDisposables.DISPOSED;
		/**
		 * 同上
		 */
		static final Disposable CANCELLED  = Disposables.disposed();

		/**
		 * 该对象的订阅者
		 */
		final CoreSubscriber<? super T> actual;
		/**
		 * 调用过程中用于保存参数的上下文对象
		 */
		final Context                   ctx;

		/**
		 * 当该对象被关闭 或者处理完资源后的清理对象
		 */
		volatile Disposable disposable;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<BaseSink, Disposable> DISPOSABLE =
				AtomicReferenceFieldUpdater.newUpdater(BaseSink.class,
						Disposable.class,
						"disposable");

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<BaseSink> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(BaseSink.class, "requested");

		/**
		 * 用于处理请求的  消费者对象
		 */
		volatile LongConsumer requestConsumer;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<BaseSink, LongConsumer>
				REQUEST_CONSUMER = AtomicReferenceFieldUpdater.newUpdater(BaseSink.class,
				LongConsumer.class,
				"requestConsumer");

		BaseSink(CoreSubscriber<? super T> actual) {
			this.actual = actual;
			this.ctx = actual.currentContext();
		}

		@Override
		public Context currentContext() {
			//we cache the context for hooks purposes, but this forces to go through the
			// chain when queried for context, in case downstream can update the Context...
			return actual.currentContext();
		}

		/**
		 * 默认情况 将 complete 转发到 subscriber.onComplete
		 */
		@Override
		public void complete() {
			if (isTerminated()) {
				return;
			}
			try {
				actual.onComplete();
			}
			finally {
				disposeResource(false);
			}
		}

		@Override
		public void error(Throwable e) {
			if (isTerminated()) {
				Operators.onOperatorError(e, ctx);
				return;
			}
			try {
				actual.onError(e);
			}
			finally {
				disposeResource(false);
			}
		}

		@Override
		public final void cancel() {
			disposeResource(true);
			onCancel();
		}

		/**
		 * 丢弃资源
		 * @param isCancel  是否是因为 关闭而触发
		 */
		void disposeResource(boolean isCancel) {
			Disposable disposed = isCancel ? CANCELLED : TERMINATED;
			Disposable d = disposable;
			if (d != TERMINATED && d != CANCELLED) {
				d = DISPOSABLE.getAndSet(this, disposed);
				// 触发disposable 的相关方法
				if (d != null && d != TERMINATED && d != CANCELLED) {
					if (isCancel && d instanceof SinkDisposable) {
						((SinkDisposable) d).cancel();
					}
					d.dispose();
				}
			}
		}

		@Override
		public long requestedFromDownstream() {
			return requested;
		}

		void onCancel() {
			// default is no-op
		}

		@Override
		public final boolean isCancelled() {
			return disposable == CANCELLED;
		}

		final boolean isTerminated() {
			return disposable == TERMINATED;
		}

		/**
		 * 通过函数对象处理 拉取的请求数
		 * @param n
		 */
		@Override
		public final void request(long n) {
			if (Operators.validate(n)) {
				Operators.addCap(REQUESTED, this, n);

				LongConsumer consumer = requestConsumer;
				if (n > 0 && consumer != null && !isCancelled()) {
					consumer.accept(n);
				}
				onRequestedFromDownstream();
			}
		}

		/**
		 * 当处理 下游的 request 后触发的钩子
		 */
		void onRequestedFromDownstream() {
			// default is no-op
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		/**
		 * 使用指定的消费者处理数据
		 * @param consumer the consumer to invoke on each request
		 * @return
		 */
		@Override
		public FluxSink<T> onRequest(LongConsumer consumer) {
			Objects.requireNonNull(consumer, "onRequest");
			onRequest(consumer, n -> {
			}, Long.MAX_VALUE);
			return this;
		}

		/**
		 * 处理请求函数
		 * @param initialRequestConsumer
		 * @param requestConsumer
		 * @param value
		 */
		protected void onRequest(LongConsumer initialRequestConsumer,
				LongConsumer requestConsumer,
				long value) {
			if (!REQUEST_CONSUMER.compareAndSet(this, null, requestConsumer)) {
				throw new IllegalStateException(
						"A consumer has already been assigned to consume requests");
			}
			else if (value > 0) {
				initialRequestConsumer.accept(value);
			}
		}

		/**
		 * 当该对象被关闭时
		 * @param d the {@link Disposable} to use as a callback
		 * @return
		 */
		@Override
		public final FluxSink<T> onCancel(Disposable d) {
			Objects.requireNonNull(d, "onCancel");
			// 该对象会在 cancel 时 触发 d
			SinkDisposable sd = new SinkDisposable(null, d);
			if (!DISPOSABLE.compareAndSet(this, null, sd)) {
				Disposable c = disposable;
				if (c == CANCELLED) {
					d.dispose();
				}
				else if (c instanceof SinkDisposable) {
					SinkDisposable current = (SinkDisposable) c;
					if (current.onCancel == null) {
						current.onCancel = d;
					}
					else {
						d.dispose();
					}
				}
			}
			return this;
		}

		@Override
		public final FluxSink<T> onDispose(Disposable d) {
			Objects.requireNonNull(d, "onDispose");
			SinkDisposable sd = new SinkDisposable(d, null);
			if (!DISPOSABLE.compareAndSet(this, null, sd)) {
				Disposable c = disposable;
				if (c == TERMINATED || c == CANCELLED) {
					d.dispose();
				}
				else if (c instanceof SinkDisposable) {
					SinkDisposable current = (SinkDisposable) c;
					if (current.disposable == null) {
						current.disposable = d;
					}
					else {
						d.dispose();
					}
				}
			}
			return this;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.TERMINATED) return disposable == TERMINATED;
			if (key == Attr.CANCELLED) return disposable == CANCELLED;
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) {
				return requested;
			}

			return InnerProducer.super.scanUnsafe(key);
		}

		@Override
		public String toString() {
			return "FluxSink";
		}
	}

	/**
	 * 忽略 背压警告
	 * @param <T>
	 */
	static final class IgnoreSink<T> extends BaseSink<T> {

		IgnoreSink(CoreSubscriber<? super T> actual) {
			super(actual);
		}

		/**
		 * 接收到一个新的数据
		 * @param t the value to emit, not null
		 * @return
		 */
		@Override
		public FluxSink<T> next(T t) {
			// 如果当前对象已经终止 丢弃收到的数据
			if (isTerminated()) {
				Operators.onNextDropped(t, ctx);
				return this;
			}
			// 如果关闭 丢弃
			if (isCancelled()) {
				Operators.onDiscard(t, ctx);
				return this;
			}

			// 触发 onNext
			actual.onNext(t);

			// 减少 req 的数量
			for (; ; ) {
				long r = requested;
				if (r == 0L || REQUESTED.compareAndSet(this, r, r - 1)) {
					return this;
				}
			}
		}

		@Override
		public String toString() {
			return "FluxSink(" + OverflowStrategy.IGNORE + ")";
		}
	}

	static abstract class NoOverflowBaseAsyncSink<T> extends BaseSink<T> {

		NoOverflowBaseAsyncSink(CoreSubscriber<? super T> actual) {
			super(actual);
		}

		@Override
		public final FluxSink<T> next(T t) {
			if (isTerminated()) {
				Operators.onNextDropped(t, ctx);
				return this;
			}

			if (requested != 0) {
				actual.onNext(t);
				Operators.produced(REQUESTED, this, 1);
			}
			// 代表 产生的数据比 request 还要多
			else {
				// 处理生产过剩
				onOverflow();
				Operators.onDiscard(t, ctx);
			}
			return this;
		}

		abstract void onOverflow();
	}

	/**
	 * 当生产过剩时 丢弃新元素
	 * @param <T>
	 */
	static final class DropAsyncSink<T> extends NoOverflowBaseAsyncSink<T> {

		DropAsyncSink(CoreSubscriber<? super T> actual) {
			super(actual);
		}

		@Override
		void onOverflow() {
			// nothing to do
		}

		@Override
		public String toString() {
			return "FluxSink(" + OverflowStrategy.DROP + ")";
		}

	}

	/**
	 * 当 数据源超过请求数量时 抛出异常
	 * @param <T>
	 */
	static final class ErrorAsyncSink<T> extends NoOverflowBaseAsyncSink<T> {

		ErrorAsyncSink(CoreSubscriber<? super T> actual) {
			super(actual);
		}

		@Override
		void onOverflow() {
			error(Exceptions.failWithOverflow());
		}


		@Override
		public String toString() {
			return "FluxSink(" + OverflowStrategy.ERROR + ")";
		}

	}

	/**
	 * 当生产过剩时 将数据先保存起来  主要避免OOM 异常
	 * @param <T>
	 */
	static final class BufferAsyncSink<T> extends BaseSink<T> {

		final Queue<T> queue;

		Throwable error;
		volatile boolean done; //done is still useful to be able to drain before the terminated handler is executed

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<BufferAsyncSink> WIP =
				AtomicIntegerFieldUpdater.newUpdater(BufferAsyncSink.class, "wip");

		BufferAsyncSink(CoreSubscriber<? super T> actual, int capacityHint) {
			super(actual);
			this.queue = Queues.<T>unbounded(capacityHint).get();
		}

		@Override
		public FluxSink<T> next(T t) {
			queue.offer(t);
			drain();
			return this;
		}

		@Override
		public void error(Throwable e) {
			error = e;
			done = true;
			drain();
		}

		@Override
		public void complete() {
			done = true;
			drain();
		}

		@Override
		void onRequestedFromDownstream() {
			drain();
		}

		@Override
		void onCancel() {
			drain();
		}

		//impl note: don't use isTerminated() in the drain loop,
		//it needs to first check the `done` status before setting `disposable` to TERMINATED
		//otherwise it would either loose the ability to drain or the ability to invoke the
		//handler at the right time.
		void drain() {
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}

			final Subscriber<? super T> a = actual;
			final Queue<T> q = queue;

			for (; ; ) {
				long r = requested;
				long e = 0L;

				while (e != r) {
					if (isCancelled()) {
						Operators.onDiscardQueueWithClear(q, ctx, null);
						if (WIP.decrementAndGet(this) != 0) {
							continue;
						}
						else {
							return;
						}
					}

					boolean d = done;

					T o = q.poll();

					boolean empty = o == null;

					if (d && empty) {
						Throwable ex = error;
						if (ex != null) {
							super.error(ex);
						}
						else {
							super.complete();
						}
						return;
					}

					if (empty) {
						break;
					}

					a.onNext(o);

					e++;
				}

				if (e == r) {
					if (isCancelled()) {
						Operators.onDiscardQueueWithClear(q, ctx, null);
						if (WIP.decrementAndGet(this) != 0) {
							continue;
						}
						else {
							return;
						}
					}

					boolean d = done;

					boolean empty = q.isEmpty();

					if (d && empty) {
						Throwable ex = error;
						if (ex != null) {
							super.error(ex);
						}
						else {
							super.complete();
						}
						return;
					}
				}

				if (e != 0) {
					Operators.produced(REQUESTED, this, e);
				}

				if (WIP.decrementAndGet(this) == 0) {
					break;
				}
			}
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.BUFFERED) {
				return queue.size();
			}
			if (key == Attr.TERMINATED) {
				return done;
			}
			if (key == Attr.ERROR) {
				return error;
			}

			return super.scanUnsafe(key);
		}

		@Override
		public String toString() {
			return "FluxSink(" + OverflowStrategy.BUFFER + ")";
		}
	}

	/**
	 * 当生产过剩时  只保留最后一个数据 这样之后如果继续调用 request 只会获取到最新的数据
	 * @param <T>
	 */
	static final class LatestAsyncSink<T> extends BaseSink<T> {

		final AtomicReference<T> queue;

		Throwable error;
		volatile boolean done; //done is still useful to be able to drain before the terminated handler is executed

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<LatestAsyncSink> WIP =
				AtomicIntegerFieldUpdater.newUpdater(LatestAsyncSink.class, "wip");

		LatestAsyncSink(CoreSubscriber<? super T> actual) {
			super(actual);
			this.queue = new AtomicReference<>();
		}

		/**
		 * 当接收到新数据时触发
		 * @param t the value to emit, not null
		 * @return
		 */
		@Override
		public FluxSink<T> next(T t) {
			T old = queue.getAndSet(t);
			Operators.onDiscard(old, ctx);
			drain();
			return this;
		}

		@Override
		public void error(Throwable e) {
			error = e;
			done = true;
			drain();
		}

		@Override
		public void complete() {
			done = true;
			drain();
		}

		@Override
		void onRequestedFromDownstream() {
			drain();
		}

		@Override
		void onCancel() {
			drain();
		}

		//impl note: don't use isTerminated() in the drain loop,
		//it needs to first check the `done` status before setting `disposable` to TERMINATED
		//otherwise it would either loose the ability to drain or the ability to invoke the
		//handler at the right time.
		// 消耗内部的数据
		void drain() {
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}

			final Subscriber<? super T> a = actual;
			final AtomicReference<T> q = queue;

			for (; ; ) {
				long r = requested;
				long e = 0L;

				// 当requested 不为0 的时候进入 并尝试消耗数据
				while (e != r) {
					if (isCancelled()) {
						T old = q.getAndSet(null);
						Operators.onDiscard(old, ctx);
						if (WIP.decrementAndGet(this) != 0) {
							continue;
						}
						else {
							return;
						}
					}

					boolean d = done;

					// request 的数据都是从 该 queue 中获取的  next() 会先设置 queue 然后 drain再从queue 中消费数据
					T o = q.getAndSet(null);

					boolean empty = o == null;

					if (d && empty) {
						Throwable ex = error;
						if (ex != null) {
							super.error(ex);
						}
						else {
							super.complete();
						}
						return;
					}

					if (empty) {
						break;
					}

					a.onNext(o);

					e++;
				}

				// 代表 request 归0
				if (e == r) {
					if (isCancelled()) {
						T old = q.getAndSet(null);
						Operators.onDiscard(old, ctx);
						if (WIP.decrementAndGet(this) != 0) {
							continue;
						}
						else {
							return;
						}
					}

					boolean d = done;

					boolean empty = q.get() == null;

					if (d && empty) {
						Throwable ex = error;
						if (ex != null) {
							super.error(ex);
						}
						else {
							super.complete();
						}
						return;
					}
				}

				if (e != 0) {
					Operators.produced(REQUESTED, this, e);
				}

				if (WIP.decrementAndGet(this) == 0) {
					break;
				}
			}
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.BUFFERED) {
				return queue.get() == null ? 0 : 1;
			}
			if (key == Attr.TERMINATED) {
				return done;
			}
			if (key == Attr.ERROR) {
				return error;
			}

			return super.scanUnsafe(key);
		}

		@Override
		public String toString() {
			return "FluxSink(" + OverflowStrategy.LATEST + ")";
		}
	}

	/**
	 * 代表下沉对象  内部组合了2个 丢弃对象 在不同的时机触发
	 */
	static final class SinkDisposable implements Disposable {

		Disposable onCancel;

		Disposable disposable;

		SinkDisposable(@Nullable Disposable disposable, @Nullable Disposable onCancel) {
			this.disposable = disposable;
			this.onCancel = onCancel;
		}

		@Override
		public void dispose() {
			if (disposable != null) {
				disposable.dispose();
			}
		}

		public void cancel() {
			if (onCancel != null) {
				onCancel.dispose();
			}
		}
	}
}
