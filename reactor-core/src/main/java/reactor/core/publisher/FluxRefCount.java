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
import java.util.function.Consumer;

import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;

/**
 * Connects to the underlying Flux once the given number of Subscribers subscribed
 * to it and disconnects once all Subscribers cancelled their Subscriptions.
 *
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 * 生成引用数对象
 */
final class FluxRefCount<T> extends Flux<T> implements Scannable, Fuseable {

	/**
	 * 数据源
	 */
	final ConnectableFlux<? extends T> source;

	/**
	 * 最少订阅数
	 */
	final int n;

	@Nullable
	RefCountMonitor<T> connection;

	FluxRefCount(ConnectableFlux<? extends T> source, int n) {
		if (n <= 0) {
			throw new IllegalArgumentException("n > 0 required but it was " + n);
		}
		this.source = Objects.requireNonNull(source, "source");
		this.n = n;
	}

	@Override
	public int getPrefetch() {
		return source.getPrefetch();
	}

	/**
	 * 当某个对象尝试订阅
	 * @param actual the {@link Subscriber} interested into the published sequence
	 */
	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		RefCountMonitor<T> conn;

		boolean connect = false;
		synchronized (this) {
			conn = connection;
			if (conn == null || conn.terminated) {
				// 初始化监控对象
				conn = new RefCountMonitor<>(this);
				connection = conn;
			}

			// 每增加一个订阅者 记录当前连接数量  (RefCount 本身就是用来统计当前连接数的)
			long c = conn.subscribers;
			conn.subscribers = c + 1;
			// 当达到连接数时 修改成 已连接 (connected)
			if (!conn.connected && c + 1 == n) {
				connect = true;
				conn.connected = true;
			}
		}

		// 每个订阅者都被包装成inner 对象 并交由conn 调控   此时source 本身也是一个 connectableFlux 也就是 触发下游的时机 应该
		// 也是要到最小订阅者数量
		source.subscribe(new RefCountInner<>(actual, conn));

		// 达到数量才触发 connect   推测 connect 才会真正将上游的数据发送到下游
		if (connect) {
			source.connect(conn);
		}
	}

	void cancel(RefCountMonitor rc) {
		Disposable dispose = null;
		synchronized (this) {
			if (rc.terminated) {
				return;
			}
			long c = rc.subscribers - 1;
			rc.subscribers = c;
			if (c != 0L || !rc.connected) {
				return;
			}
			if (rc == connection) {
				dispose = RefCountMonitor.DISCONNECT.getAndSet(rc, Disposables.disposed());
				connection = null;
			}
		}
		if (dispose != null) {
			dispose.dispose();
		}
	}

	void terminated(RefCountMonitor rc) {
		synchronized (this) {
			if (!rc.terminated) {
				rc.terminated = true;
				connection = null;
			}
		}
	}

	@Override
	@Nullable
	public Object scanUnsafe(Attr key) {
		if (key == Attr.PREFETCH) return getPrefetch();
		if (key == Attr.PARENT) return source;

		return null;
	}

	/**
	 * 连接数监控对象
	 * @param <T>
	 */
	static final class RefCountMonitor<T> implements Consumer<Disposable> {

		final FluxRefCount<? extends T> parent;

		long subscribers;

		boolean terminated;
		boolean connected;

		volatile Disposable disconnect;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<RefCountMonitor, Disposable> DISCONNECT =
				AtomicReferenceFieldUpdater.newUpdater(RefCountMonitor.class, Disposable.class, "disconnect");

		RefCountMonitor(FluxRefCount<? extends T> parent) {
			this.parent = parent;
		}

		@Override
		public void accept(Disposable r) {
			OperatorDisposables.replace(DISCONNECT, this, r);
		}

		void innerCancelled() {
			parent.cancel(this);
		}
		
		void upstreamFinished() {
			parent.terminated(this);
		}
	}

	static final class RefCountInner<T>
			implements QueueSubscription<T>, InnerOperator<T, T> {

		/**
		 * 实际订阅者
		 */
		final CoreSubscriber<? super T> actual;

		/**
		 * 被该监控器管理
		 */
		final RefCountMonitor<T> connection;

		Subscription s;
		QueueSubscription<T> qs;

		volatile     int parentDone; //used to guard against doubly terminating subscribers (eg. double cancel)
		static final AtomicIntegerFieldUpdater<RefCountInner> PARENT_DONE =
				AtomicIntegerFieldUpdater.newUpdater(RefCountInner.class, "parentDone");

		RefCountInner(CoreSubscriber<? super T> actual, RefCountMonitor<T> connection) {
			this.actual = actual;
			this.connection = connection;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr. PARENT) return s;
			if (key == Attr.TERMINATED) return parentDone == 1;
			if (key == Attr.CANCELLED) return parentDone == 2;

			return InnerOperator.super.scanUnsafe(key);
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
			actual.onNext(t);
		}

		@Override
		public void onError(Throwable t) {
			if (PARENT_DONE.compareAndSet(this, 0, 1)) {
				connection.upstreamFinished();
				actual.onError(t);
			}
			else {
				Operators.onErrorDropped(t, actual.currentContext());
			}
		}

		@Override
		public void onComplete() {
			if (PARENT_DONE.compareAndSet(this, 0, 1)) {
				connection.upstreamFinished();
				actual.onComplete();
			}
		}

		@Override
		public void request(long n) {
			s.request(n);
		}

		@Override
		public void cancel() {
			s.cancel();
			if (PARENT_DONE.compareAndSet(this, 0, 2)) {
				connection.innerCancelled();
			}
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		@SuppressWarnings("unchecked")
		public int requestFusion(int requestedMode) {
			if(s instanceof QueueSubscription){
				qs = (QueueSubscription<T>)s;
				return qs.requestFusion(requestedMode);
			}
			return Fuseable.NONE;
		}

		@Override
		@Nullable
		public T poll() {
			return qs.poll();
		}

		@Override
		public int size() {
			return qs.size();
		}

		@Override
		public boolean isEmpty() {
			return qs.isEmpty();
		}

		@Override
		public void clear() {
			qs.clear();
		}
	}
}
