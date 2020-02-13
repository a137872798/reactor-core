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

import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CorePublisher;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.scheduler.Scheduler;
import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;
import reactor.util.context.Context;

/**
 * @param <T>
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 * 该对象跟 其他 flux 的不同点是  flux 每当设置一个订阅者 都要把所有的数据都重新发送到下游
 * 而该对象在设置第一个订阅者后 开始向 最上游拉取数据 同时之后的订阅者可能获取不到完整的数据 这要看buffer 的过期策略
 * 同时 当上游的数据发射完成后 之后的订阅者就没办法拿到数据了
 * 一般的 flux 为每个订阅者单独创建的 subscription 内封装了 done 属性 也就是跟数据源无关
 * 这样从外部看来 每个订阅者共同消费一批数据
 * 一般的flux 像是每个订阅者消费一批数据
 */
final class FluxReplay<T> extends ConnectableFlux<T> implements Scannable, Fuseable,
                                                                OptimizableOperator<T, T> {

	/**
	 * 最上层数据源
	 */
	final CorePublisher<T>   source;
	/**
	 * 代表重复多少个数据
	 */
	final int            history;
	final long           ttl;
	/**
	 * 内部是一个  ScheduledExecutorService[]
	 */
	final Scheduler scheduler;

	volatile ReplaySubscriber<T> connection;

	interface ReplaySubscription<T> extends QueueSubscription<T>, InnerProducer<T> {

		/**
		 * 返回实际的订阅者
		 * @return
		 */
		@Override
		CoreSubscriber<? super T> actual();

		/**
		 * 是否允许访问
		 * @return
		 */
		boolean enter();

		int leave(int missed);

		/**
		 * 代表产生了一些数据  每当往下游发送了数据会触发该方法
		 * @param n
		 */
		void produced(long n);

		void node(@Nullable Object node);

		@Nullable
		Object node();

		int tailIndex();

		void tailIndex(int tailIndex);

		int index();

		void index(int index);

		int fusionMode();

		boolean isCancelled();

		long requested();
	}

	/**
	 * 用于存放重播数据的 容器
	 * @param <T>
	 */
	interface ReplayBuffer<T> {

		/**
		 * 往容器中添加数据  (当上游将数据传播过来时 先触发该方法)
		 * @param value
		 */
		void add(T value);

		/**
		 * 代表当上游传播数据 遇到异常时
		 * @param ex
		 */
		void onError(Throwable ex);

		/**
		 * 获取内部异常对象
		 * @return
		 */
		@Nullable
		Throwable getError();

		/**
		 * 代表上游的数据流全部传播到下游
		 */
		void onComplete();

		/**
		 * 使用该 rs 对象 去消费 buffer 中的数据
		 * @param rs
		 */
		void replay(ReplaySubscription<T> rs);

		boolean isDone();

		/**
		 * 从 buffer 中获取某个数据体
		 * @param rs
		 * @return
		 */
		@Nullable
		T poll(ReplaySubscription<T> rs);

		/**
		 * 从 buffer 中清除该 subscription 关联的数据  看来每次重复任务都会变成一个节点对象保存在buffer中 并且以链表形式连接
		 * @param rs
		 */
		void clear(ReplaySubscription<T> rs);

		boolean isEmpty(ReplaySubscription<T> rs);

		int size(ReplaySubscription<T> rs);

		int size();

		int capacity();

		boolean isExpired();
	}

	/**
	 * 用于存储 重复数据的容器
	 * @param <T>
	 */
	static final class SizeAndTimeBoundReplayBuffer<T> implements ReplayBuffer<T> {

		/**
		 * 该对象本身 是使用原子更新器 进行更新的
		 * @param <T>
		 */
		static final class TimedNode<T> extends AtomicReference<TimedNode<T>> {

			final T    value;
			final long time;

			TimedNode(@Nullable T value, long time) {
				this.value = value;
				this.time = time;
			}
		}

		/**
		 * 代表buffer 内部最多存放多少元素  size 不会超过该值
		 */
		final int            limit;
		final long           maxAge;
		final Scheduler scheduler;
		/**
		 * 代表buffer 内部数据的长度
		 */
		int size;

		/**
		 * 代表容器的头节点 该字段使用 volatile 修饰
		 */
		volatile TimedNode<T> head;

		/**
		 * 容器尾节点
		 */
		TimedNode<T> tail;

		Throwable error;
		static final long NOT_DONE = Long.MIN_VALUE;

		/**
		 * 推测是最近一次 触发的时间戳
		 */
		volatile long done = NOT_DONE;

		/**
		 *
		 * @param limit
		 * @param maxAge  应该是重复数据触发的最大时间间隔
		 * @param scheduler
		 */
		SizeAndTimeBoundReplayBuffer(int limit,
				long maxAge,
				Scheduler scheduler) {
			this.limit = limit;
			this.maxAge = maxAge;
			this.scheduler = scheduler;
			// 初始情况 头/尾 节点 都指向一个 没有数据的空节点  需要被重复发往下游的数据 应该就会被包装成node 并填充到buffer中
			TimedNode<T> h = new TimedNode<>(null, 0L);
			this.tail = h;
			this.head = h;
		}

		/**
		 * 判断 重复数据是否过期了
		 * @return
		 */
		@Override
		public boolean isExpired() {
			long done = this.done;
			// 代表至少触发过一次 且 距离上次触发 超过时间间隔
			return done != NOT_DONE && scheduler.now(TimeUnit.MILLISECONDS) - maxAge > done;
		}

		/**
		 * 不同的 rs 对象 可对同一份数据进行消费
		 */
		@SuppressWarnings("unchecked")
		void replayNormal(ReplaySubscription<T> rs) {
			int missed = 1;
			// 关联的订阅者
			final Subscriber<? super T> a = rs.actual();

			for (; ; ) {
				// 获取 subscription 关联的 node
				@SuppressWarnings("unchecked") TimedNode<T> node =
						(TimedNode<T>) rs.node();
				// 如果该节点 还没有指定node 那么就是从head 开始 如果已经指定了node 就从node 开始往下找位置
				if (node == null) {
					// 将 node 指向 head
					node = head;
					// 代表数据流还没有被处理完
					if (done == NOT_DONE) {
						// skip old entries
						// 丢弃 太旧的节点
						long limit = scheduler.now(TimeUnit.MILLISECONDS) - maxAge;
						TimedNode<T> next = node;
						while (next != null) {
							long ts = next.time;
							if (ts > limit) {
								break;
							}
							node = next;
							next = node.get();
						}
					}
				}

				// 获取 subscription 已经被调用的请求数
				long r = rs.requested();
				long e = 0L;

				// 这里就根据请求数 不断往下搜索 node 连接的节点
				while (e != r) {
					// 如果本 subscription 已经被关闭 直接返回  这里将rs.node 置空了
					if (rs.isCancelled()) {
						rs.node(null);
						return;
					}

					boolean d = done != NOT_DONE;
					// 获取下个节点
					TimedNode<T> next = node.get();
					boolean empty = next == null;

					// 如果 done == true 那么可以触发 onComplete 了
					if (d && empty) {
						rs.node(null);
						Throwable ex = error;
						if (ex != null) {
							a.onError(ex);
						}
						else {
							a.onComplete();
						}
						return;
					}

					// 代表还没有数据可以消费
					if (empty) {
						break;
					}

					// 将node 携带的数据传到下游
					a.onNext(next.value);

					e++;
					// 同时更新节点
					node = next;
				}

				// 代表该 subscription 相关的数据都处理完了
				if (e == r) {
					if (rs.isCancelled()) {
						rs.node(null);
						return;
					}

					boolean d = done != NOT_DONE;
					boolean empty = node.get() == null;

					// 如果 本数据流已经关闭了 触发 onComplete
					if (d && empty) {
						rs.node(null);
						Throwable ex = error;
						if (ex != null) {
							a.onError(ex);
						}
						else {
							a.onComplete();
						}
						return;
					}
				}

				// 如果确实消费了 一些数据  触发 produced
				if (e != 0L) {
					if (r != Long.MAX_VALUE) {
						// 内部就是减少请求数
						rs.produced(e);
					}
				}

				// 更新node 指针
				rs.node(node);

				// missed 默认为 1 那么这里会不断循环 直到 request 的请求数全部被消耗
				missed = rs.leave(missed);
				if (missed == 0) {
					break;
				}
			}
		}

		/**
		 * 融合模式进行重复
		 * @param rs
		 */
		void replayFused(ReplaySubscription<T> rs) {
			int missed = 1;

			final Subscriber<? super T> a = rs.actual();

			for (; ; ) {

				if (rs.isCancelled()) {
					rs.node(null);
					return;
				}

				boolean d = done != NOT_DONE;

				// 将null 传播到下游
				a.onNext(null);

				// 代表数据已经处理完了
				if (d) {
					Throwable ex = error;
					if (ex != null) {
						a.onError(ex);
					}
					else {
						a.onComplete();
					}
					return;
				}

				// 修改 wip
				missed = rs.leave(missed);
				if (missed == 0) {
					break;
				}
			}
		}

		/**
		 * 记录当前异常 以及终结时间
		 * @param ex
		 */
		@Override
		public void onError(Throwable ex) {
			done = scheduler.now(TimeUnit.MILLISECONDS);
			error = ex;
		}

		@Override
		@Nullable
		public Throwable getError() {
			return error;
		}

		/**
		 * 当该 buffer 对象对应的上游数据流结束时
		 */
		@Override
		public void onComplete() {
			// 记录 读取完数据的时间
			done = scheduler.now(TimeUnit.MILLISECONDS);
		}

		@Override
		public boolean isDone() {
			return done != NOT_DONE;
		}

		/**
		 * 获取该 subscription 在该buffer 内 最早的节点
		 * @param rs
		 * @return
		 */
		@SuppressWarnings("unchecked")
		TimedNode<T> latestHead(ReplaySubscription<T> rs) {
			long now = scheduler.now(TimeUnit.MILLISECONDS) - maxAge;

			// 应该是这个套路 如果 node 还没有设置 那么就从head 开始查找
			TimedNode<T> h = (TimedNode<T>) rs.node();
			if (h == null) {
				h = head;
			}
			TimedNode<T> n;
			// 返回第一个时间超过 now的 并且node 的排列是按照 time 递增的顺序的
			while ((n = h.get()) != null) {
				if (n.time > now) {
					break;
				}
				h = n;
			}
			return h;
		}

		/**
		 * 获取 subscription 相关的node
		 * @param rs
		 * @return
		 */
		@Override
		@Nullable
		public T poll(ReplaySubscription<T> rs) {
			TimedNode<T> node = latestHead(rs);
			TimedNode<T> next;
			long now = scheduler.now(TimeUnit.MILLISECONDS) - maxAge;
			while ((next = node.get()) != null) {
				if (next.time > now) {
					node = next;
					break;
				}
				node = next;
			}
			if (next == null) {
				return null;
			}
			rs.node(next);

			return node.value;
		}

		@Override
		public void clear(ReplaySubscription<T> rs) {
			rs.node(null);
		}

		@Override
		@SuppressWarnings("unchecked")
		public boolean isEmpty(ReplaySubscription<T> rs) {
			TimedNode<T> node = latestHead(rs);
			return node.get() == null;
		}

		/**
		 * 从该subscription 开始一共有多少节点
		 * @param rs
		 * @return
		 */
		@Override
		public int size(ReplaySubscription<T> rs) {
			// 先获取该subscription 最早的node
			TimedNode<T> node = latestHead(rs);
			int count = 0;

			TimedNode<T> next;
			while ((next = node.get()) != null && count != Integer.MAX_VALUE) {
				count++;
				node = next;
			}

			return count;
		}

		/**
		 * 返回总长度  从head 开始读取直到末尾
		 * @return
		 */
		@Override
		public int size() {
			TimedNode<T> node = head;
			int count = 0;

			TimedNode<T> next;
			while ((next = node.get()) != null && count != Integer.MAX_VALUE) {
				count++;
				node = next;
			}

			return count;
		}

		@Override
		public int capacity() {
			return limit;
		}

		/**
		 * 往该容器中添加数据
		 * @param value
		 */
		@Override
		public void add(T value) {
			// 将传入的值 与当前时间包装成一个 node 对象
			TimedNode<T> n = new TimedNode<>(value, scheduler.now(TimeUnit.MILLISECONDS));
			// 此时 head 和 tail 还是指向同一内存 所以通过操作tail 可以影响到 head
			tail.set(n);
			tail = n;
			int s = size;
			// 当超过界限时 size 不变 而是将head 指向下一个节点
			if (s == limit) {
				head = head.get();
			}
			else {
				// 否则增加size
				size = s + 1;
			}
			// 将当前时间往前数一个单位 在这之前的节点都要被丢弃
			long limit = scheduler.now(TimeUnit.MILLISECONDS) - maxAge;

			// 从头节点 往下遍历
			TimedNode<T> h = head;
			TimedNode<T> next;
			int removed = 0;
			for (; ; ) {
				next = h.get();
				if (next == null) {
					break;
				}

				// 找到 超过 limit 的 节点
				if (next.time > limit) {
					// 代表 有在 limit之前的节点
					if (removed != 0) {
						// 减少节点数量
						size = size - removed;
						head = h;
					}
					break;
				}

				h = next;
				removed++;
			}
		}

		/**
		 * buffer 对象会接收最上游的 flux数据 之后 通过 ReplaySubscriber  将数据保存到buffer 中  并通知所有维护的 ReplaySubscription对象去拉数据
		 * @param rs
		 */
		@Override
		@SuppressWarnings("unchecked")
		public void replay(ReplaySubscription<T> rs) {
			// 如果 rs 不允许进入 直接返回
			if (!rs.enter()) {
				return;
			}

			// 代表没有指定模式
			if (rs.fusionMode() == NONE) {
				replayNormal(rs);
			}
			else {
				replayFused(rs);
			}
		}
	}

	/**
	 * 该对象 就是没有 limit
	 * @param <T>
	 */
	static final class UnboundedReplayBuffer<T> implements ReplayBuffer<T> {

		final int batchSize;

		volatile int size;

		// head tail 都是数组对象  长度为 batch+1

		final Object[] head;

		Object[] tail;

		/**
		 * 代表 当前添加到 tail 的下标
		 */
		int tailIndex;

		volatile boolean done;
		Throwable error;

		/**
		 * 使用一个 批大小来初始化 head 和tail 都是 batch+1
		 * @param batchSize
		 */
		UnboundedReplayBuffer(int batchSize) {
			this.batchSize = batchSize;
			Object[] n = new Object[batchSize + 1];
			this.tail = n;
			this.head = n;
		}

		@Override
		public boolean isExpired() {
			return false;
		}

		@Override
		@Nullable
		public Throwable getError() {
			return error;
		}

		@Override
		public int capacity() {
			return Integer.MAX_VALUE;
		}

		/**
		 * 往 buffer 中填充数据
		 * @param value
		 */
		@Override
		public void add(T value) {
			int i = tailIndex;
			Object[] a = tail;
			// 这里的 扩容 注意 一旦超过了 batch 就往下一个下标中 设置一个 等大的 数组 并且重置 tailIndex
			if (i == a.length - 1) {
				Object[] b = new Object[a.length];
				b[0] = value;
				tailIndex = 1;
				a[i] = b;
				tail = b;
			}
			else {
				a[i] = value;
				tailIndex = i + 1;
			}
			size++;
		}

		@Override
		public void onError(Throwable ex) {
			error = ex;
			done = true;
		}

		@Override
		public void onComplete() {
			done = true;
		}

		/**
		 * 不断的消费 应该是这个意思???
		 * @param rs
		 */
		void replayNormal(ReplaySubscription<T> rs) {
			int missed = 1;

			final Subscriber<? super T> a = rs.actual();
			final int n = batchSize;

			for (; ; ) {

				long r = rs.requested();
				long e = 0L;

				// 同样的套路 如果没有设置node 属性 则指向 head数组
				Object[] node = (Object[]) rs.node();
				if (node == null) {
					node = head;
				}

				// 获取 当前消耗到的头尾数组的下标  只有该对象会使用到这2个属性
				int tailIndex = rs.tailIndex();
				int index = rs.index();

				while (e != r) {
					if (rs.isCancelled()) {
						rs.node(null);
						return;
					}

					boolean d = done;
					boolean empty = index == size;

					if (d && empty) {
						rs.node(null);
						Throwable ex = error;
						if (ex != null) {
							a.onError(ex);
						}
						else {
							a.onComplete();
						}
						return;
					}

					if (empty) {
						break;
					}

					// 代表已经消费到了末尾
					if (tailIndex == n) {
						// node 数组中每个元素都是一个新的数组 这里从 下一个数组的头部开始 所以 tailIndex 重置了
						node = (Object[]) node[tailIndex];
						tailIndex = 0;
					}

					@SuppressWarnings("unchecked") T v = (T) node[tailIndex];

					a.onNext(v);

					e++;
					tailIndex++;
					index++;
				}

				// 代表消耗完了所有数据  判断是否要触发 onError 或者 onComplete
				if (e == r) {
					if (rs.isCancelled()) {
						rs.node(null);
						return;
					}

					boolean d = done;
					boolean empty = index == size;

					if (d && empty) {
						rs.node(null);
						Throwable ex = error;
						if (ex != null) {
							a.onError(ex);
						}
						else {
							a.onComplete();
						}
						return;
					}
				}

				// 减少requested
				if (e != 0L) {
					if (r != Long.MAX_VALUE) {
						rs.produced(e);
					}
				}

				rs.index(index);
				rs.tailIndex(tailIndex);
				rs.node(node);

				missed = rs.leave(missed);
				if (missed == 0) {
					break;
				}
			}
		}

		/**
		 * 区别就是传播到下面的是null
		 * @param rs
		 */
		void replayFused(ReplaySubscription<T> rs) {
			int missed = 1;

			final Subscriber<? super T> a = rs.actual();

			for (; ; ) {

				if (rs.isCancelled()) {
					rs.node(null);
					return;
				}

				boolean d = done;

				a.onNext(null);

				if (d) {
					Throwable ex = error;
					if (ex != null) {
						a.onError(ex);
					}
					else {
						a.onComplete();
					}
					return;
				}

				missed = rs.leave(missed);
				if (missed == 0) {
					break;
				}
			}
		}

		@Override
		public void replay(ReplaySubscription<T> rs) {
			if (!rs.enter()) {
				return;
			}

			if (rs.fusionMode() == NONE) {
				replayNormal(rs);
			}
			else {
				replayFused(rs);
			}
		}

		@Override
		public boolean isDone() {
			return done;
		}

		@Override
		@Nullable
		public T poll(ReplaySubscription<T> rs) {
			int index = rs.index();
			if (index == size) {
				return null;
			}
			Object[] node = (Object[]) rs.node();
			if (node == null) {
				node = head;
				rs.node(node);
			}
			int tailIndex = rs.tailIndex();
			if (tailIndex == batchSize) {
				node = (Object[]) node[tailIndex];
				tailIndex = 0;
				rs.node(node);
			}
			@SuppressWarnings("unchecked") T v = (T) node[tailIndex];
			rs.index(index + 1);
			rs.tailIndex(tailIndex + 1);
			return v;
		}

		@Override
		public void clear(ReplaySubscription<T> rs) {
			rs.node(null);
		}

		@Override
		public boolean isEmpty(ReplaySubscription<T> rs) {
			return rs.index() == size;
		}

		@Override
		public int size(ReplaySubscription<T> rs) {
			return size - rs.index();
		}

		@Override
		public int size() {
			return size;
		}

	}

	/**
	 * 该对象没有 使用 time 属性
	 * @param <T>
	 */
	static final class SizeBoundReplayBuffer<T> implements ReplayBuffer<T> {

		/**
		 * 应该是只允许这么多的重复数据
		 */
		final int limit;

		volatile Node<T> head;

		Node<T> tail;

		int size;

		/**
		 * 这里 done 没有用 long 类型
		 */
		volatile boolean done;
		Throwable error;

		SizeBoundReplayBuffer(int limit) {
			if(limit < 0){
				throw new IllegalArgumentException("Limit cannot be negative");
			}
			this.limit = limit;
			Node<T> n = new Node<>(null);
			this.tail = n;
			this.head = n;
		}

		/**
		 * 该对象内 没有过期的概念
		 * @return
		 */
		@Override
		public boolean isExpired() {
			return false;
		}

		@Override
		public int capacity() {
			return limit;
		}

		/**
		 * 往buffer 内部添加一个元素 同样不会超过limit
		 * @param value
		 */
		@Override
		public void add(T value) {
			Node<T> n = new Node<>(value);
			tail.set(n);
			tail = n;
			int s = size;
			if (s == limit) {
				head = head.get();
			}
			else {
				size = s + 1;
			}
		}

		@Override
		public void onError(Throwable ex) {
			error = ex;
			done = true;
		}

		@Override
		public void onComplete() {
			done = true;
		}

		/**
		 * 普通模式下重复消耗数据
		 * @param rs
		 */
		void replayNormal(ReplaySubscription<T> rs) {
			final Subscriber<? super T> a = rs.actual();

			int missed = 1;

			for (; ; ) {

				// 获取当前针对 该 subscription 的请求数
				long r = rs.requested();
				long e = 0L;

				// 从 当前关联的node 开始往下开始消耗数据
				@SuppressWarnings("unchecked") Node<T> node = (Node<T>) rs.node();
				if (node == null) {
					node = head;
				}

				while (e != r) {
					if (rs.isCancelled()) {
						rs.node(null);
						return;
					}

					boolean d = done;
					Node<T> next = node.get();
					boolean empty = next == null;

					// 触发过 onError 或者 onComplete 那么 触发 下游的对应函数
					if (d && empty) {
						rs.node(null);
						Throwable ex = error;
						if (ex != null) {
							a.onError(ex);
						}
						else {
							a.onComplete();
						}
						return;
					}

					if (empty) {
						break;
					}

					// 如果 node 关联了下个节点 触发onNext
					a.onNext(next.value);

					e++;
					node = next;
				}

				// 代表 request 都被消耗完了
				if (e == r) {
					// 再次确认是否触发了 onComplete

					if (rs.isCancelled()) {
						rs.node(null);
						return;
					}

					boolean d = done;
					boolean empty = node.get() == null;

					if (d && empty) {
						rs.node(null);
						Throwable ex = error;
						if (ex != null) {
							a.onError(ex);
						}
						else {
							a.onComplete();
						}
						return;
					}
				}

				// 减少 requested
				if (e != 0L) {
					if (r != Long.MAX_VALUE) {
						rs.produced(e);
					}
				}

				// 更新node 引用
				rs.node(node);

				// 修改 wip
				missed = rs.leave(missed);
				if (missed == 0) {
					break;
				}
			}
		}

		/**
		 * 以融合模式 重复消费数据
		 * @param rs
		 */
		void replayFused(ReplaySubscription<T> rs) {
			int missed = 1;

			final Subscriber<? super T> a = rs.actual();

			for (; ; ) {

				if (rs.isCancelled()) {
					rs.node(null);
					return;
				}

				boolean d = done;

				// 区别就在于 这里使用null 来触发 onNext
				a.onNext(null);

				if (d) {
					Throwable ex = error;
					if (ex != null) {
						a.onError(ex);
					}
					else {
						a.onComplete();
					}
					return;
				}

				missed = rs.leave(missed);
				// 代表本次处理结束 退出 replay 方法
				if (missed == 0) {
					break;
				}
			}
		}

		/**
		 * @param rs
		 */
		@Override
		public void replay(ReplaySubscription<T> rs) {
			if (!rs.enter()) {
				return;
			}

			// 普通模式
			if (rs.fusionMode() == NONE) {
				replayNormal(rs);
			}
			else {
				// 融合模式
				replayFused(rs);
			}
		}

		@Override
		@Nullable
		public Throwable getError() {
			return error;
		}

		@Override
		public boolean isDone() {
			return done;
		}

		/**
		 * 该 node 没有 time 属性
		 * @param <T>
		 */
		static final class Node<T> extends AtomicReference<Node<T>> {

			/** */
			private static final long serialVersionUID = 3713592843205853725L;

			final T value;

			Node(@Nullable T value) {
				this.value = value;
			}
		}

		@Override
		@Nullable
		public T poll(ReplaySubscription<T> rs) {
			@SuppressWarnings("unchecked") Node<T> node = (Node<T>) rs.node();
			if (node == null) {
				node = head;
				rs.node(node);
			}

			Node<T> next = node.get();
			if (next == null) {
				return null;
			}
			rs.node(next);

			return next.value;
		}

		@Override
		public void clear(ReplaySubscription<T> rs) {
			rs.node(null);
		}

		@Override
		public boolean isEmpty(ReplaySubscription<T> rs) {
			@SuppressWarnings("unchecked") Node<T> node = (Node<T>) rs.node();
			if (node == null) {
				node = head;
				rs.node(node);
			}
			return node.get() == null;
		}

		@Override
		public int size(ReplaySubscription<T> rs) {
			@SuppressWarnings("unchecked") Node<T> node = (Node<T>) rs.node();
			if (node == null) {
				node = head;
			}
			int count = 0;

			Node<T> next;
			while ((next = node.get()) != null && count != Integer.MAX_VALUE) {
				count++;
				node = next;
			}

			return count;
		}

		@Override
		public int size() {
			Node<T> node = head;
			int count = 0;

			Node<T> next;
			while ((next = node.get()) != null && count != Integer.MAX_VALUE) {
				count++;
				node = next;
			}

			return count;
		}
	}

	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<FluxReplay, ReplaySubscriber> CONNECTION =
			AtomicReferenceFieldUpdater.newUpdater(FluxReplay.class,
					ReplaySubscriber.class,
					"connection");

	@Nullable
	final OptimizableOperator<?, T> optimizableOperator;

	/**
	 *
	 * @param source
	 * @param history  代表会从source 中选多少进行重复消费
	 * @param ttl
	 * @param scheduler
	 */
	FluxReplay(CorePublisher<T> source,
			int history,
			long ttl,
			@Nullable Scheduler scheduler) {
		this.source = Objects.requireNonNull(source, "source");
		this.optimizableOperator = source instanceof OptimizableOperator ? (OptimizableOperator) source : null;
		this.history = history;
		if(history < 0){
			throw new IllegalArgumentException("History cannot be negative : " + history);
		}
		if (scheduler != null && ttl < 0) {
			throw new IllegalArgumentException("TTL cannot be negative : " + ttl);
		}
		this.ttl = ttl;
		// 调度器 可以为null
		this.scheduler = scheduler;
	}

	/**
	 * 推荐 request 的参数
	 * @return
	 */
	@Override
	public int getPrefetch() {
		return history;
	}

	/**
	 * 返回合适的 subscriber 包装对象
	 * @return
	 */
	ReplaySubscriber<T> newState() {
		if (scheduler != null) {
			return new ReplaySubscriber<>(new SizeAndTimeBoundReplayBuffer<>(history,
					ttl,
					scheduler),
					this);
		}
		if (history != Integer.MAX_VALUE) {
			return new ReplaySubscriber<>(new SizeBoundReplayBuffer<>(history),
					this);
		}
		return new ReplaySubscriber<>(new UnboundedReplayBuffer<>(Queues.SMALL_BUFFER_SIZE),
					this);
	}

	/**
	 * 代表订阅数满足最小订阅者条件 可以开始让真正的数据源往下发送数据了 并且在 disconnect 时 触发consumer
	 * @param cancelSupport the callback is called with a Disposable instance that can
	 */
	@Override
	public void connect(Consumer<? super Disposable> cancelSupport) {
		boolean doConnect;
		ReplaySubscriber<T> s;
		// 初始化 connection 对象
		for (; ; ) {
			s = connection;
			if (s == null) {
				ReplaySubscriber<T> u = newState();
				if (!CONNECTION.compareAndSet(this, null, u)) {
					continue;
				}

				s = u;
			}

			// 尝试连接  也就是将连接状态从 0 改成1
			doConnect = s.tryConnect();
			break;
		}

		cancelSupport.accept(s);
		if (doConnect) {
			// 此时才将上游数据下发到 下游
			source.subscribe(s);
		}
	}

	/**
	 * 为该 flux 设置订阅者 也就是 外部访问的入口 FluxReplay 是在最上层flux 的基础上又封装了一层 增加了 replay 的能力
	 * @param actual the {@link Subscriber} interested into the published sequence
	 */
	@Override
	@SuppressWarnings("unchecked")
	public void subscribe(CoreSubscriber<? super T> actual) {
		// 最下层的订阅者 首先会被包装
		CoreSubscriber nextSubscriber = subscribeOrReturn(actual);
		if (nextSubscriber == null) {
			return;
		}
		// 该对象默认 情况只要设置了订阅者 就会将数据发往 replaySubscriber
		source.subscribe(nextSubscriber);
	}

	@Override
	public final CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		boolean expired;
		for (; ; ) {
			ReplaySubscriber<T> c = connection;
			// 判断是否有定时功能
			expired = scheduler != null && c != null && c.buffer.isExpired();
			if (c == null || expired) {
				// 创建订阅者对象 并设置到 connection 字段中
				ReplaySubscriber<T> u = newState();
				if (!CONNECTION.compareAndSet(this, c, u)) {
					continue;
				}

				c = u;
			}

			// 将真正的订阅者 包装成 inner 对象
			ReplayInner<T> inner = new ReplayInner<>(actual);
			// 触发 inner.request  此时 inner 内部的 parent 还没有设置 只会增加 requested
			actual.onSubscribe(inner);
			// 为 ReplaySubscriber 增加 inner
			// ReplaySubscriber 对象内部有一个 subscriber[] 用于管理下面所有的inner 也就是可以往该对象设置多个订阅者
			c.add(inner);

			// 如果尝试添加的inner 已经被关闭了 那么 直接从 ReplaySubscriber 中移除该对象
			if (inner.isCancelled()) {
				c.remove(inner);
				return null;
			}

			inner.parent = c;
			// 此时 buffer 对象就会不断扫描内部是否有数据 有的话 就会通过inner 传播到下游
			c.buffer.replay(inner);

			if (expired) {
				return c;
			}

			break;
		}
		return null;
	}

	/**
	 * 最上游的数据源(flux)对象
	 * @return
	 */
	@Override
	public final CorePublisher<? extends T> source() {
		return source;
	}

	@Override
	public final OptimizableOperator<?, ? extends T> nextOptimizableSource() {
		return optimizableOperator;
	}

	@Override
	@Nullable
	public Object scanUnsafe(Scannable.Attr key) {
		if (key == Attr.PREFETCH) return getPrefetch();
		if (key == Attr.PARENT) return source;
		if (key == Attr.RUN_ON) return scheduler;

		return null;
	}

	/**
	 * 重复订阅者  首先  最上游的 flux 通过调用 replay  生成 fluxReplay 对象
	 * 之后触发 subscribe(actual) 时 将 actual 包装成了  ReplayInner
	 * 之后再将 inner 对象 添加到 ReplaySubscriber
	 * 那么 ReplaySubscriber 会被上游的 flux 包装成一个 subscription
	 * 之后 触发本对象的 onNext 也就是接收 从最上游传播过来的数据
	 * @param <T>
	 */
	static final class ReplaySubscriber<T>
			implements InnerConsumer<T>, Disposable {

		/**
		 * 该对象是由 哪个 replay 对象创建的
		 */
		final FluxReplay<T>   parent;
		/**
		 * 该订阅者关联的 buffer
		 */
		final ReplayBuffer<T> buffer;

		/**
		 * 内部封装了 本对象 以及最上游的 flux
		 */
		volatile Subscription s;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<ReplaySubscriber, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(ReplaySubscriber.class,
						Subscription.class,
						"s");

		/**
		 * 该 subscriber内部管理的所有 subscription
		 */
		volatile ReplaySubscription<T>[] subscribers;

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<ReplaySubscriber> WIP =
				AtomicIntegerFieldUpdater.newUpdater(ReplaySubscriber.class, "wip");

		/**
		 * 该对象的连接数
		 */
		volatile int connected;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<ReplaySubscriber> CONNECTED =
				AtomicIntegerFieldUpdater.newUpdater(ReplaySubscriber.class, "connected");

		@SuppressWarnings("rawtypes")
		static final ReplaySubscription[] EMPTY      = new ReplaySubscription[0];
		@SuppressWarnings("rawtypes")
		static final ReplaySubscription[] TERMINATED = new ReplaySubscription[0];

		/**
		 * 该对象是否被关闭
		 */
		volatile boolean cancelled;

		@SuppressWarnings("unchecked")
		ReplaySubscriber(ReplayBuffer<T> buffer,
				FluxReplay<T> parent) {
			this.buffer = buffer;
			this.parent = parent;
			this.subscribers = EMPTY;
		}

		/**
		 * 当该对象作为订阅者 该对象会被封装成Subscription 后 再触发该方法
		 * @param s
		 */
		@Override
		public void onSubscribe(Subscription s) {
			// 如果 buffer 已经停止工作了 关闭 Subscription
			if(buffer.isDone()){
				s.cancel();
			}
			else if (Operators.setOnce(S, this, s)) {
				// 代表重复的数量
				long max = parent.history;
				for (ReplaySubscription<T> subscriber : subscribers) {
					max = Math.max(subscriber.fusionMode() != Fuseable.NONE ? Long.MAX_VALUE : subscriber.requested(), max);
					if (max == Long.MAX_VALUE) break;
				}
				// 按照最大的请求数 向上游申请数据
				s.request(max);
			}
		}

		/**
		 * 当该对象 接收最上游的数据时 触发
		 * @param t
		 */
		@Override
		public void onNext(T t) {
			// 如果 buffer 已经被关闭了 那么丢弃传下来的数据
			ReplayBuffer<T> b = buffer;
			if (b.isDone()) {
				Operators.onNextDropped(t, currentContext());
			}
			else {
				// 否则将数据添加到 buffer 中  注意都是设置到 tail 字段
				// 其他 flux 对象 当触发 onNext 时 数据都是直接消耗掉的 没有做存储  而该对象 使用一个buffer 保存了数据
				b.add(t);
				// 当上游收到数据 该对象作为一个转发器 通知维护的 subscriber 去 buffer 中读取最新的数据
				for (ReplaySubscription<T> rs : subscribers) {
					b.replay(rs);
				}
			}
		}

		/**
		 * 当该对象接收到上游的异常时
		 * @param t
		 */
		@Override
		public void onError(Throwable t) {
			ReplayBuffer<T> b = buffer;
			if (b.isDone()) {
				Operators.onErrorDropped(t, currentContext());
			}
			else {
				// 同样做了转发
				b.onError(t);

				for (ReplaySubscription<T> rs : terminate()) {
					b.replay(rs);
				}
			}
		}

		@Override
		public void onComplete() {
			ReplayBuffer<T> b = buffer;
			if (!b.isDone()) {
				b.onComplete();

				for (ReplaySubscription<T> rs : terminate()) {
					b.replay(rs);
				}
			}
		}

		@Override
		public void dispose() {
			if (cancelled) {
				return;
			}
			if (Operators.terminate(S, this)) {
				cancelled = true;

				CONNECTION.lazySet(parent, null);

				CancellationException ex = new CancellationException("Disconnected");
				buffer.onError(ex);

				for (ReplaySubscription<T> inner : terminate()) {
					buffer.replay(inner);
				}

			}
		}

		/**
		 * 为 订阅者 增加一个inner 对象
		 * @param inner
		 * @return
		 */
		boolean add(ReplayInner<T> inner) {
			// 已经被关闭的情况下 忽略
			if (subscribers == TERMINATED) {
				return false;
			}
			synchronized (this) {
				ReplaySubscription<T>[] a = subscribers;
				// 如果本subscriber 已经被关闭了 那么不允许添加
				if (a == TERMINATED) {
					return false;
				}
				int n = a.length;

				// 使用拷贝的方式 进行扩容
				@SuppressWarnings("unchecked") ReplayInner<T>[] b =
						new ReplayInner[n + 1];
				System.arraycopy(a, 0, b, 0, n);
				b[n] = inner;

				subscribers = b;
				return true;
			}
		}

		/**
		 * 从 replaySubscriber 中移除该对象
		 * @param inner
		 */
		@SuppressWarnings("unchecked")
		void remove(ReplaySubscription<T> inner) {
			ReplaySubscription<T>[] a = subscribers;
			if (a == TERMINATED || a == EMPTY) {
				return;
			}
			synchronized (this) {
				a = subscribers;
				if (a == TERMINATED || a == EMPTY) {
					return;
				}

				int j = -1;
				int n = a.length;
				for (int i = 0; i < n; i++) {
					if (a[i] == inner) {
						j = i;
						break;
					}
				}
				if (j < 0) {
					return;
				}

				ReplaySubscription<T>[] b;
				if (n == 1) {
					b = EMPTY;
				}
				else {
					b = new ReplayInner[n - 1];
					System.arraycopy(a, 0, b, 0, j);
					System.arraycopy(a, j + 1, b, j, n - j - 1);
				}

				subscribers = b;
			}
		}

		@SuppressWarnings("unchecked")
		ReplaySubscription<T>[] terminate() {
			ReplaySubscription<T>[] a = subscribers;
			if (a == TERMINATED) {
				return a;
			}
			synchronized (this) {
				a = subscribers;
				if (a != TERMINATED) {
					subscribers = TERMINATED;
				}
				return a;
			}
		}

		boolean isTerminated() {
			return subscribers == TERMINATED;
		}

		boolean tryConnect() {
			return connected == 0 && CONNECTED.compareAndSet(this, 0, 1);
		}

		@Override
		public Context currentContext() {
			return Operators.multiSubscribersContext(subscribers);
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.PREFETCH) return Integer.MAX_VALUE;
			if (key == Attr.CAPACITY) return buffer.capacity();
			if (key == Attr.ERROR) return buffer.getError();
			if (key == Attr.BUFFERED) return buffer.size();
			if (key == Attr.TERMINATED) return isTerminated();
			if (key == Attr.CANCELLED) return cancelled;

			return null;
		}

		@Override
		public Stream<? extends Scannable> inners() {
			return Stream.of(subscribers);
		}

		@Override
		public boolean isDisposed() {
			return cancelled;
		}

	}

	/**
	 * 代表一个 可重复的 subscription
	 * @param <T>
	 */
	static final class ReplayInner<T>
			implements ReplaySubscription<T> {

		/**
		 * 该对象内部包含的 订阅者
		 */
		final CoreSubscriber<? super T> actual;

		/**
		 * 该对象用于收集子对象的数据
		 */
		ReplaySubscriber<T> parent;

		// 对应 head[] 和 tail[]

		int index;

		int tailIndex;

		/**
		 * 该对象关联的node
		 */
		Object node;

		/**
		 * 融合的模式
		 */
		int fusionMode;

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<ReplayInner> WIP =
				AtomicIntegerFieldUpdater.newUpdater(ReplayInner.class, "wip");


		/**
		 * 向该对象 索要的数据量
		 */
		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<ReplayInner> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(ReplayInner.class, "requested");


		ReplayInner(CoreSubscriber<? super T> actual) {
			this.actual = actual;
		}

		/**
		 * 向该对象拉取数据
		 * @param n
		 */
		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				// 普通模式下 才会增加 requested
				if (fusionMode() == NONE) {
					Operators.addCapCancellable(REQUESTED, this, n);
				}
				ReplaySubscriber<T> p = parent;
				if (p != null) {
					// 将请求转发到 buffer 中 该方法内会将数据 转发到 actual 中
					p.buffer.replay(this);
				}
			}
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return parent;
			if (key == Attr.TERMINATED) return parent != null && parent.isTerminated();
			if (key == Attr.BUFFERED) return size();
			if (key == Attr.CANCELLED) return isCancelled();
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return Math.max(0L, requested);
			if (key == Attr.RUN_ON) return parent.parent.scheduler;

			return ReplaySubscription.super.scanUnsafe(key);
		}

		/**
		 * 关闭本对象
		 */
		@Override
		public void cancel() {
			if (REQUESTED.getAndSet(this, Long.MIN_VALUE) != Long.MIN_VALUE) {
				ReplaySubscriber<T> p = parent;
				if (p != null) {
					// 将该对象 从父类的引用移除
					p.remove(this);
				}
				if (enter()) {
					node = null;
				}
			}
		}

		@Override
		public long requested() {
			return requested;
		}

		@Override
		public boolean isCancelled() {
			return requested == Long.MIN_VALUE;
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		public int requestFusion(int requestedMode) {
			if ((requestedMode & ASYNC) != 0) {
				fusionMode = ASYNC;
				return ASYNC;
			}
			return NONE;
		}


		@Override
		@Nullable
		public T poll() {
			ReplaySubscriber<T> p = parent;
			if(p != null){
				return p.buffer.poll(this);
			}
			return null;
		}

		@Override
		public void clear() {
			ReplaySubscriber<T> p = parent;
			if(p != null) {
				p.buffer.clear(this);
			}
		}

		@Override
		public boolean isEmpty() {
			ReplaySubscriber<T> p = parent;
			return p == null || p.buffer.isEmpty(this);
		}

		@Override
		public int size() {
			ReplaySubscriber<T> p = parent;
			if(p != null) {
				return p.buffer.size(this);
			}
			return 0;
		}

		@Override
		public void node(@Nullable Object node) {
			this.node = node;
		}

		@Override
		public int fusionMode() {
			return fusionMode;
		}

		@Override
		@Nullable
		public Object node() {
			return node;
		}

		@Override
		public int index() {
			return index;
		}

		@Override
		public void index(int index) {
			this.index = index;
		}

		@Override
		public int tailIndex() {
			return tailIndex;
		}

		@Override
		public void tailIndex(int tailIndex) {
			this.tailIndex = tailIndex;
		}

		/**
		 * 是否允许访问内部数据
		 * @return
		 */
		@Override
		public boolean enter() {
			return WIP.getAndIncrement(this) == 0;
		}

		@Override
		public int leave(int missed) {
			return WIP.addAndGet(this, -missed);
		}

		/**
		 * 减少请求数
		 * @param n
		 */
		@Override
		public void produced(long n) {
			REQUESTED.addAndGet(this, -n);
		}
	}
}
