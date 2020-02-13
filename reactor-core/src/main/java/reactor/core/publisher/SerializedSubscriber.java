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


import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.util.annotation.Nullable;

/**
 * Subscriber that makes sure signals are delivered sequentially in case the onNext, onError or onComplete methods are
 * called concurrently.
 * <p>
 * <p>
 * The implementation uses {@code synchronized (this)} to ensure mutual exclusion.
 * <p>
 * <p>
 * Note that the class implements Subscription to save on allocation.
 *
 * @param <T> the value type
 *           确保在并发环境中时下发到下游的元素是串行的
 */
final class SerializedSubscriber<T> implements InnerOperator<T, T> {

	final CoreSubscriber<? super T> actual;

	boolean emitting;

	boolean missed;

	volatile boolean done;

	volatile boolean cancelled;

	LinkedArrayNode<T> head;

	LinkedArrayNode<T> tail;

	Throwable error;

	Subscription s;

	SerializedSubscriber(CoreSubscriber<? super T> actual) {
		this.actual = actual;
	}

	/**
	 * 为该对象设置数据源
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
	 * 下发数据
	 * @param t
	 */
	@Override
	public void onNext(T t) {
		if (cancelled || done) {
			return;
		}

		synchronized (this) {
			if (cancelled || done) {
				return;
			}

			// 在并发环境 先将元素存储到一个队列中 而不是急着下发  因为在 synchronized 所以不需要使用并发队列
			if (emitting) {
				serAdd(t);
				missed = true;
				return;
			}

			// 代表当前有个线程正在下发数据
			emitting = true;
		}

		actual.onNext(t);

		// 按照串行的方式 挨个下发元素
		serDrainLoop(actual);
	}

	@Override
	public void onError(Throwable t) {
		if (cancelled || done) {
			return;
		}

		synchronized (this) {
			if (cancelled || done) {
				return;
			}

			done = true;
			error = t;

			if (emitting) {
				missed = true;
				return;
			}
		}

		actual.onError(t);
	}

	@Override
	public void onComplete() {
		if (cancelled || done) {
			return;
		}

		synchronized (this) {
			if (cancelled || done) {
				return;
			}

			done = true;

			if (emitting) {
				missed = true;
				return;
			}
		}

		actual.onComplete();
	}

	/**
	 * 向上游拉取数据
	 * @param n
	 */
	@Override
	public void request(long n) {
		s.request(n);
	}

	@Override
	public void cancel() {
		cancelled = true;
		s.cancel();
	}

	/**
	 * 将元素暂存到一个队列中
	 * @param value
	 */
	void serAdd(T value) {
		// 看来是 数组+ 链表的结构
		LinkedArrayNode<T> t = tail;

		if (t == null) {
			t = new LinkedArrayNode<>(value);

			head = t;
			tail = t;
		}
		else {
			// 代表达到数组长度上限 需要增加一个新节点
			if (t.count == LinkedArrayNode.DEFAULT_CAPACITY) {
				LinkedArrayNode<T> n = new LinkedArrayNode<>(value);

				t.next = n;
				tail = n ;
			}
			// 追加到链表节点的数组中
			else {
				t.array[t.count++] = value;
			}
		}
	}

	/**
	 * 将链表结构内的数据挨个下发
	 * @param actual
	 */
	void serDrainLoop(CoreSubscriber<? super T> actual) {
		for (; ; ) {

			if (cancelled) {
				return;
			}

			boolean d;
			Throwable e;
			LinkedArrayNode<T> n;

			synchronized (this) {
				if (cancelled) {
					return;
				}

				if (!missed) {
					emitting = false;
					return;
				}

				missed = false;

				d = done;
				e = error;
				n = head;

				head = null;
				tail = null;
			}

			while (n != null) {

				T[] arr = n.array;
				int c = n.count;

				for (int i = 0; i < c; i++) {

					if (cancelled) {
						return;
					}

					actual.onNext(arr[i]);
				}

				n = n.next;
			}

			if (cancelled) {
				return;
			}

			if (e != null) {
				actual.onError(e);
				return;
			}
			else if (d) {
				actual.onComplete();
				return;
			}
		}
	}

	@Override
	public CoreSubscriber<? super T> actual() {
		return actual;
	}

	@Override
	@Nullable
	public Object scanUnsafe(Attr key) {
		if (key == Attr.PARENT) return s;
		if (key == Attr.ERROR) return error;
		if (key == Attr.BUFFERED) return producerCapacity();
		if (key == Attr.CAPACITY) return LinkedArrayNode.DEFAULT_CAPACITY;
		if (key == Attr.CANCELLED) return cancelled;
		if (key == Attr.TERMINATED) return done;

		return InnerOperator.super.scanUnsafe(key);
	}

	int producerCapacity() {
		LinkedArrayNode<T> node = tail;
		if(node != null){
			return node.count;
		}
		return 0;
	}

	/**
	 * Node in a linked array list that is only appended.
	 *
	 * @param <T> the value type
	 *           该对象是一个链表 + 数组的结构
	 */
	static final class LinkedArrayNode<T> {

		static final int DEFAULT_CAPACITY = 16;

		final T[] array;
		int count;

		LinkedArrayNode<T> next;

		@SuppressWarnings("unchecked")
		LinkedArrayNode(T value) {
			array = (T[]) new Object[DEFAULT_CAPACITY];
			array[0] = value;
			count = 1;
		}
	}
}
