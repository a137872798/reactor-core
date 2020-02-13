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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.scheduler.Schedulers;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * @see <a href="https://github.com/reactor/reactive-streams-commons">https://github.com/reactor/reactive-streams-commons</a>
 * 单订阅者 同时继承于栅栏对象  该对象走的不是 baseSubscriber 这套
 */
abstract class BlockingSingleSubscriber<T> extends CountDownLatch
		implements InnerConsumer<T>, Disposable {

	T         value;
	Throwable error;

	/**
	 * 订阅者 通过该对象与  observable 交互
	 */
	Subscription s;

	/**
	 * 判断该对象是否被关闭
	 */
	volatile boolean cancelled;

	BlockingSingleSubscriber() {
		super(1);
	}

	/**
	 * 在设置subscription 的同时 拉取所有元素
	 * @param s
	 */
	@Override
	public final void onSubscribe(Subscription s) {
		this.s = s;
		if (!cancelled) {
			s.request(Long.MAX_VALUE);
		}
	}

	/**
	 * 当处理完内部所有元素时 解除 阻塞状态
	 */
	@Override
	public final void onComplete() {
		countDown();
	}

	@Override
	public Context currentContext() {
		return Context.empty();
	}

	/**
	 * 丢弃该对象
	 */
	@Override
	public final void dispose() {
		cancelled = true;
		Subscription s = this.s;
		if (s != null) {
			// 优先置空 再关闭 是一个好的技巧 不然 其他线程访问到 s 此时 s 会处在一个不确定的状态
			this.s = null;
			s.cancel();
		}
	}

	/**
	 * Block until the first value arrives and return it, otherwise
	 * return null for an empty source and rethrow any exception.
	 *
	 * @return the first value or null if the source is empty
	 * 阻塞 并等待获取到数据
	 */
	@Nullable
	final T blockingGet() {
		if (Schedulers.isInNonBlockingThread()) {
			throw new IllegalStateException("block()/blockFirst()/blockLast() are blocking, which is not supported in thread " + Thread.currentThread().getName());
		}
		if (getCount() != 0) {
			try {
				await();
			}
			// 该线程被外部唤醒时 触发
			catch (InterruptedException ex) {
				// 丢弃该 subscription
				dispose();
				throw Exceptions.propagate(ex);
			}
		}

		// 判断 获取到元素时 是否发现了异常
		Throwable e = error;
		if (e != null) {
			RuntimeException re = Exceptions.propagate(e);
			//this is ok, as re is always a new non-singleton instance
			re.addSuppressed(new Exception("#block terminated with an error"));
			throw re;
		}
		// 返回获取到的首个值
		return value;
	}

	/**
	 * Block until the first value arrives and return it, otherwise
	 * return null for an empty source and rethrow any exception.
	 *
	 * @param timeout the timeout to wait
	 * @param unit the time unit
	 *
	 * @return the first value or null if the source is empty
	 * 阻塞指定时间
	 */
	@Nullable
	final T blockingGet(long timeout, TimeUnit unit) {
		if (Schedulers.isInNonBlockingThread()) {
			throw new IllegalStateException("block()/blockFirst()/blockLast() are blocking, which is not supported in thread " + Thread.currentThread().getName());
		}
		if (getCount() != 0) {
			try {
				if (!await(timeout, unit)) {
					dispose();
					throw new IllegalStateException("Timeout on blocking read for " + timeout + " " + unit);
				}
			}
			catch (InterruptedException ex) {
				dispose();
				RuntimeException re = Exceptions.propagate(ex);
				//this is ok, as re is always a new non-singleton instance
				re.addSuppressed(new Exception("#block has been interrupted"));
				throw re;
			}
		}

		Throwable e = error;
		if (e != null) {
			RuntimeException re = Exceptions.propagate(e);
			//this is ok, as re is always a new non-singleton instance
			re.addSuppressed(new Exception("#block terminated with an error"));
			throw re;
		}
		return value;
	}


	/**
	 * 通过传入的 状态Key 获取当前状态
	 * @param key a {@link Attr} to resolve for the component.
	 * @return
	 */
	@Override
	@Nullable
	public Object scanUnsafe(Attr key) {
		if (key == Attr.TERMINATED) return getCount() == 0;
		if (key == Attr.PARENT) return  s;
		if (key == Attr.CANCELLED) return cancelled;
		if (key == Attr.ERROR) return error;
		if (key == Attr.PREFETCH) return Integer.MAX_VALUE;

		return null;
	}

	/**
	 * getCount == 0 代表已经获取到元素了 该对象就可以被关闭了
	 *
	 * @return
	 */
	@Override
	public boolean isDisposed() {
		return cancelled || getCount() == 0;
	}
}
