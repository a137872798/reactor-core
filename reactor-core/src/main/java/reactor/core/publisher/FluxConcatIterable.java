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

import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.reactivestreams.Publisher;
import reactor.core.CoreSubscriber;

/**
 * Concatenates a fixed array of Publishers' values.
 *
 * @param <T> the value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 * 将多个 pub 整合成一个 flux
 */
final class FluxConcatIterable<T> extends Flux<T> implements SourceProducer<T> {

	/**
	 * 内部包含的多个 pub
	 */
	final Iterable<? extends Publisher<? extends T>> iterable;

	FluxConcatIterable(Iterable<? extends Publisher<? extends T>> iterable) {
		this.iterable = Objects.requireNonNull(iterable, "iterable");
	}

	/**
	 * 当为该对象设置订阅者时
	 * @param actual the {@link Subscriber} interested into the published sequence
	 */
	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {

		Iterator<? extends Publisher<? extends T>> it;

		try {
			it = Objects.requireNonNull(iterable.iterator(),
					"The Iterator returned is null");
		}
		catch (Throwable e) {
			Operators.error(actual, Operators.onOperatorError(e, actual.currentContext()));
			return;
		}

		ConcatIterableSubscriber<T> parent = new ConcatIterableSubscriber<>(actual, it);

		// 这里 先设置 request 数量
		actual.onSubscribe(parent);

		if (!parent.isCancelled()) {
			// 这里的 onComplete 会 间接的 遍历迭代器中的 对象 并触发到上游的 onSubscribe 方法 进而在触发到该对象的 onNext onComplete
			// 因为在上一步已经设置了 request 就可以立即拉取数据到下游了
			parent.onComplete();
		}
	}

	@Override
	public Object scanUnsafe(Attr key) {
		return null; //no particular key to be represented, still useful in hooks
	}

	/**
	 * 该对象 对应一个 订阅者 和 一组 发布者
	 * @param <T>
	 */
	static final class ConcatIterableSubscriber<T>
			extends Operators.MultiSubscriptionSubscriber<T, T> {

		/**
		 * 当 上游的某个 数据处理完后 会挨个将迭代器 中所有元素发送到下游
		 */
		final Iterator<? extends Publisher<? extends T>> it;

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<ConcatIterableSubscriber> WIP =
				AtomicIntegerFieldUpdater.newUpdater(ConcatIterableSubscriber.class,
						"wip");

		long produced;

		ConcatIterableSubscriber(CoreSubscriber<? super T> actual,
				Iterator<? extends Publisher<? extends T>> it) {
			super(actual);
			this.it = it;
		}

		/**
		 * 该对象的父类 包含一个 onSubscribe 方法 触发后 subscription 会调用该方法  实际上该对象可以看作是一个代理对象
		 * @param t
		 */
		@Override
		public void onNext(T t) {
			produced++;

			actual.onNext(t);
		}

		/**
		 * 当上游的 subscription 发送完所有数据时 触发该方法
		 */
		@Override
		public void onComplete() {
			if (WIP.getAndIncrement(this) == 0) {
				Iterator<? extends Publisher<? extends T>> a = this.it;
				do {
					if (isCancelled()) {
						return;
					}

					boolean b;

					try {
						b = a.hasNext();
					}
					catch (Throwable e) {
						onError(Operators.onOperatorError(this, e,
								actual.currentContext()));
						return;
					}

					if (isCancelled()) {
						return;
					}

					if (!b) {
						actual.onComplete();
						return;
					}

					Publisher<? extends T> p;

					try {
						p = Objects.requireNonNull(it.next(),
								"The Publisher returned by the iterator is null");
					}
					catch (Throwable e) {
						actual.onError(Operators.onOperatorError(this, e,
								actual.currentContext()));
						return;
					}

					if (isCancelled()) {
						return;
					}

					long c = produced;
					if (c != 0L) {
						produced = 0L;
						produced(c);
					}

					// 当上游的 某个 subscription 触发 onComplete 时 会调用该方法  遍历内部的迭代器 得到一个元素后 继续触发本对象的 onSubscribe
					// 这样 就慢慢将所有pub 的数据都发送到下游了
					p.subscribe(this);

					if (isCancelled()) {
						return;
					}

				}
				while (WIP.decrementAndGet(this) != 0);
			}

		}
	}
}
