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

import org.reactivestreams.Subscriber;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.util.annotation.Nullable;

/**
 * A Stream that emits only one value and then complete.
 * <p>
 * Since the flux retains the value in a final field, any
 * {@link this#subscribe(Subscriber)} will
 * replay the value. This is a "Cold" fluxion.
 * <p>
 * Create such flux with the provided factory, E.g.:
 * <pre>
 * {@code
 * Flux.just(1).subscribe(
 *    log::info,
 *    log::error,
 *    ()-> log.info("complete")
 * )
 * }
 * </pre>
 * Will log:
 * <pre>
 * {@code
 * 1
 * complete
 * }
 * </pre>
 *
 * @author Stephane Maldini
 * 该flux 只包含一个对象
 */
final class FluxJust<T> extends Flux<T>
		implements Fuseable.ScalarCallable<T>, Fuseable,
		           SourceProducer<T> {

	/**
	 * 内部数据源
	 */
	final T value;

	FluxJust(T value) {
		this.value = Objects.requireNonNull(value, "value");
	}

	/**
	 * 调用 call 返回内部数据实体
	 * @return
	 * @throws Exception
	 */
	@Override
	public T call() throws Exception {
		return value;
	}

	/**
	 * 当为该数据源追加订阅者时触发
	 * @param actual the {@link Subscriber} interested into the published sequence
	 */
	@Override
	public void subscribe(final CoreSubscriber<? super T> actual) {
		// 套路就是 将订阅者与数据源包装成一个 Subscription 之后触发 onSubscribe  这里面又会触发 subscription.request
		actual.onSubscribe(new WeakScalarSubscription<>(value, actual));
	}

	/**
	 * 继承自 Scannable 接口 通过某个Key 查询指标数据
	 * @param key
	 * @return
	 */
	@Override
	public Object scanUnsafe(Attr key) {
		// 返回背压队列的长度??? 默认为1
		if (key == Attr.BUFFERED) return 1;
		return null;
	}

	/**
	 * 针对 Flux.just(T t).subscribe()  后 创建的 subscription 对象
	 * @param <T>
	 */
	static final class WeakScalarSubscription<T> implements QueueSubscription<T>,
	                                                        InnerProducer<T>{

		/**
		 * 代表已经订阅过数据了  设置该标识就代表该数据流只能处理一次
		 */
		boolean terminado;
		/**
		 * 数据源
		 */
		final T                     value;
		/**
		 * 能被包装的订阅者
		 */
		final CoreSubscriber<? super T> actual;

		WeakScalarSubscription(@Nullable T value, CoreSubscriber<? super T> actual) {
			this.value = value;
			this.actual = actual;
		}

		/**
		 * 订阅者 会触发 Subscription.request()  并开始拉取数据
		 * @param elements
		 */
		@Override
		public void request(long elements) {
			if (terminado) {
				return;
			}

			// 不是 volatile 修饰
			// 修改为 true 代表已经消费过数据源
			terminado = true;
			if (value != null) {
				actual.onNext(value);
			}
			actual.onComplete();
		}

		/**
		 * 关闭该对象就是直接设置标识 这样调用request 也不会触发 onNext
		 */
		@Override
		public void cancel() {
			terminado = true;
		}

		/**
		 * 代表该对象仅支持 同步调用
		 * @param requestedMode the mode requested by the intermediate operator
		 * @return
		 */
		@Override
		public int requestFusion(int requestedMode) {
			if ((requestedMode & Fuseable.SYNC) != 0) {
				return Fuseable.SYNC;
			}
			return 0;
		}

		/**
		 * 用于结合 2个 flux 的数据时会调用该方法
		 * @return
		 */
		@Override
		@Nullable
		public T poll() {
			if (!terminado) {
				terminado = true;
				return value;
			}
			return null;
		}

		@Override
		public boolean isEmpty() {
			return terminado;
		}

		@Override
		public int size() {
			return isEmpty() ? 0 : 1;
		}

		@Override
		public void clear() {
			terminado = true;
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.TERMINATED || key == Attr.CANCELLED) return terminado;

			return InnerProducer.super.scanUnsafe(key);
		}
	}
}
