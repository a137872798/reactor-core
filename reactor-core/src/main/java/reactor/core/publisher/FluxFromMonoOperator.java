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

import org.reactivestreams.Publisher;
import reactor.core.CorePublisher;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;

/**
 * A decorating {@link Flux} {@link Publisher} that exposes {@link Flux} API over an
 * arbitrary {@link Publisher}. Useful to create operators which return a {@link Flux}.
 *
 * @param <I> delegate {@link Publisher} type
 * @param <O> produced type
 *           用于适配 Mono , Flux
 */
abstract class FluxFromMonoOperator<I, O> extends Flux<O> implements Scannable,
                                                                     OptimizableOperator<O, I> {

	/**
	 * 该对象内部实际上是一个 Mono 对象  flux相关api 都由该对象实现
	 */
	protected final Mono<? extends I> source;

	@Nullable
	final OptimizableOperator<?, I> optimizableOperator;

	/**
	 * Build a {@link FluxFromMonoOperator} wrapper around the passed parent {@link Publisher}
	 *
	 * @param source the {@link Publisher} to decorate
	 */
	protected FluxFromMonoOperator(Mono<? extends I> source) {
		this.source = Objects.requireNonNull(source);
		// 如果 Mono 实现 OptimizableOperator 设置 optimizableOperator 属性
		// 很可能是一个嵌套结构 比如 Mono 也是一个 OptimizableOperator 它内部又有一个 source
		this.optimizableOperator = source instanceof OptimizableOperator ? (OptimizableOperator) source : null;
	}

	@Override
	@Nullable
	public Object scanUnsafe(Attr key) {
		if (key == Attr.PREFETCH) return getPrefetch();
		if (key == Attr.PARENT) return source;
		return null;
	}


	/**
	 * 核心方法  为该适配对象设置 订阅者时
	 * @param subscriber the {@link Subscriber} interested into the published sequence
	 */
	@Override
	@SuppressWarnings("unchecked")
	public final void subscribe(CoreSubscriber<? super O> subscriber) {
		OptimizableOperator operator = this;
		while (true) {
			subscriber = operator.subscribeOrReturn(subscriber);
			if (subscriber == null) {
				// null means "I will subscribe myself", returning...
				return;
			}
			// 尝试获取一个更优的对象 并进行订阅 如果没有的话 则使用source订阅
			OptimizableOperator newSource = operator.nextOptimizableSource();
			if (newSource == null) {
				operator.source().subscribe(subscriber);
				return;
			}
			operator = newSource;
		}
	}

	@Override
	@Nullable
	public abstract CoreSubscriber<? super I> subscribeOrReturn(CoreSubscriber<? super O> actual);

	@Override
	public final CorePublisher<? extends I> source() {
		return source;
	}

	@Override
	public final OptimizableOperator<?, ? extends I> nextOptimizableSource() {
		return optimizableOperator;
	}

}
