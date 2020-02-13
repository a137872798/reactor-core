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
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.util.context.Context;

/**
 * A simple base class for a {@link Subscriber} implementation that lets the user
 * perform a {@link #request(long)} and {@link #cancel()} on it directly. As the targeted
 * use case is to manually handle requests, the {@link #hookOnSubscribe(Subscription)} and
 * {@link #hookOnNext(Object)} hooks are expected to be implemented, but they nonetheless
 * default to an unbounded request at subscription time. If you need to define a {@link Context}
 * for this {@link BaseSubscriber}, simply override its {@link #currentContext()} method.
 * <p>
 * Override the other optional hooks {@link #hookOnComplete()},
 * {@link #hookOnError(Throwable)} and {@link #hookOnCancel()}
 * to customize the base behavior. You also have a termination hook,
 * {@link #hookFinally(SignalType)}.
 * <p>
 * Most of the time, exceptions triggered inside hooks are propagated to
 * {@link #onError(Throwable)} (unless there is a fatal exception). The class is in the
 * {@code reactor.core.publisher} package, as this subscriber is tied to a single
 * {@link org.reactivestreams.Publisher}.
 *
 * @author Simon Baslé
 * 订阅者基类
 */
public abstract class BaseSubscriber<T> implements CoreSubscriber<T>, Subscription,
                                                   Disposable {

	/**
	 * 订阅对象 内部会包含一个  subscription  该对象像是一个桥梁 内部包含了 Observable 内部的数据
	 */
	volatile Subscription subscription;

	/**
	 * 以原子方式更新内部的 subscription字段
	 */
	static AtomicReferenceFieldUpdater<BaseSubscriber, Subscription> S =
			AtomicReferenceFieldUpdater.newUpdater(BaseSubscriber.class, Subscription.class, "subscription");

	/**
	 * Return current {@link Subscription}
	 * @return current {@link Subscription}
	 * 获取上游对象 默认就是获取 subscription
	 */
	protected Subscription upstream() {
		return subscription;
	}

	/**
	 * 如果 该 subscription 已经被关闭 那么 isDisposed 返回true
	 * @return
	 */
	@Override
	public boolean isDisposed() {
		return subscription == Operators.cancelledSubscription();
	}

	/**
	 * {@link Disposable#dispose() Dispose} the {@link Subscription} by
	 * {@link Subscription#cancel() cancelling} it.
	 *  订阅者本身实现了 disposable 接口 当调用 dispose 时 就是调用cancel
	 */
	@Override
	public void dispose() {
		cancel();
	}

	/**
	 * Hook for further processing of onSubscribe's Subscription. Implement this method
	 * to call {@link #request(long)} as an initial request. Values other than the
	 * unbounded {@code Long.MAX_VALUE} imply that you'll also call request in
	 * {@link #hookOnNext(Object)}.
	 * <p> Defaults to request unbounded Long.MAX_VALUE as in {@link #requestUnbounded()}
	 *
	 * @param subscription the subscription to optionally process
	 *                     从subscription 中 拉取所有的元素
	 */
	protected void hookOnSubscribe(Subscription subscription){
		subscription.request(Long.MAX_VALUE);
	}

	/**
	 * Hook for processing of onNext values. You can call {@link #request(long)} here
	 * to further request data from the source {@link org.reactivestreams.Publisher} if
	 * the {@link #hookOnSubscribe(Subscription) initial request} wasn't unbounded.
	 * <p>Defaults to doing nothing.
	 *
	 * @param value the emitted value to process
	 *              默认情况下 当subscription 内部的元素 往下游传播时  触发的钩子函数为 NOOP
	 */
	protected void hookOnNext(T value){
		// NO-OP
	}

	/**
	 * Optional hook for completion processing. Defaults to doing nothing.
	 * 当读取到最后一个 元素时触发
	 */
	protected void hookOnComplete() {
		// NO-OP
	}

	/**
	 * Optional hook for error processing. Default is to call
	 * {@link Exceptions#errorCallbackNotImplemented(Throwable)}.
	 *
	 * @param throwable the error to process
	 *                  在执行过程中 如果遇到了异常情况  选择包装异常后抛出
	 */
	protected void hookOnError(Throwable throwable) {
		throw Exceptions.errorCallbackNotImplemented(throwable);
	}

	/**
	 * Optional hook executed when the subscription is cancelled by calling this
	 * Subscriber's {@link #cancel()} method. Defaults to doing nothing.
	 * 当检测到 subscription 被关闭时触发的钩子
	 */
	protected void hookOnCancel() {
		//NO-OP
	}

	/**
	 * Optional hook executed after any of the termination events (onError, onComplete,
	 * cancel). The hook is executed in addition to and after {@link #hookOnError(Throwable)},
	 * {@link #hookOnComplete()} and {@link #hookOnCancel()} hooks, even if these callbacks
	 * fail. Defaults to doing nothing. A failure of the callback will be caught by
	 * {@link Operators#onErrorDropped(Throwable, reactor.util.context.Context)}.
	 *
	 * @param type the type of termination event that triggered the hook
	 * ({@link SignalType#ON_ERROR}, {@link SignalType#ON_COMPLETE} or
	 * {@link SignalType#CANCEL})
	 *             当触发 onError onComplete cancel 后 最终会触发的钩子
	 */
	protected void hookFinally(SignalType type) {
		//NO-OP
	}

	/**
	 * 当调用该方法时 会将subscriber 内部的 subscription 原子更新器 设置为跟定的Subscription
	 * @param s
	 */
	@Override
	public final void onSubscribe(Subscription s) {
		// 设置subscriber 内部的 subscription
		if (Operators.setOnce(S, this, s)) {
			try {
				// 成功时 开始从 subscription 中拉取元素 默认是拉取所有元素 也就是 request(Long.MaxValue)
				hookOnSubscribe(s);
			}
			// 当遇到异常时触发对应钩子
			catch (Throwable throwable) {
				onError(Operators.onOperatorError(s, throwable, currentContext()));
			}
		}
	}

	@Override
	public final void onNext(T value) {
		Objects.requireNonNull(value, "onNext");
		try {
			hookOnNext(value);
		}
		catch (Throwable throwable) {
			// Operators.onOperatorError  使用钩子处理获取到的异常
			// 使用 onError 处理最终版本的异常对象
			onError(Operators.onOperatorError(subscription, throwable, value, currentContext()));
		}
	}

	/**
	 * 处理异常
	 * @param t
	 */
	@Override
	public final void onError(Throwable t) {
		Objects.requireNonNull(t, "onError");

		// 将内部的 subscription 设置成 cancelledSubscription   如果原本就是 cancelledSubscription
		// 代表异常已经被丢弃了
		if (S.getAndSet(this, Operators.cancelledSubscription()) == Operators
				.cancelledSubscription()) {
			//already cancelled concurrently
			Operators.onErrorDropped(t, currentContext());
			return;
		}


		try {
			// 抛出异常
			hookOnError(t);
		}
		catch (Throwable e) {
			// 增加额外的异常信息后 抛出异常
			e = Exceptions.addSuppressed(e, t);
			Operators.onErrorDropped(e, currentContext());
		}
		finally {
			// 触发 hookFinish
			safeHookFinally(SignalType.ON_ERROR);
		}
	}

	/**
	 * 代表在 subscription 层 成功处理完了所有元素
	 */
	@Override
	public final void onComplete() {
		// 将subscription 设置成cancelledSubscription
		if (S.getAndSet(this, Operators.cancelledSubscription()) != Operators
				.cancelledSubscription()) {
			//we're sure it has not been concurrently cancelled
			try {
				// 触发钩子函数
				hookOnComplete();
			}
			catch (Throwable throwable) {
				//onError itself will short-circuit due to the CancelledSubscription being set above
				hookOnError(Operators.onOperatorError(throwable, currentContext()));
			}
			finally {
				// 触发 hookFinish 函数
				safeHookFinally(SignalType.ON_COMPLETE);
			}
		}
	}

	/**
	 * 手动触发 subscription.request  原本在 设置subscription 时就会被动触发  request
	 * @param n
	 */
	@Override
	public final void request(long n) {
		// 校验数字是否正常 (不能为负数)
		if (Operators.validate(n)) {
			Subscription s = this.subscription;
			if (s != null) {
				s.request(n);
			}
		}
	}

	/**
	 * {@link #request(long) Request} an unbounded amount.
	 * 尽可能的获取元素 也就是 n = Long.Max_Value
	 */
	public final void requestUnbounded() {
		request(Long.MAX_VALUE);
	}

	/**
	 * 关闭 subscriber 对象
	 */
	@Override
	public final void cancel() {
		if (Operators.terminate(S, this)) {
			try {
				// 如果 subscription 已经是 CancelledSubscription 不会触发该钩子
				hookOnCancel();
			}
			catch (Throwable throwable) {
				hookOnError(Operators.onOperatorError(subscription, throwable, currentContext()));
			}
			finally {
				safeHookFinally(SignalType.CANCEL);
			}
		}
	}

	void safeHookFinally(SignalType type) {
		try {
			hookFinally(type);
		}
		catch (Throwable finallyFailure) {
			Operators.onErrorDropped(finallyFailure, currentContext());
		}
	}

	@Override
	public String toString() {
		return getClass().getSimpleName();
	}
}
