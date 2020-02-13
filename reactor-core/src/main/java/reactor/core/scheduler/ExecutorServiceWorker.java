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

package reactor.core.scheduler;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.Scannable;

/**
 * @author Stephane Maldini
 * 通过调度器来创建
 */
final class ExecutorServiceWorker implements Scheduler.Worker, Disposable, Scannable {

	/**
	 * 执行者
	 */
	final ScheduledExecutorService exec;

	/**
	 * 该接口代表一组任务  默认情况下内部为空 需要从外部不断添加
	 */
	final Composite tasks;


	ExecutorServiceWorker(ScheduledExecutorService exec) {
		this.exec = exec;
		this.tasks = Disposables.composite();
	}

	/**
	 * 执行某个任务  同时会添加到 tasks 中 便于维护
	 * @param task the task to schedule
	 * @return
	 */
	@Override
	public Disposable schedule(Runnable task) {
		return Schedulers.workerSchedule(exec, tasks, task, 0L, TimeUnit.MILLISECONDS);
	}

	@Override
	public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
		return Schedulers.workerSchedule(exec, tasks, task, delay, unit);
	}

	@Override
	public Disposable schedulePeriodically(Runnable task,
			long initialDelay,
			long period,
			TimeUnit unit) {
		return Schedulers.workerSchedulePeriodically(exec,
				tasks,
				task,
				initialDelay,
				period,
				unit);
	}

	@Override
	public void dispose() {
		tasks.dispose();
	}

	@Override
	public boolean isDisposed() {
		return tasks.isDisposed();
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.BUFFERED) return tasks.size();
		if (key == Attr.TERMINATED || key == Attr.CANCELLED) return isDisposed();
		if (key == Attr.NAME) return "ExecutorServiceWorker"; //could be parallel, single or fromExecutorService

		return Schedulers.scanExecutor(exec, key);
	}
}
