use crate::example;
use rand::random;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, VecDeque};
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use std::thread;
use std::thread::sleep;
use std::time::Duration;
use tokio::time::Instant;

struct BatchScheduler {
    tasks: Vec<Pin<Box<dyn Future<Output = ()> + Send + 'static>>>,
    batch_size: usize,
}

impl BatchScheduler {
    fn new(batch_size: usize) -> Self {
        BatchScheduler {
            tasks: Vec::new(),
            batch_size,
        }
    }

    fn run(&mut self) {
        // noop waker vtable
        const NOOP_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
            |_| RawWaker::new(std::ptr::null(), &NOOP_WAKER_VTABLE),
            |_| {},
            |_| {},
            |_| {},
        );

        let waker = unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &NOOP_WAKER_VTABLE)) };

        let mut ctx = Context::from_waker(&waker);
        let mut current_batch = Vec::with_capacity(self.batch_size);

        for task in &mut self.tasks {
            if let Poll::Ready(()) = task.as_mut().poll(&mut ctx) {
                continue;
            }

            current_batch.push(task);
            if current_batch.len() >= self.batch_size {
                for t in &mut current_batch {
                    let _ = t.as_mut().poll(&mut ctx);
                }

                current_batch.clear();
            }
        }
    }
}

#[test]
fn test_batch_scheduler() {
    let mut scheduler = BatchScheduler {
        batch_size: 2,
        tasks: vec![
            Box::pin(async {
                println!("task 1 starting");
                sleep(Duration::from_secs(1));
                println!("task 1 finished");
            }),
            Box::pin(async {
                println!("task 2 starting");
                sleep(Duration::from_secs(2));
                println!("task 2 finished");
            }),
            Box::pin(async {
                println!("task 3 starting");
                sleep(Duration::from_secs(3));
                println!("task 3 finished");
            }),
        ],
    };

    scheduler.run();
}

struct Delay {
    end_time: Instant,
}

impl Future for Delay {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if Instant::now() >= unsafe { self.get_unchecked_mut() }.end_time {
            Poll::Ready(())
        } else {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

struct Retry<F, O, E, Fut> {
    operation: F,
    max_retries: usize,
    current_attempt: usize,
    delay: Option<Pin<Box<Delay>>>,
    operation_future: Option<Pin<Box<Fut>>>,
    last_error: Option<E>,
    _phantom: std::marker::PhantomData<(O, Fut)>,
}

impl<F, O, E, Fut> Future for Retry<F, O, E, Fut>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<O, E>>,
{
    type Output = Result<O, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        if this.current_attempt > this.max_retries {
            return Poll::Ready(Err(this.last_error.take().unwrap()));
        }

        if let Some(delay) = this.delay.as_mut() {
            match delay.as_mut().poll(cx) {
                Poll::Pending => {
                    // 3. Wait for said delay.
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                // 4. Otherwise, retry.
                Poll::Ready(_) => {
                    println!("retry attempt {} after backoff", this.current_attempt);
                    this.delay = None;
                }
            }
        }

        if this.operation_future.is_none() {
            this.operation_future = Some(Box::pin((this.operation)()));
        }

        if let Some(mut operation) = this.operation_future.take() {
            let b = operation.as_mut().poll(cx);
            match b {
                Poll::Ready(Ok(val)) => {
                    println!("op succeeded!");
                    return Poll::Ready(Ok(val));
                }
                Poll::Ready(Err(e)) if this.current_attempt <= this.max_retries => {
                    this.current_attempt += 1;
                    let delay_ms = 2u64.pow(this.current_attempt as u32) * 100;
                    println!("op failed. retrying in {}ms...", delay_ms);

                    this.last_error = Some(e);
                    this.delay = Some(Box::pin(Delay {
                        end_time: Instant::now() + Duration::from_millis(delay_ms),
                    }));

                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                Poll::Ready(Err(e)) => {
                    return Poll::Ready(Err(e));
                }

                Poll::Pending => {
                    this.operation_future = Some(operation);
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
            }
        }

        Poll::Pending
    }
}

#[derive(Debug)]
struct RetryOutputError {
    message: String,
}

#[test]
fn test_retry_output() {
    let retry = Retry {
        operation: || async {
            let delay = Delay {
                end_time: Instant::now() + Duration::from_millis(100),
            };

            delay.await;
            let random_val = random::<u8>() % 10;
            match random_val {
                0..=2 => Ok("passed"),
                _ => Err(RetryOutputError {
                    message: "transient error".into(),
                }),
            }
        },

        max_retries: 3,
        current_attempt: 0,
        delay: None,
        operation_future: None,
        last_error: None,
        _phantom: PhantomData,
    };

    println!("starting op with max 3 retries...");
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            match retry.await {
                Ok(res) => println!("outcome: op succeeded: {}", res),
                Err(e) => println!("abort due to: op failed after all retries: {:?}", e),
            }
        });
}

struct RecursiveTask {
    depth: usize,
    child: Option<Pin<Box<RecursiveTask>>>,
}

impl Future for RecursiveTask {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        println!("recursing at depth: {}", self.depth);
        let this = unsafe { self.get_unchecked_mut() };

        if this.depth == 0 {
            return Poll::Ready(());
        }

        if this.child.is_none() {
            this.child = Some(Box::pin(RecursiveTask {
                depth: this.depth - 1,
                child: None,
            }));
        }

        match this.child.as_mut().unwrap().as_mut().poll(cx) {
            Poll::Ready(()) => Poll::Ready(()),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[test]
fn test_recursive_task() {
    const NOOP_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
        |_| RawWaker::new(std::ptr::null(), &NOOP_WAKER_VTABLE),
        |_| {},
        |_| {},
        |_| {},
    );

    let waker = unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &NOOP_WAKER_VTABLE)) };
    let mut ctx = Context::from_waker(&waker);

    let mut task = Pin::new(Box::new(RecursiveTask {
        depth: 5,
        child: None,
    }));

    loop {
        match task.as_mut().poll(&mut ctx) {
            Poll::Ready(_) => {
                println!("recursive task completed!");
                break;
            }

            Poll::Pending => {
                continue;
            }
        }
    }
}

#[derive(Clone)]
struct PriorityWaker {
    priority: u8,
    waker: Waker,
}

impl PartialEq for PriorityWaker {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority
    }
}

impl Eq for PriorityWaker {}

impl PartialOrd for PriorityWaker {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.priority.cmp(&other.priority))
    }
}

impl Ord for PriorityWaker {
    fn cmp(&self, other: &Self) -> Ordering {
        self.priority.cmp(&other.priority)
    }
}

struct PriorityMutex<T> {
    data: Mutex<(T, BinaryHeap<PriorityWaker>)>,
}

impl<T> PriorityMutex<T> {
    fn new(value: T) -> Self {
        PriorityMutex {
            data: Mutex::new((value, BinaryHeap::new())),
        }
    }

    fn lock(&self, priority: u8) -> impl Future<Output = PriorityMutexGuard<'_, T>> {
        struct LockFuture<'a, T> {
            mutex: &'a PriorityMutex<T>,
            priority: u8,
        }

        impl<'a, T> Future for LockFuture<'a, T> {
            type Output = PriorityMutexGuard<'a, T>;

            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                let mut guard = self.mutex.data.lock().unwrap();

                if guard.1.is_empty() {
                    // 1. Lock acquired, store priority for future contention.
                    guard.1.push(PriorityWaker {
                        priority: self.priority,
                        waker: cx.waker().clone(),
                    });

                    Poll::Ready(PriorityMutexGuard {
                        guard: Some(guard),
                        mutex: self.mutex,
                    })
                } else {
                    // 2. Yield and re-insert with priority.
                    guard.1.push(PriorityWaker {
                        priority: self.priority,
                        waker: cx.waker().clone(),
                    });

                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }
        }

        LockFuture {
            mutex: self,
            priority,
        }
    }
}

struct PriorityMutexGuard<'a, T: 'a> {
    guard: Option<MutexGuard<'a, (T, BinaryHeap<PriorityWaker>)>>,
    mutex: &'a PriorityMutex<T>,
}

impl<'a, T> Deref for PriorityMutexGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.guard.as_ref().unwrap().0
    }
}

impl<'a, T> DerefMut for PriorityMutexGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard.as_mut().unwrap().0
    }
}

impl<'a, T> Drop for PriorityMutexGuard<'a, T> {
    fn drop(&mut self) {
        if let Some(mut guard) = self.guard.take() {
            if let Some(priority_waker) = guard.1.pop() {
                priority_waker.waker.wake()
            }
        }
    }
}

#[test]
fn test_priority_mutex() {
    let mutex = Arc::new(PriorityMutex::new(0));

    let mutex1 = mutex.clone();
    let mutex2 = mutex.clone();

    let handle1 = thread::spawn(move || {
        const NOOP_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
            |_| RawWaker::new(std::ptr::null(), &NOOP_WAKER_VTABLE),
            |_| {},
            |_| {},
            |_| {},
        );

        let waker = unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &NOOP_WAKER_VTABLE)) };

        let mut context = Context::from_waker(&waker);
        let mut lock_future = mutex1.lock(2);
        let mut pinned_future = unsafe { Pin::new_unchecked(&mut lock_future) };

        match pinned_future.as_mut().poll(&mut context) {
            Poll::Ready(mut val) => {
                println!("task 1 acquired lock with priority 2");
                *val += 1;
                thread::sleep(Duration::from_millis(100));
                println!("task 1 releasing lock");
            }
            Poll::Pending => {
                println!("task 1 could not acquire lock")
            }
        }
    });

    let handle2 = thread::spawn(move || {
        const NOOP_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
            |_| RawWaker::new(std::ptr::null(), &NOOP_WAKER_VTABLE),
            |_| {},
            |_| {},
            |_| {},
        );

        let waker = unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &NOOP_WAKER_VTABLE)) };
        let mut ctx = Context::from_waker(&waker);

        let mut lock_future = mutex2.lock(1);
        let mut pinned_future = unsafe { Pin::new_unchecked(&mut lock_future) };

        match pinned_future.as_mut().poll(&mut ctx) {
            Poll::Ready(mut value) => {
                println!("task 2 acquired lock with priority 1");
                *value += 2;
                println!("task 2 releasing lock");
            }

            Poll::Pending => println!("task 2 could not acquire lock"),
        }
    });

    handle1.join().unwrap();
    handle2.join().unwrap();
}

struct LocalQueue {
    tasks: VecDeque<Pin<Box<dyn Future<Output = ()> + Send>>>,
    steal_counter: AtomicUsize,
}

impl LocalQueue {
    fn new() -> Self {
        Self {
            tasks: VecDeque::new(),
            steal_counter: AtomicUsize::new(0),
        }
    }

    fn push(&mut self, task: Pin<Box<dyn Future<Output = ()> + Send>>) {
        self.tasks.push_back(task)
    }

    fn steal(&mut self, target: &mut LocalQueue) -> usize {
        let steal_count = self.steal_counter.load(Relaxed);
        let len = self.tasks.len();

        if len > 1 {
            let split = len >> 1;
            let stolen_tasks = self.tasks.drain(..split).collect::<Vec<_>>();

            target.tasks.extend(stolen_tasks);
            self.steal_counter.store(steal_count + 1, Relaxed);
        }

        steal_count
    }
}

fn executor_loop(queue: &mut LocalQueue) {
    const NOOP_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
        |_| RawWaker::new(std::ptr::null(), &NOOP_WAKER_VTABLE),
        |_| {},
        |_| {},
        |_| {},
    );

    let waker = unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &NOOP_WAKER_VTABLE)) };
    let mut ctx = Context::from_waker(&waker);

    while let Some(mut task) = queue.tasks.pop_front() {
        if task.as_mut().poll(&mut ctx) == Poll::Pending {
            queue.tasks.push_back(task);
        }
    }
}

async fn test_task(id: usize) {
    println!("task {} started", id);
    thread::sleep(Duration::from_millis(100 * id as u64));
    println!("task {} completed", id);
}

#[test]
fn test_local_queue() {
    let mut queue = LocalQueue::new();
    for i in 0..5 {
        let task = Box::pin(test_task(i));
        queue.push(task);
    }

    let mut steal_queue = LocalQueue::new();
    println!(
        "before stealing: main queue size = {}, steal queue size = {}",
        queue.tasks.len(),
        steal_queue.tasks.len()
    );

    queue.steal(&mut steal_queue);
    println!(
        "after stealing: main queue size = {}, steal queue size = {}",
        queue.tasks.len(),
        steal_queue.tasks.len()
    );

    executor_loop(&mut queue);
    executor_loop(&mut steal_queue);
}
