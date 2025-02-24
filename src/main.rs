mod scheduler;

fn main() {
    // println!("Hello, world!");

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(example())
}

async fn example() {
    let a = 1;
    yield_once().await;
    let b = &a;
    yield_once().await;
    println!("{}", b);
}

async fn yield_once() {}


