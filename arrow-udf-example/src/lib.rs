use arrow_udf::function;

#[function("gcd(int, int) -> int")]
fn gcd(mut a: i32, mut b: i32) -> i32 {
    while b != 0 {
        let t = b;
        b = a % b;
        a = t;
    }
    a
}

#[function("div(int, int) -> int")]
fn div(x: i32, y: i32) -> Result<i32, &'static str> {
    if y == 0 {
        return Err("division by zero");
    }
    Ok(x / y)
}

#[function("segfault()")]
fn segfault() {
    unsafe { (usize::MAX as *const i32).read_volatile() };
}

#[function("oom()")]
fn oom() {
    _ = vec![0u8; usize::MAX];
}

#[function("create_file()")]
fn create_file() {
    std::fs::File::create("test").unwrap();
}

// sleep is allowed for now
#[function("sleep(int) -> int")]
fn sleep(second: i32) -> i32 {
    std::thread::sleep(std::time::Duration::from_secs(second as u64));
    0
}

#[function("length(varchar) -> int")]
#[function("length(bytea) -> int")]
fn length(s: impl AsRef<[u8]>) -> i32 {
    s.as_ref().len() as i32
}

#[function("key_value(varchar) -> struct<key:varchar, value:varchar>")]
fn key_value(kv: &str) -> Option<(&str, &str)> {
    kv.split_once('=')
}
