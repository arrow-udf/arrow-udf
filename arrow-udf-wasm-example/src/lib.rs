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

#[function("segfault() -> int")]
fn segfault() -> i32 {
    unsafe { (usize::MAX as *const i32).read_volatile() }
}

#[function("oom() -> int")]
fn oom() -> i32 {
    _ = vec![0u8; usize::MAX];
    0
}

#[function("create_file() -> int")]
fn create_file() -> i32 {
    std::fs::File::create("test").unwrap();
    0
}

#[function("length(varchar) -> int")]
#[function("length(bytea) -> int")]
fn length(s: impl AsRef<[u8]>) -> i32 {
    s.as_ref().len() as i32
}
