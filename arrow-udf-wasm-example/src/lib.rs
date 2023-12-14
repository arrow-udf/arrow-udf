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

#[function("length(varchar) -> int")]
#[function("length(bytea) -> int")]
fn length(s: impl AsRef<[u8]>) -> i32 {
    s.as_ref().len() as i32
}
