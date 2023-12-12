use std::fmt::Write;

use arrow_wasm_udf::function;

// test simd with no arguments
#[function("zero() -> int")]
fn zero() -> i32 {
    0
}

// test simd with 1 arguments
#[function("double(int) -> int")]
fn double(x: i32) -> i32 {
    x * 2
}

// test simd with 2 arguments
#[function("gcd(int, int) -> int")]
fn gcd(mut a: i32, mut b: i32) -> i32 {
    while b != 0 {
        let t = b;
        b = a % b;
        a = t;
    }
    a
}

#[function("count(varchar) -> int")]
fn count(s: &str) -> i32 {
    s.len() as i32
}

// #[function("to_string(int) -> varchar")]
// fn to_string(x: i32, output: &mut impl Write) {
//     write!(output, "{}", x).unwrap();
// }
