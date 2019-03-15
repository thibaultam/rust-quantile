#![feature(test)]
#[cfg(test)]
extern crate test;

// Run benches using nightly: rustup run nightly cargo bench

use ::rand::rngs::StdRng;
use ::rand::{Rng, SeedableRng};

use quantile::{Quantile, Stream};
use test::Bencher;

fn geberate_random_data() -> Vec<f64> {
    let mut data = Vec::with_capacity(10_123);
    let seed = [
        22, 11, 31, 21, 15, 14, 18, 2, 23, 19, 16, 32, 25, 1, 13, 26, 8, 4, 24, 3, 17, 28, 27, 6,
        5, 20, 29, 9, 30, 7, 12, 10,
    ];
    let mut rng: StdRng = SeedableRng::from_seed(seed);
    for _ in 0..10_123 {
        data.push(rng.gen());
    }
    data
}

#[bench]
fn bench_insertion(b: &mut Bencher) {
    let data = geberate_random_data();

    let mut stream = Stream::new(vec![
        Quantile::new(0.1, 0.0001),
        Quantile::new(0.5, 0.01),
        Quantile::new(0.9, 0.005),
        Quantile::new(0.99, 0.0001),
    ]);

    b.iter(|| {
        for v in data.iter() {
            stream.observe(*v);
        }
        stream.query(0.1);
        stream.query(0.5);
    });
}

#[bench]
fn bench_query(b: &mut Bencher) {
    let data = geberate_random_data();

    let mut stream = Stream::new(vec![
        Quantile::new(0.1, 0.0001),
        Quantile::new(0.5, 0.01),
        Quantile::new(0.9, 0.005),
        Quantile::new(0.99, 0.0001),
    ]);

    for v in data.iter() {
        stream.observe(*v);
    }
    // force flush
    stream.query(0.1);

    b.iter(|| {
        stream.query(0.1);
        stream.query(0.5);
        stream.query(0.9);
        stream.query(0.99);
    });
}
