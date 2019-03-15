// Copyright 2019 thibaultam (lathib2@gmail.com)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

/*!
This crate computes approximate quantiles over an unbounded data stream with
a predefined error margin.

It implements the algorithm `Effective computation of biased quantiles over data streams`
by Cormode, Korn, Muthukrishnan and Srivastava. With this algorithm a bit of precision
is traded for a better memory usage.

# Usage

```rust
use quantile::{Stream, Quantile};

// Track the 50th quantile with a precision of 0.5%
// and the 90th quantile with a precision of 0.5%.
let mut stream = Stream::new(vec![
    Quantile::new(0.5, 0.005),
    Quantile::new(0.9, 0.005),
]);

for i in 1..101 {
    // Observe a value.
    stream.observe(i as f64);
}

assert_eq!(50.0_f64, stream.query(0.5));
assert_eq!(90.0_f64, stream.query(0.9));
```

# Thread safety

This implementation is NOT thread safe by design. To make it thread safe you must lock
before using the public methods of the [`Stream`](::Stream).
!*/

use std::fmt;

/// The quantile that needs to be computed and its associated error margin,
/// such that the value returned is be at a rank ±epsilon
/// percent around the exact value.
pub struct Quantile {
    value: f64,
    error: f64,

    // Pre computed values used in the invariant.
    u: f64,
    v: f64,
}

impl Quantile {
    /// Creates a new `Quantile` to track and its associated error.
    ///
    /// # panic
    ///
    /// Panics if `value < 0`, `value > 1`, `error < 0` or `error > 1`.
    pub fn new(value: f64, error: f64) -> Quantile {
        assert!(value >= 0.0, "quantile value should be >= 0: {}", value);
        assert!(value <= 1.0, "quantile value should be <= 1: {}", value);
        assert!(error >= 0.0, "quantile error should be >= 0: {}", error);
        assert!(error <= 1.0, "quantile error should be <= 0: {}", error);

        let u = 2.0 * error / value;
        let v = 2.0 * error / (1.0 - value);
        Quantile { value, error, u, v }
    }
}

// Avoid printing u and v when debugging, as this is used in an assert_debug (see query()).
impl fmt::Debug for Quantile {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Quantile {{ target: {}, error: {} }}",
            self.value, self.error
        )
    }
}

const BUFFER_SIZE: usize = 500;

/// `Stream` computes the requested quantiles for a stream of `f64`.
pub struct Stream {
    targets: Vec<Quantile>,
    samples: Vec<Sample>,
    number_items: u64, // Number of items seen in the stream
    buffer: Vec<f64>,  // Buffer to temporarily store the oberserved values
}

// A sample measurement with error for compression.
#[derive(Debug)]
struct Sample {
    // v(i) in the paper, the value of this sample.
    v: f64,

    // g(i) in the paper, the difference between the lowest rank of this sample and its predecessor.
    g: f64,

    // ∆(i) in the paper, the error on this sample.
    d: i64,
}

impl Stream {
    /// Creates a new stream that will track the quantiles passed as parameter.
    pub fn new(quantiles: Vec<Quantile>) -> Stream {
        Stream {
            targets: quantiles,
            samples: Vec::new(),
            number_items: 0,
            buffer: Vec::with_capacity(BUFFER_SIZE),
        }
    }

    // The f function that is used to maintain the error when inserting a sample.
    fn invariant(&self, r: f64) -> f64 {
        let mut min = std::f64::MAX;
        let mut fj: f64;
        for target in self.targets.iter() {
            if r < target.error * self.number_items as f64 {
                fj = target.v * (self.number_items as f64 - r);
            } else {
                fj = target.u * r;
            }
            min = f64::min(min, fj)
        }
        min
    }

    /// Adds a new value to the stream.
    pub fn observe(&mut self, value: f64) {
        self.buffer.push(value);
        if self.buffer.len() == BUFFER_SIZE {
            self.flush();
        }
    }

    // Flush the buffer into the list of samples.
    pub fn flush(&mut self) {
        self.buffer.sort_by(|x, y| x.partial_cmp(y).unwrap());

        let mut idx = 0;
        let mut r: f64 = 0.0;
        for value in self.buffer.iter() {
            let value = *value;
            while idx < self.samples.len() && self.samples[idx].v <= value {
                r += self.samples[idx].g;
                idx += 1;
            }

            let new_sample: Sample;
            if idx == 0 || idx == self.samples.len() {
                new_sample = Sample {
                    v: value,
                    g: 1.0,
                    d: 0,
                };
            } else {
                new_sample = Sample {
                    v: value,
                    g: 1.0_f64,
                    d: self.invariant(r).floor() as i64 - 1,
                };
            }
            if idx == self.samples.len() {
                self.samples.push(new_sample);
            } else {
                self.samples.insert(idx, new_sample);
            }
            self.number_items += 1;
        }

        self.buffer.clear();
        self.compress();
    }

    // Remove useless records while keeping the requested quantiles within the error margin.
    fn compress(&mut self) {
        if self.samples.len() < 2 {
            return;
        }
        let mut r = 0.0_f64;
        let mut i = 0;
        while i < self.samples.len() - 2 {
            let current = &self.samples[i];
            let next = &self.samples[i + 1];

            if current.g + next.g + next.d as f64 <= self.invariant(r) {
                println!("removing");
                self.samples[i + 1].g += current.g;
                self.samples.remove(i);
            }
            r += self.samples[i].g;
            i += 1;
        }
    }

    /// Retrieve the value that is within the defined error margin around `quantile`.
    /// This will default to `0.0` as a convention if no value have been fed to this strean
    /// using [`observe()`](::Stream::observe).
    ///
    /// # panic
    ///
    /// This will panic in debug mode only if the requested `target` is not a target defined when
    /// constructed the stream with [`new`](::Stream::new).
    pub fn query(&mut self, quantile: f64) -> f64 {
        debug_assert!(self.targets.iter().filter(|t| t.value == quantile).count() == 1,
            "The queried quantile {} should have been defined when constructing the stream (got: {:?})",
            quantile, &self.targets);

        self.flush();

        if self.samples.is_empty() {
            return 0.0;
        }

        let t = quantile * self.number_items as f64
            + self.invariant(quantile * self.number_items as f64) / 2.0;
        let mut r = 0.0_f64;
        for i in 1..self.samples.len() - 1 {
            r += self.samples[i - 1].g;
            let current = &self.samples[i];
            if r + current.g + current.d as f64 > t {
                return self.samples[i - 1].v;
            }
        }
        let last = &self.samples[self.samples.len() - 1];
        last.v
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Check that the samples kept in the stream are ordered, an invariant of the
    // algorithm.
    fn assert_samples_ordered(stream: &Stream) {
        let mut v = 0.0_f64;
        for s in stream.samples.iter() {
            if s.v < v {
                panic!("Not sorted {:?}", stream.samples)
            }
            v = s.v;
        }
    }

    #[test]
    fn insert_maintains_order() {
        let mut stream = Stream::new(vec![Quantile::new(0.5, 0.1), Quantile::new(0.9, 0.01)]);

        stream.observe(5.0);
        stream.observe(4.0);
        stream.observe(6.0);
        stream.observe(4.0);
        stream.observe(3.0);
        stream.observe(7.0);
        stream.observe(6.0);
        stream.flush();

        assert_samples_ordered(&stream);

        assert_eq!(7, stream.number_items);
    }

    #[test]
    fn test_compress() {
        let mut stream = Stream::new(vec![Quantile::new(0.5, 0.1), Quantile::new(0.9, 0.1)]);

        stream.observe(5.0);
        stream.observe(4.0);
        stream.observe(6.0);
        stream.observe(4.0);
        stream.observe(3.0);
        stream.observe(7.0);
        stream.observe(6.0);
        stream.observe(5.0);
        stream.observe(5.1);
        stream.observe(5.2);
        stream.observe(5.3);
        stream.observe(5.4);
        stream.observe(8.0);
        stream.observe(9.0);
        stream.flush();

        stream.compress();
        assert_samples_ordered(&stream);

        println!("{:?}", stream.samples);
        println!("len {}", stream.samples.len());

        assert_eq!(true, stream.samples.len() < 14);
    }

    #[test]
    fn test_simple() {
        let mut stream = Stream::new(vec![Quantile::new(0.5, 0.05), Quantile::new(0.9, 0.05)]);

        stream.observe(1.0);
        stream.observe(2.0);
        stream.observe(3.0);
        stream.observe(6.0);
        stream.observe(5.0);
        stream.observe(4.0);
        stream.observe(9.1);
        stream.observe(7.0);
        stream.observe(8.0);
        stream.observe(10.0);

        stream.flush();
        stream.compress();
        assert_samples_ordered(&stream);

        println!("{:?}", stream.samples);
        println!("len {}", stream.samples.len());

        assert_eq!(5.0, stream.query(0.5));
    }

    #[test]
    fn no_observation() {
        let mut stream = Stream::new(vec![Quantile::new(0.5, 0.05), Quantile::new(0.9, 0.05)]);
        stream.compress();
        assert_eq!(0.0, stream.query(0.5));
    }

    #[test]
    fn one_observation() {
        let mut stream = Stream::new(vec![Quantile::new(0.5, 0.05), Quantile::new(0.9, 0.05)]);
        stream.observe(3.0);
        stream.compress();
        assert_eq!(3.0, stream.query(0.5));
        assert_eq!(3.0, stream.query(0.9));
    }

    #[test]
    fn query_returns_the_last_sample() {
        let mut stream = Stream::new(vec![Quantile::new(0.5, 0.05), Quantile::new(0.9, 0.05)]);
        stream.observe(3.0);
        stream.observe(5.0);
        stream.compress();
        assert_eq!(5.0, stream.query(0.9));
    }

    #[test]
    #[should_panic]
    fn panic_if_quantile_is_negative() {
        let _stream = Stream::new(vec![Quantile::new(-0.5, 0.05)]);
    }

    #[test]
    #[should_panic]
    fn panic_if_quantile_is_greater_than_one() {
        let _stream = Stream::new(vec![Quantile::new(1.5, 0.05)]);
    }

    #[test]
    #[should_panic]
    fn panic_if_error_is_negative() {
        let _stream = Stream::new(vec![Quantile::new(0.5, -0.05)]);
    }

    #[test]
    #[should_panic]
    fn panic_if_error_is_greater_than_one() {
        let _stream = Stream::new(vec![Quantile::new(0.5, 1.5)]);
    }

    #[test]
    #[should_panic]
    fn panic_if_query_an_untrack_quantile() {
        let mut stream = Stream::new(vec![Quantile::new(0.9, 0.01)]);
        stream.query(0.5);
    }

}
