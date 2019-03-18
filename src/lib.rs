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
before using the public methods of the [`Stream`](struct.Stream.html).
!*/

use std::cell::RefCell;
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
    new_samples: RefCell<Vec<Sample>>,
    number_items: u64, // Number of items seen in the stream
    buffer: Vec<f64>,  // Buffer to temporarily store the oberserved values
}

// A sample measurement with error for compression.
#[derive(Debug, Clone)]
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
            new_samples: RefCell::new(Vec::new()),
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
            self.flush_and_compress();
        }
    }

    // Flushes the buffer containing the last observations into the list of samples.
    // This function will iterate over both the actual samples and the new observations
    // in self.buffer in ascending order. For each value it will merge it with the previous
    // one if possible and insert it in a new vector.
    // The usage of a new vector makes the insertion and removal (during a merge) of a sample
    // non-expensive.
    // The cost of the second array is limited as we keep the same array and use `Vec.clean()`
    // to avoid allocating at each iteration.
    fn flush_and_compress(&mut self) {
        if self.buffer.is_empty() {
            return;
        }
        self.buffer.sort_by(|x, y| x.partial_cmp(y).unwrap());

        let mut idx = 0;
        // Contains the sum of all the previous sample.g excluding the last one added,
        // as it should not be taken into account in the merging of samples.
        let mut prev_r = 0.0_f64;

        for value in self.buffer.iter() {
            let value = *value;
            // Iterate over samples smaller than value
            while idx < self.samples.len() && self.samples[idx].v <= value {
                self.merge_and_insert(&self.samples[idx], &mut prev_r);
                idx += 1;
            }

            // Insert new value
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
                    d: self.invariant(prev_r + self.samples[idx].g).floor() as i64 - 1,
                };
            }
            self.merge_and_insert(&new_sample, &mut prev_r);
            self.number_items += 1;
        }
        // Values after last insertion
        while idx < self.samples.len() {
            self.merge_and_insert(&self.samples[idx], &mut prev_r);
            idx += 1;
        }

        self.buffer.clear();

        std::mem::swap(&mut self.samples, &mut self.new_samples.borrow_mut());
        self.new_samples.borrow_mut().clear();
    }

    // This function tries to merge the `current` sample with the last sample in
    // `new_samples` if possible.
    // It always inserts the `current` sample, either by adding it to the list or by merging it.
    // In addition it will maintain the value of `prev_r` by adding the appropriate value to it
    // when a merge occurs.
    fn merge_and_insert(&self, current: &Sample, prev_r: &mut f64) {
        let mut new_samples = self.new_samples.borrow_mut();
        if new_samples.is_empty() {
            new_samples.push(current.clone());
            return;
        }
        let last_idx = new_samples.len() - 1;
        let prev = &mut new_samples[last_idx];
        if prev.g + current.g + current.d as f64 <= self.invariant(*prev_r) {
            // Merge
            prev.g += current.g;
            prev.v = current.v;
            prev.d = current.d;
        } else {
            // Insert
            *prev_r += prev.g;
            new_samples.push(current.clone());
        }
    }

    /// Retrieve the value that is within the defined error margin around `quantile`.
    /// This will default to `0.0` as a convention if no value have been fed to this strean
    /// using [`observe()`](struct.Stream.html#method.observe).
    ///
    /// # panic
    ///
    /// This will panic in debug mode only if the requested `target` is not a target defined when
    /// constructed the stream with [`new`](struct.Stream.html#method.new).
    pub fn query(&mut self, quantile: f64) -> f64 {
        debug_assert!(self.targets.iter().filter(|t| t.value == quantile).count() == 1,
            "The queried quantile {} should have been defined when constructing the stream (got: {:?})",
            quantile, &self.targets);

        self.flush_and_compress();

        if self.samples.is_empty() {
            println!("Empty");
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

    #[test]
    fn samples_should_be_maintain_ordered_and_compressed() {
        let mut stream = Stream::new(vec![Quantile::new(0.5, 0.1)]);

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
        stream.flush_and_compress();

        // Assert that the samples are ordered
        let mut v = 0.0_f64;
        for s in stream.samples.iter() {
            if s.v < v {
                panic!("Not sorted {:?}", stream.samples)
            }
            v = s.v;
        }

        assert_eq!(
            true,
            stream.samples.len() < 14,
            "Compression did not take place: got {} samples",
            stream.samples.len()
        );

        assert_eq!(
            stream.new_samples.borrow().len(),
            0,
            "New samples are not cleaned"
        );
    }

    #[test]
    fn test_simple() {
        let mut stream = Stream::new(vec![Quantile::new(0.5, 0.05), Quantile::new(0.95, 0.05)]);

        stream.observe(1.0);
        stream.observe(2.0);
        stream.observe(3.0);
        stream.observe(4.0);
        stream.observe(5.0);
        stream.observe(6.0);
        stream.observe(7.0);
        stream.observe(8.0);
        stream.observe(9.0);
        stream.observe(10.0);

        assert_eq!(5.0, stream.query(0.5));
        assert_eq!(10.0, stream.query(0.95));
    }

    #[test]
    fn no_observation() {
        let mut stream = Stream::new(vec![Quantile::new(0.5, 0.05), Quantile::new(0.9, 0.05)]);
        assert_eq!(0.0, stream.query(0.5));
    }

    #[test]
    fn one_observation() {
        let mut stream = Stream::new(vec![Quantile::new(0.5, 0.05), Quantile::new(0.9, 0.05)]);
        stream.observe(3.0);
        assert_eq!(3.0, stream.query(0.5));
        assert_eq!(3.0, stream.query(0.9));
    }

    #[test]
    fn query_returns_the_last_sample() {
        let mut stream = Stream::new(vec![Quantile::new(0.5, 0.05), Quantile::new(0.9, 0.05)]);
        stream.observe(3.0);
        stream.observe(5.0);
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
