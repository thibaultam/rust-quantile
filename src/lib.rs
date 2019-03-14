#![allow(dead_code)] // TODO

/*!
This crate computes approximate quantiles with a predefined error margin
over an unbounded data stream.

It implements the algorithm `Effective computation of biased quantiles over data streams`
by Cormode, Korn, Muthukrishnan and Srivastava. With this algorithm a bit of precision
is traded for a better memory usage.

# Usage

```rust
use quantile;

// Track the 50th quantile with a precision of 0.5% and the 90th quantile with a precision of 0.5%.
let mut stream = quantile::new(vec![
    quantile::new_quantile(0.5, 0.005),
    quantile::new_quantile(0.9, 0.005),
]);

for i in 1..101 {
    // Observe a value.
    stream.observe(i as f64);
}

assert_eq!(50.0_f64, stream.query(0.5));
assert_eq!(90.0_f64, stream.query(0.9));
!*/

// TODO thread safety

/// The quantile that needs to be computed and its associated error margin,
/// such that the value returned for a target is garanteed to be at a rank ±epsilon
/// percent around the value.
pub struct Quantile {
    value: f64,
    error: f64,
}

/// Creates a new `Quantile` to track and its associated error.
///
/// # panic
///
/// Panics if `value < 0`, `value > 1`, `error < 0` or `error > 1`.
pub fn new_quantile(value: f64, error: f64) -> Quantile {
    assert!(value >= 0.0, "quantile value should be >= 0: {}", value);
    assert!(value <= 1.0, "quantile value should be <= 1: {}", value);
    assert!(error >= 0.0, "quantile error should be >= 0: {}", error);
    assert!(error <= 1.0, "quantile error should be <= 0: {}", error);
    Quantile { value, error }
}

/// `Stream` computes the requested quantiles for a stream of `f64`.
pub struct Stream {
    targets: Vec<Quantile>,
    samples: Vec<Sample>,
    number_items: f64, // Number of items seen in the stream (could be an u64 but is used in floating point operations)
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

/// Creates a new stream that will track the quantiles passed as parameter.
pub fn new(quantiles: Vec<Quantile>) -> Stream {
    Stream {
        targets: quantiles,
        samples: Vec::new(),
        number_items: 0.0, // TODO int
    }
}

impl Stream {
    fn add_target(&mut self, target: Quantile) {
        self.targets.push(target);
    }

    // The f function that is used to maintain the error when inserting a sample.
    fn invariant(&self, r: f64) -> f64 {
        let mut min = std::f64::MAX;
        let mut fj: f64;
        for target in self.targets.iter() {
            if r < target.error * self.number_items {
                fj = 2.0 * target.error * (self.number_items - r) / (1.0 - target.value);
            } else {
                fj = 2.0 * target.error * r / target.value;
            }
            min = f64::min(min, fj)
        }
        min
    }

    /// Adds a new value to the stream.
    pub fn observe(&mut self, value: f64) {
        let mut r: f64 = 0.0;
        let mut idx = 0;
        for sample in self.samples.iter() {
            if sample.v > value {
                break;
            }
            r += sample.g;
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
                // d: i64::max(self.invariant(r).floor() as i64 - 1, 0), // TODO: this is not in the paper :D
                d: self.invariant(r).floor() as i64 - 1, // TODO this is the proper implem
            };
        }
        if idx == self.samples.len() {
            self.samples.push(new_sample);
        } else {
            self.samples.insert(idx, new_sample);
        }
        self.number_items += 1.0;
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

    /// Retrieve the value that is within the defined error margin around quantile `target`.
    /// This will default to `0.0` as a convention if no value have been fed to this strean
    /// using [`observe()`](::observe).
    ///
    /// # panic
    ///
    /// This will panic in debug mode only if the requested `target` is not a target defined when
    /// constructed the stream with [`new`](::new).
    pub fn query(&self, target: f64) -> f64 {
        // TODO assert

        if self.samples.is_empty() {
            return 0.0;
        }
        let mut r = 0.0_f64;
        for i in 1..self.samples.len() - 1 {
            r += self.samples[i - 1].g;
            let current = &self.samples[i];
            // TODO precompute
            if r + current.g + current.d as f64
                > target * self.number_items + self.invariant(target * self.number_items) / 2.0
            {
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
        let mut stream = new(vec![new_quantile(0.5, 0.1), new_quantile(0.9, 0.01)]);

        stream.observe(5.0);
        stream.observe(4.0);
        stream.observe(6.0);
        stream.observe(4.0);
        stream.observe(3.0);
        stream.observe(7.0);
        stream.observe(6.0);

        assert_samples_ordered(&stream);
    }

    #[test]
    fn test_compress() {
        let mut stream = new(vec![new_quantile(0.5, 0.1), new_quantile(0.9, 0.1)]);

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

        stream.compress();
        assert_samples_ordered(&stream);

        println!("{:?}", stream.samples);
        println!("len {}", stream.samples.len());

        assert_eq!(true, stream.samples.len() < 14);
    }

    #[test]
    fn test_simple() {
        let mut stream = new(vec![new_quantile(0.5, 0.05), new_quantile(0.9, 0.05)]);

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

        stream.compress();
        assert_samples_ordered(&stream);

        println!("{:?}", stream.samples);
        println!("len {}", stream.samples.len());

        assert_eq!(5.0, stream.query(0.5));
    }

    #[test]
    fn no_observation() {
        let mut stream = new(vec![new_quantile(0.5, 0.05), new_quantile(0.9, 0.05)]);
        stream.compress();
        assert_eq!(0.0, stream.query(0.5));
    }

    #[test]
    fn one_observation() {
        let mut stream = new(vec![new_quantile(0.5, 0.05), new_quantile(0.9, 0.05)]);
        stream.observe(3.0);
        stream.compress();
        assert_eq!(3.0, stream.query(0.5));
        assert_eq!(3.0, stream.query(0.9));
    }

    #[test]
    fn query_returns_the_last_sample() {
        let mut stream = new(vec![new_quantile(0.5, 0.05), new_quantile(0.9, 0.05)]);
        stream.observe(3.0);
        stream.observe(5.0);
        stream.compress();
        assert_eq!(5.0, stream.query(0.9));
    }

    #[test]
    #[should_panic]
    fn negative_quantile_is_forbidden() {
        let _stream = new(vec![new_quantile(-0.5, 0.05)]);
    }

    #[test]
    #[should_panic]
    fn quantile_greater_than_one_is_forbidden() {
        let _stream = new(vec![new_quantile(1.5, 0.05)]);
    }

    #[test]
    #[should_panic]
    fn negative_error_is_forbidden() {
        let _stream = new(vec![new_quantile(0.5, -0.05)]);
    }

    #[test]
    #[should_panic]
    fn error_greater_than_one_is_forbidden() {
        let _stream = new(vec![new_quantile(1.5, 1.5)]);
    }

}
