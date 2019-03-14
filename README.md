# Rust quantiles computation

[![Build Status](https://travis-ci.org/thibaultam/rust-quantile.svg?branch=master)](https://travis-ci.org/thibaultam/rust-quantile)

This crate computes approximate quantiles over an unbounded data stream with
a predefined error margin.

It implements the algorithm `Effective computation of biased quantiles over data streams`
by Comodre, Korn, Muthukrishnan and Srivastava. With this algorithm a bit of precision
is traded for a better memory usage.

## Usage

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

## Thanks

Many thanks to:
* Comodre, Korn, Muthukrishnan and Srivastava for writting the original paper,
* [`beorn7`](https://github.com/beorn7/perks) for his implementation in Go of the same algorithm that proved to be a great inspiration,