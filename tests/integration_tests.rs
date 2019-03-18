#[cfg(test)]
extern crate rand;

use ::rand::distributions::{Distribution, Normal};
use ::rand::rngs::StdRng;
use ::rand::{Rng, SeedableRng};

use quantile::{Quantile, Stream};

// Defines the distribution of the point to observe when building a stream.
enum DistributionType {
    Uniform,
    Normal(f64, f64), // Normal distribution with mean and standard deviation
}

// Populates an array of random values using the specified probability distribution.
fn build_stream_uniform(distribution: DistributionType) -> Vec<f64> {
    let mut stream = Vec::with_capacity(10_000);
    let seed = [
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25,
        26, 27, 28, 29, 30, 32, 31,
    ];
    let mut rng: StdRng = SeedableRng::from_seed(seed);
    let normal = match distribution {
        DistributionType::Normal(mean, std_dev) => Normal::new(mean, std_dev),
        _ => Normal::new(0.0, 1.0), // Won't be used
    };

    for _ in 0..10_000 {
        let x: f64 = match distribution {
            DistributionType::Uniform => rng.gen(),
            DistributionType::Normal(_, _) => normal.sample(&mut rng),
        };
        stream.push(x);
    }
    stream
}

fn assert_quantile_in_error(stream: &Vec<f64>, quantile: f64, error: f64, value: f64) {
    let pos_min: usize = (stream.len() as f64 * (quantile - error)).floor() as usize;
    let pos_max: usize = (stream.len() as f64 * (quantile + error)).floor() as usize;
    let min = stream[pos_min];
    let max = stream[pos_max];

    if value < min || value > max {
        panic!("Invalid value for quantile ({quantile}, {error}): value {value} should be between {min} and {max} (excluded)",
            quantile = quantile,
            error = error,
            min = min,
            max = max,
            value = value);
    }
}

#[test]
#[should_panic]
fn test_quantile_too_low() {
    let mut stream: Vec<f64> = Vec::with_capacity(10);
    for i in 0..10 {
        stream.push(i as f64);
    }

    assert_quantile_in_error(&stream, 0.5, 0.1, 3.0);
}

#[test]
#[should_panic]
fn test_quantile_too_high() {
    let mut stream: Vec<f64> = Vec::with_capacity(10);
    for i in 0..10 {
        stream.push(i as f64);
    }

    assert_quantile_in_error(&stream, 0.5, 0.1, 7.0);
}

#[test]
fn integration_test_simple() {
    let mut data: Vec<f64> = Vec::with_capacity(100);
    let mut stream = Stream::new(vec![Quantile::new(0.5, 0.005), Quantile::new(0.9, 0.005)]);

    for i in 1..101 {
        stream.observe(i as f64);
        data.push(i as f64);
    }

    assert_eq!(50.0_f64, stream.query(0.5));
    assert_eq!(90.0_f64, stream.query(0.9));

    assert_quantile_in_error(&data, 0.5, 0.005, stream.query(0.5));
    assert_quantile_in_error(&data, 0.9, 0.005, stream.query(0.9));
}

#[test]
fn quantiles_uniformely_distributed() {
    let mut data = build_stream_uniform(DistributionType::Uniform);

    let mut stream = Stream::new(vec![
        Quantile::new(0.1, 0.0001),
        Quantile::new(0.5, 0.01),
        Quantile::new(0.9, 0.005),
        Quantile::new(0.99, 0.0001),
    ]);

    for x in data.iter() {
        stream.observe(*x);
    }

    data.sort_by(|a, b| a.partial_cmp(b).unwrap());

    assert_quantile_in_error(&data, 0.1, 0.001, stream.query(0.1));
    assert_quantile_in_error(&data, 0.5, 0.01, stream.query(0.5));
    assert_quantile_in_error(&data, 0.9, 0.05, stream.query(0.9));
    assert_quantile_in_error(&data, 0.99, 0.0001, stream.query(0.99));
}

#[test]
fn quantiles_normal_distribution() {
    let mut data = build_stream_uniform(DistributionType::Normal(3.0, 1.0));

    let mut stream = Stream::new(vec![
        Quantile::new(0.1, 0.0001),
        Quantile::new(0.5, 0.01),
        Quantile::new(0.9, 0.005),
        Quantile::new(0.99, 0.0001),
    ]);

    for x in data.iter() {
        stream.observe(*x);
    }

    data.sort_by(|a, b| a.partial_cmp(b).unwrap());

    assert_quantile_in_error(&data, 0.1, 0.001, stream.query(0.1));
    assert_quantile_in_error(&data, 0.5, 0.01, stream.query(0.5));
    assert_quantile_in_error(&data, 0.9, 0.05, stream.query(0.9));
    assert_quantile_in_error(&data, 0.99, 0.0001, stream.query(0.99));
}

#[test]
fn random_data() {
    for _i in 1..1000 {
        let mut data = build_stream_uniform(DistributionType::Uniform);

        let mut stream = Stream::new(vec![
            Quantile::new(0.1, 0.0001),
            Quantile::new(0.5, 0.01),
            Quantile::new(0.9, 0.005),
            Quantile::new(0.99, 0.0001),
        ]);

        for x in data.iter() {
            stream.observe(*x);
        }

        data.sort_by(|a, b| a.partial_cmp(b).unwrap());

        assert_quantile_in_error(&data, 0.1, 0.001, stream.query(0.1));
        assert_quantile_in_error(&data, 0.5, 0.01, stream.query(0.5));
        assert_quantile_in_error(&data, 0.9, 0.05, stream.query(0.9));
        assert_quantile_in_error(&data, 0.99, 0.0001, stream.query(0.99));
    }
}
