use std::time::{Duration, Instant};

pub struct Throttle {
    timeout: u128,
    last: Instant,
}

impl Throttle {
    pub fn new(rate: Duration, threshold: usize) -> Throttle {
        let timeout = (rate.as_millis()) / (threshold as u128);
        let last = Instant::now();

        Throttle {
            timeout: timeout,
            last: last,
        }
    }

    pub fn accept(&mut self) -> Result<(), Instant> {
        if self.last.elapsed().as_millis() < self.timeout {
            Err(self.last)
        } else {
            self.last = Instant::now();

            Ok(())
        }
    }
}
