use std::time::Instant;

/// Evaluating how many times something occurs every second.
pub struct RateMetrics {
    count: u32,
    last_calculated_at: Instant,
}

impl RateMetrics {
    pub fn new(time: Instant) -> Self {
        Self {
            count: 0,
            last_calculated_at: time,
        }
    }

    pub fn increment(&mut self) {
        self.count += 1;
    }

    pub fn has_enough_time_passed(&self, time: Instant) -> bool {
        (time - self.last_calculated_at).as_millis() > 200
    }

    pub(crate) fn calculate_rate(&mut self, time: Instant) -> f32 {
        let elapsed_time = time - self.last_calculated_at;
        let milliseconds = elapsed_time.as_secs() as f32 * 1000.0
            + elapsed_time.subsec_nanos() as f32 / 1_000_000.0;

        let rate = if milliseconds > 0.0 {
            self.count as f32 / milliseconds
        } else {
            0.0
        };

        // Reset the counter and start time for the next period
        self.count = 0;
        self.last_calculated_at = time;

        rate
    }
}
