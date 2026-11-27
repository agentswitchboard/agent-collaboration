//! Builds the sample service and injects its ports.
use sample_service::{SampleService, SampleSettings};

pub fn build_sample_service(settings: SampleSettings) -> SampleService {
    SampleService::new(settings)
}
