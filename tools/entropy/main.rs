#[allow(dead_code)]
#[path = "../counters.rs"]
mod counters;
mod frame;
mod measure;

fn main() {
    counters::entry(Some(measure::run));
}
