use std::collections::HashMap;
use std::fs;
use std::fs::{DirEntry, File, OpenOptions, read, read_dir};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use chrono::Duration;
use tokio::time;
use crate::storage::domain::Record;
use crate::storage::vessel2::Vessel2;

mod cursor;
mod vessel;
mod domain;
mod open_chunk;
mod threading;
mod storage;

static CHUNK_SIZE:i32 = 1000;

struct TemperatureReading {
    value: f64,
    altitude: f64
}

impl TemperatureReading {
    fn from_string(string: &str) -> Self {
        let vals = string.split(",").collect::<Vec<&str>>();

        return TemperatureReading {
            value: vals[0].parse::<f64>().unwrap(),
            altitude: vals[1].parse::<f64>().unwrap()
        };
    }

    fn to_string(&self) -> String {
        let value = self.value;
        let altitude = self.altitude;

        return String::from(&format!("{value},{altitude}"));
    }
}

#[tokio::main]
async fn main() {
    let root = "/home/chris/rusty_vessel";

    let mut vessel = Vessel2::new(
        root,
        "Temperature",
        Duration::seconds(60));

    for i in 0..10000 {
        let reading = TemperatureReading{ value: 9.0, altitude: 99.0 };

        // let record = Record {
        //     key: "London".to_string(),
        //     index: i,
        //     data: reading.to_string()
        // };

        //vessel.write(record);
        //tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
