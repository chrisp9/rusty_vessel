use std::rc::Rc;
use crate::{Blob, StreamDefinition, StreamRef, Vessel};
use crate::domain::UnixTime;

use crate::streaming::domain::{Aggregator, Calc};
use crate::streaming::streams::stream::Stream;

pub struct AggregateStream {
    pub stream_def: StreamRef,
    vessel: Vessel,
    buf: Aggregator,
}


impl AggregateStream {
    pub fn new(stream_def: StreamRef, calc: Calc, vessel: Vessel, count: usize, interval: usize) -> AggregateStream {
        return AggregateStream {
            stream_def,
            vessel,
            buf: Aggregator::new(calc, count, interval)
        }
    }
}

impl Stream for AggregateStream {
    fn replay(&mut self, since: UnixTime) -> Box<dyn Iterator<Item=Vec<Blob>>> {
        return Box::new(self.vessel.read_from(since));
    }

    fn flush(&mut self) {
        self.vessel.flush();
    }

    fn on_next(&mut self, source: StreamRef, input: Rc<Vec<Blob>>) -> Rc<Vec<Blob>> {
        let mut output = vec!();

        for record in input.iter() {
            if let Some(val) = self.buf.add(record.clone()){
                output.push(val);
            }
        }

        let records = Rc::new(output);
        self.vessel.write(records.clone());
        return records.clone();
    }
}

