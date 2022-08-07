use std::rc::Rc;
use crate::{Blob, Stream, StreamDefinition, Vessel};
use crate::domain::UnixTime;

use crate::streaming::domain::{Aggregator, Calc};

pub struct AggregateStream {
    pub stream_def: StreamDefinition,
    vessel: Vessel,
    buf: Aggregator
}


impl AggregateStream {
    pub fn new(stream_def: StreamDefinition, calc: Calc, vessel: Vessel, count: usize, interval: usize) -> AggregateStream {
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

    fn on_next(&mut self, input: Rc<Vec<Blob>>) -> Rc<Vec<Blob>> {
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
