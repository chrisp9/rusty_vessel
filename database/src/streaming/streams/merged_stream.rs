// use std::rc::Rc;
// use crate::{Blob, StreamDefinition, UnixTime, Vessel};
// use crate::streaming::streams::stream::Stream;
//
// pub struct MergedStream {
//     pub stream_def: StreamDefinition,
//     pub sources: Vec<StreamDefinition>,
//     vessel: Vessel
// }
//
// impl MergedStream {
//     pub fn new(stream_def: StreamDefinition, vessel: Vessel) -> MergedStream {
//         return MergedStream {
//             stream_def,
//             vessel
//         }
//     }
// }
//
// impl Stream for MergedStream {
//     fn replay(&mut self, since: UnixTime) -> Box<dyn Iterator<Item=Vec<Blob>>> {
//         return Box::new(self.vessel.read_from(since));
//     }
//
//     fn flush(&mut self) {
//         self.vessel.flush();
//     }
//
//     fn on_next(&mut self, record: Rc<Vec<Blob>>) -> Rc<Vec<Blob>> {
//         self.vessel.write(record.clone());
//         return record.clone();
//     }
// }
