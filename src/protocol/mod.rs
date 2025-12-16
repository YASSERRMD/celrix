//! VCP Protocol - Velocity Cache Protocol
//!
//! Custom binary protocol optimized for high-performance caching.
//! Uses 22-byte fixed headers for minimal parsing overhead.

mod codec;
mod command;
mod extended_commands;
mod frame;
mod response;

pub use codec::VcpCodec;
pub use command::Command;
pub use extended_commands::ExtendedCommand;
pub use frame::{Frame, FrameHeader, OpCode, HEADER_SIZE, MAGIC};
pub use response::Response;
