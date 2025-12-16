//! Security Module
//!
//! Authentication, authorization, encryption, and audit logging.

pub mod auth;
pub mod acl;
pub mod tls;
pub mod audit;

pub use auth::{AuthManager, AuthConfig, Credentials, AuthResult};
pub use acl::{AclManager, Permission, Role, AclRule};
pub use tls::{TlsConfig, TlsAcceptor};
pub use audit::{AuditLogger, AuditEvent, AuditEventType};
