//! Audit Logging
//!
//! Security audit trail for compliance requirements.

use std::collections::VecDeque;
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

/// Audit event type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditEventType {
    /// Authentication attempt
    AuthAttempt,
    /// Successful login
    Login,
    /// Logout
    Logout,
    /// Command executed
    Command,
    /// Permission denied
    PermissionDenied,
    /// Configuration change
    ConfigChange,
    /// Admin action
    AdminAction,
    /// Connection opened
    Connect,
    /// Connection closed
    Disconnect,
}

/// Audit event
#[derive(Debug, Clone)]
pub struct AuditEvent {
    /// Event type
    pub event_type: AuditEventType,
    /// Timestamp (ms since epoch)
    pub timestamp: u64,
    /// Username (if authenticated)
    pub username: Option<String>,
    /// Client IP address
    pub client_ip: Option<String>,
    /// Command executed
    pub command: Option<String>,
    /// Key accessed
    pub key: Option<String>,
    /// Success/failure
    pub success: bool,
    /// Additional message
    pub message: Option<String>,
}

impl AuditEvent {
    pub fn new(event_type: AuditEventType) -> Self {
        Self {
            event_type,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            username: None,
            client_ip: None,
            command: None,
            key: None,
            success: true,
            message: None,
        }
    }

    pub fn with_user(mut self, username: &str) -> Self {
        self.username = Some(username.to_string());
        self
    }

    pub fn with_client(mut self, ip: &str) -> Self {
        self.client_ip = Some(ip.to_string());
        self
    }

    pub fn with_command(mut self, cmd: &str) -> Self {
        self.command = Some(cmd.to_string());
        self
    }

    pub fn with_key(mut self, key: &str) -> Self {
        self.key = Some(key.to_string());
        self
    }

    pub fn failed(mut self) -> Self {
        self.success = false;
        self
    }

    pub fn with_message(mut self, msg: &str) -> Self {
        self.message = Some(msg.to_string());
        self
    }

    /// Format as JSON
    pub fn to_json(&self) -> String {
        let mut fields = vec![
            format!(r#""type":"{:?}""#, self.event_type),
            format!(r#""timestamp":{}"#, self.timestamp),
            format!(r#""success":{}"#, self.success),
        ];

        if let Some(ref u) = self.username {
            fields.push(format!(r#""username":"{}""#, u));
        }
        if let Some(ref ip) = self.client_ip {
            fields.push(format!(r#""client_ip":"{}""#, ip));
        }
        if let Some(ref cmd) = self.command {
            fields.push(format!(r#""command":"{}""#, cmd));
        }
        if let Some(ref key) = self.key {
            fields.push(format!(r#""key":"{}""#, key));
        }
        if let Some(ref msg) = self.message {
            fields.push(format!(r#""message":"{}""#, msg));
        }

        format!("{{{}}}", fields.join(","))
    }
}

/// Audit logger
pub struct AuditLogger {
    /// In-memory buffer
    buffer: RwLock<VecDeque<AuditEvent>>,
    /// Maximum buffer size
    max_size: usize,
    /// Logging enabled
    enabled: bool,
}

impl AuditLogger {
    pub fn new(max_size: usize) -> Self {
        Self {
            buffer: RwLock::new(VecDeque::with_capacity(max_size)),
            max_size,
            enabled: true,
        }
    }

    /// Log an event
    pub fn log(&self, event: AuditEvent) {
        if !self.enabled {
            return;
        }

        let mut buffer = self.buffer.write().unwrap();
        if buffer.len() >= self.max_size {
            buffer.pop_front();
        }
        buffer.push_back(event);
    }

    /// Log a login attempt
    pub fn log_login(&self, username: &str, client_ip: &str, success: bool) {
        let mut event = AuditEvent::new(if success {
            AuditEventType::Login
        } else {
            AuditEventType::AuthAttempt
        })
        .with_user(username)
        .with_client(client_ip);

        if !success {
            event = event.failed();
        }

        self.log(event);
    }

    /// Log a command execution
    pub fn log_command(&self, username: &str, command: &str, key: Option<&str>) {
        let mut event = AuditEvent::new(AuditEventType::Command)
            .with_user(username)
            .with_command(command);

        if let Some(k) = key {
            event = event.with_key(k);
        }

        self.log(event);
    }

    /// Log a permission denial
    pub fn log_denied(&self, username: &str, command: &str, key: Option<&str>) {
        let mut event = AuditEvent::new(AuditEventType::PermissionDenied)
            .with_user(username)
            .with_command(command)
            .failed();

        if let Some(k) = key {
            event = event.with_key(k);
        }

        self.log(event);
    }

    /// Get recent events
    pub fn recent(&self, count: usize) -> Vec<AuditEvent> {
        let buffer = self.buffer.read().unwrap();
        buffer.iter().rev().take(count).cloned().collect()
    }

    /// Get events as JSON
    pub fn export_json(&self) -> String {
        let buffer = self.buffer.read().unwrap();
        let events: Vec<String> = buffer.iter().map(|e| e.to_json()).collect();
        format!("[{}]", events.join(","))
    }

    /// Clear the buffer
    pub fn clear(&self) {
        let mut buffer = self.buffer.write().unwrap();
        buffer.clear();
    }
}

impl Default for AuditLogger {
    fn default() -> Self {
        Self::new(10000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_audit_event() {
        let event = AuditEvent::new(AuditEventType::Login)
            .with_user("admin")
            .with_client("192.168.1.1");

        assert_eq!(event.event_type, AuditEventType::Login);
        assert!(event.success);
    }

    #[test]
    fn test_audit_logger() {
        let logger = AuditLogger::new(100);
        logger.log_login("user1", "127.0.0.1", true);
        logger.log_command("user1", "GET", Some("foo"));

        let recent = logger.recent(10);
        assert_eq!(recent.len(), 2);
    }

    #[test]
    fn test_json_export() {
        let logger = AuditLogger::new(100);
        logger.log_login("admin", "10.0.0.1", true);

        let json = logger.export_json();
        assert!(json.contains("Login"));
        assert!(json.contains("admin"));
    }
}
