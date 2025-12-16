//! Authentication
//!
//! User authentication and credential management.

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::Instant;

/// Authentication result
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthResult {
    Success,
    Failed,
    Expired,
    Disabled,
}

/// User credentials
#[derive(Debug, Clone)]
pub struct Credentials {
    pub username: String,
    pub password_hash: String,
    pub enabled: bool,
    pub created_at: Instant,
    pub last_login: Option<Instant>,
}

impl Credentials {
    pub fn new(username: &str, password_hash: &str) -> Self {
        Self {
            username: username.to_string(),
            password_hash: password_hash.to_string(),
            enabled: true,
            created_at: Instant::now(),
            last_login: None,
        }
    }
}

/// Authentication configuration
#[derive(Debug, Clone)]
pub struct AuthConfig {
    /// Require authentication
    pub require_auth: bool,
    /// Session timeout in seconds
    pub session_timeout: u64,
    /// Max login attempts before lockout
    pub max_attempts: u32,
    /// Lockout duration in seconds
    pub lockout_duration: u64,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            require_auth: true,
            session_timeout: 3600,
            max_attempts: 5,
            lockout_duration: 300,
        }
    }
}

/// Authentication manager
pub struct AuthManager {
    config: AuthConfig,
    users: RwLock<HashMap<String, Credentials>>,
    sessions: RwLock<HashMap<String, (String, Instant)>>,
    failed_attempts: RwLock<HashMap<String, (u32, Instant)>>,
}

impl AuthManager {
    pub fn new(config: AuthConfig) -> Self {
        Self {
            config,
            users: RwLock::new(HashMap::new()),
            sessions: RwLock::new(HashMap::new()),
            failed_attempts: RwLock::new(HashMap::new()),
        }
    }

    /// Add a user
    pub fn add_user(&self, username: &str, password_hash: &str) {
        let mut users = self.users.write().unwrap();
        users.insert(username.to_string(), Credentials::new(username, password_hash));
    }

    /// Remove a user
    pub fn remove_user(&self, username: &str) -> bool {
        let mut users = self.users.write().unwrap();
        users.remove(username).is_some()
    }

    /// Authenticate user
    pub fn authenticate(&self, username: &str, password_hash: &str) -> AuthResult {
        // Check lockout
        if self.is_locked_out(username) {
            return AuthResult::Failed;
        }

        let users = self.users.read().unwrap();
        if let Some(creds) = users.get(username) {
            if !creds.enabled {
                return AuthResult::Disabled;
            }
            if creds.password_hash == password_hash {
                self.clear_failed_attempts(username);
                return AuthResult::Success;
            }
        }

        self.record_failed_attempt(username);
        AuthResult::Failed
    }

    /// Create a session
    pub fn create_session(&self, username: &str) -> String {
        let token = format!("{}_{}", username, Instant::now().elapsed().as_nanos());
        let mut sessions = self.sessions.write().unwrap();
        sessions.insert(token.clone(), (username.to_string(), Instant::now()));
        token
    }

    /// Validate a session
    pub fn validate_session(&self, token: &str) -> Option<String> {
        let sessions = self.sessions.read().unwrap();
        if let Some((username, created)) = sessions.get(token) {
            if created.elapsed().as_secs() < self.config.session_timeout {
                return Some(username.clone());
            }
        }
        None
    }

    /// End a session
    pub fn end_session(&self, token: &str) {
        let mut sessions = self.sessions.write().unwrap();
        sessions.remove(token);
    }

    /// Check if user is locked out
    fn is_locked_out(&self, username: &str) -> bool {
        let attempts = self.failed_attempts.read().unwrap();
        if let Some((count, since)) = attempts.get(username) {
            if *count >= self.config.max_attempts {
                return since.elapsed().as_secs() < self.config.lockout_duration;
            }
        }
        false
    }

    /// Record a failed login attempt
    fn record_failed_attempt(&self, username: &str) {
        let mut attempts = self.failed_attempts.write().unwrap();
        let entry = attempts.entry(username.to_string()).or_insert((0, Instant::now()));
        entry.0 += 1;
        entry.1 = Instant::now();
    }

    /// Clear failed attempts
    fn clear_failed_attempts(&self, username: &str) {
        let mut attempts = self.failed_attempts.write().unwrap();
        attempts.remove(username);
    }
}

impl Default for AuthManager {
    fn default() -> Self {
        Self::new(AuthConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auth_manager() {
        let auth = AuthManager::default();
        auth.add_user("admin", "hashed_password");

        assert_eq!(auth.authenticate("admin", "hashed_password"), AuthResult::Success);
        assert_eq!(auth.authenticate("admin", "wrong"), AuthResult::Failed);
    }

    #[test]
    fn test_session() {
        let auth = AuthManager::default();
        auth.add_user("user1", "pass");

        let token = auth.create_session("user1");
        assert_eq!(auth.validate_session(&token), Some("user1".to_string()));

        auth.end_session(&token);
        assert_eq!(auth.validate_session(&token), None);
    }
}
