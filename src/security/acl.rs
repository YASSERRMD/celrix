//! Access Control Lists (ACL)
//!
//! Role-based access control for commands and keys.

use std::collections::{HashMap, HashSet};
use std::sync::RwLock;

/// Permission type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Permission {
    /// Read operations (GET, EXISTS, SCAN)
    Read,
    /// Write operations (SET, DEL)
    Write,
    /// Admin operations (CONFIG, SHUTDOWN)
    Admin,
    /// Cluster operations
    Cluster,
    /// All permissions
    All,
}

/// ACL rule for key patterns
#[derive(Debug, Clone)]
pub struct AclRule {
    /// Key pattern (glob style)
    pub pattern: String,
    /// Allowed permissions
    pub permissions: HashSet<Permission>,
}

impl AclRule {
    pub fn new(pattern: &str) -> Self {
        Self {
            pattern: pattern.to_string(),
            permissions: HashSet::new(),
        }
    }

    pub fn with_permission(mut self, perm: Permission) -> Self {
        self.permissions.insert(perm);
        self
    }

    pub fn with_read(self) -> Self {
        self.with_permission(Permission::Read)
    }

    pub fn with_write(self) -> Self {
        self.with_permission(Permission::Write)
    }

    pub fn with_all(mut self) -> Self {
        self.permissions.insert(Permission::All);
        self
    }

    /// Check if key matches pattern
    pub fn matches_key(&self, key: &str) -> bool {
        glob_match(&self.pattern, key)
    }

    /// Check if permission is allowed
    pub fn allows(&self, perm: Permission) -> bool {
        self.permissions.contains(&Permission::All) || self.permissions.contains(&perm)
    }
}

/// Role definition
#[derive(Debug, Clone)]
pub struct Role {
    pub name: String,
    pub rules: Vec<AclRule>,
    pub commands: HashSet<String>,
    pub denied_commands: HashSet<String>,
}

impl Role {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            rules: Vec::new(),
            commands: HashSet::new(),
            denied_commands: HashSet::new(),
        }
    }

    /// Add an ACL rule
    pub fn with_rule(mut self, rule: AclRule) -> Self {
        self.rules.push(rule);
        self
    }

    /// Allow a command
    pub fn allow_command(mut self, cmd: &str) -> Self {
        self.commands.insert(cmd.to_uppercase());
        self
    }

    /// Deny a command
    pub fn deny_command(mut self, cmd: &str) -> Self {
        self.denied_commands.insert(cmd.to_uppercase());
        self
    }

    /// Check if command is allowed
    pub fn can_execute(&self, cmd: &str) -> bool {
        let cmd = cmd.to_uppercase();
        if self.denied_commands.contains(&cmd) {
            return false;
        }
        self.commands.is_empty() || self.commands.contains(&cmd) || self.commands.contains("*")
    }

    /// Check if key access is allowed
    pub fn can_access(&self, key: &str, perm: Permission) -> bool {
        for rule in &self.rules {
            if rule.matches_key(key) && rule.allows(perm) {
                return true;
            }
        }
        false
    }
}

/// Predefined roles
impl Role {
    pub fn admin() -> Self {
        Self::new("admin")
            .with_rule(AclRule::new("*").with_all())
            .allow_command("*")
    }

    pub fn read_only() -> Self {
        Self::new("readonly")
            .with_rule(AclRule::new("*").with_read())
            .allow_command("GET")
            .allow_command("EXISTS")
            .allow_command("SCAN")
            .allow_command("KEYS")
            .allow_command("MGET")
    }

    pub fn write_only() -> Self {
        Self::new("writeonly")
            .with_rule(AclRule::new("*").with_write())
            .allow_command("SET")
            .allow_command("DEL")
            .allow_command("MSET")
    }
}

/// ACL manager
pub struct AclManager {
    roles: RwLock<HashMap<String, Role>>,
    user_roles: RwLock<HashMap<String, Vec<String>>>,
}

impl AclManager {
    pub fn new() -> Self {
        let mut roles = HashMap::new();
        roles.insert("admin".to_string(), Role::admin());
        roles.insert("readonly".to_string(), Role::read_only());
        roles.insert("writeonly".to_string(), Role::write_only());

        Self {
            roles: RwLock::new(roles),
            user_roles: RwLock::new(HashMap::new()),
        }
    }

    /// Add a role
    pub fn add_role(&self, role: Role) {
        let mut roles = self.roles.write().unwrap();
        roles.insert(role.name.clone(), role);
    }

    /// Assign role to user
    pub fn assign_role(&self, username: &str, role_name: &str) {
        let mut user_roles = self.user_roles.write().unwrap();
        user_roles
            .entry(username.to_string())
            .or_default()
            .push(role_name.to_string());
    }

    /// Check if user can execute command
    pub fn can_execute(&self, username: &str, cmd: &str) -> bool {
        let user_roles = self.user_roles.read().unwrap();
        let roles = self.roles.read().unwrap();

        if let Some(role_names) = user_roles.get(username) {
            for role_name in role_names {
                if let Some(role) = roles.get(role_name) {
                    if role.can_execute(cmd) {
                        return true;
                    }
                }
            }
        }
        false
    }

    /// Check if user can access key
    pub fn can_access(&self, username: &str, key: &str, perm: Permission) -> bool {
        let user_roles = self.user_roles.read().unwrap();
        let roles = self.roles.read().unwrap();

        if let Some(role_names) = user_roles.get(username) {
            for role_name in role_names {
                if let Some(role) = roles.get(role_name) {
                    if role.can_access(key, perm) {
                        return true;
                    }
                }
            }
        }
        false
    }
}

impl Default for AclManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Simple glob pattern matching
fn glob_match(pattern: &str, text: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if pattern.ends_with('*') {
        return text.starts_with(&pattern[..pattern.len() - 1]);
    }
    if pattern.starts_with('*') {
        return text.ends_with(&pattern[1..]);
    }
    pattern == text
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_acl_rule() {
        let rule = AclRule::new("user:*").with_read().with_write();
        assert!(rule.matches_key("user:123"));
        assert!(!rule.matches_key("admin:456"));
        assert!(rule.allows(Permission::Read));
        assert!(!rule.allows(Permission::Admin));
    }

    #[test]
    fn test_role() {
        let role = Role::admin();
        assert!(role.can_execute("GET"));
        assert!(role.can_execute("CONFIG"));
        assert!(role.can_access("any:key", Permission::Admin));
    }

    #[test]
    fn test_acl_manager() {
        let acl = AclManager::new();
        acl.assign_role("user1", "readonly");

        assert!(acl.can_execute("user1", "GET"));
        assert!(!acl.can_execute("user1", "SET"));
        assert!(acl.can_access("user1", "foo", Permission::Read));
        assert!(!acl.can_access("user1", "foo", Permission::Write));
    }
}
