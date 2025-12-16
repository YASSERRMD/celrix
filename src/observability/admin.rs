//! Admin HTTP API
//!
//! RESTful admin endpoints for management and monitoring.

use std::collections::HashMap;

/// Admin API configuration
#[derive(Debug, Clone)]
pub struct AdminConfig {
    /// Admin API port
    pub port: u16,
    /// Enable admin API
    pub enabled: bool,
    /// Require authentication
    pub require_auth: bool,
    /// Admin API key
    pub api_key: Option<String>,
}

impl Default for AdminConfig {
    fn default() -> Self {
        Self {
            port: 9090,
            enabled: true,
            require_auth: false,
            api_key: None,
        }
    }
}

impl AdminConfig {
    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    pub fn with_auth(mut self, api_key: &str) -> Self {
        self.require_auth = true;
        self.api_key = Some(api_key.to_string());
        self
    }
}

/// Admin endpoint handler result
#[derive(Debug, Clone)]
pub struct AdminResponse {
    pub status: u16,
    pub content_type: String,
    pub body: String,
}

impl AdminResponse {
    pub fn ok(body: &str) -> Self {
        Self {
            status: 200,
            content_type: "application/json".to_string(),
            body: body.to_string(),
        }
    }

    pub fn error(status: u16, message: &str) -> Self {
        Self {
            status,
            content_type: "application/json".to_string(),
            body: format!(r#"{{"error":"{}"}}"#, message),
        }
    }

    pub fn not_found() -> Self {
        Self::error(404, "Not found")
    }

    pub fn unauthorized() -> Self {
        Self::error(401, "Unauthorized")
    }
}

/// Admin API handler type
pub type AdminHandler = Box<dyn Fn(&AdminRequest) -> AdminResponse + Send + Sync>;

/// Admin request
#[derive(Debug, Clone)]
pub struct AdminRequest {
    pub method: String,
    pub path: String,
    pub query: HashMap<String, String>,
    pub headers: HashMap<String, String>,
    pub body: String,
}

impl AdminRequest {
    pub fn new(method: &str, path: &str) -> Self {
        Self {
            method: method.to_uppercase(),
            path: path.to_string(),
            query: HashMap::new(),
            headers: HashMap::new(),
            body: String::new(),
        }
    }

    pub fn with_query(mut self, key: &str, value: &str) -> Self {
        self.query.insert(key.to_string(), value.to_string());
        self
    }

    pub fn with_header(mut self, key: &str, value: &str) -> Self {
        self.headers.insert(key.to_lowercase(), value.to_string());
        self
    }

    pub fn get_header(&self, key: &str) -> Option<&String> {
        self.headers.get(&key.to_lowercase())
    }
}

/// Admin API router
pub struct AdminApi {
    config: AdminConfig,
    handlers: HashMap<String, AdminHandler>,
}

impl AdminApi {
    pub fn new(config: AdminConfig) -> Self {
        let mut api = Self {
            config,
            handlers: HashMap::new(),
        };
        api.register_default_handlers();
        api
    }

    fn register_default_handlers(&mut self) {
        // GET /health
        self.register("GET /health", Box::new(|_req| {
            AdminResponse::ok(r#"{"status":"ok"}"#)
        }));

        // GET /info
        self.register("GET /info", Box::new(|_req| {
            AdminResponse::ok(&format!(
                r#"{{"version":"{}","name":"celrix"}}"#,
                env!("CARGO_PKG_VERSION")
            ))
        }));

        // GET /stats
        self.register("GET /stats", Box::new(|_req| {
            AdminResponse::ok(r#"{"ops_total":0,"keys":0,"memory_bytes":0}"#)
        }));

        // POST /config/reload
        self.register("POST /config/reload", Box::new(|_req| {
            AdminResponse::ok(r#"{"reloaded":true}"#)
        }));

        // POST /cache/flush
        self.register("POST /cache/flush", Box::new(|_req| {
            AdminResponse::ok(r#"{"flushed":true}"#)
        }));

        // GET /debug/pprof
        self.register("GET /debug/pprof", Box::new(|_req| {
            AdminResponse::ok(r#"{"profile":"not available in release"}"#)
        }));
    }

    /// Register a handler
    pub fn register(&mut self, route: &str, handler: AdminHandler) {
        self.handlers.insert(route.to_string(), handler);
    }

    /// Handle a request
    pub fn handle(&self, req: &AdminRequest) -> AdminResponse {
        // Check auth if required
        if self.config.require_auth {
            let auth_header = req.get_header("authorization");
            let expected = self.config.api_key.as_ref().map(|k| format!("Bearer {}", k));

            if auth_header != expected.as_ref() {
                return AdminResponse::unauthorized();
            }
        }

        // Find handler
        let route = format!("{} {}", req.method, req.path);
        if let Some(handler) = self.handlers.get(&route) {
            handler(req)
        } else {
            AdminResponse::not_found()
        }
    }

    /// Get registered routes
    pub fn routes(&self) -> Vec<String> {
        self.handlers.keys().cloned().collect()
    }
}

impl Default for AdminApi {
    fn default() -> Self {
        Self::new(AdminConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_admin_api() {
        let api = AdminApi::default();

        let req = AdminRequest::new("GET", "/health");
        let resp = api.handle(&req);

        assert_eq!(resp.status, 200);
        assert!(resp.body.contains("ok"));
    }

    #[test]
    fn test_admin_auth() {
        let config = AdminConfig::default().with_auth("secret123");
        let api = AdminApi::new(config);

        // Without auth
        let req = AdminRequest::new("GET", "/health");
        let resp = api.handle(&req);
        assert_eq!(resp.status, 401);

        // With auth
        let req = AdminRequest::new("GET", "/health")
            .with_header("Authorization", "Bearer secret123");
        let resp = api.handle(&req);
        assert_eq!(resp.status, 200);
    }

    #[test]
    fn test_not_found() {
        let api = AdminApi::default();
        let req = AdminRequest::new("GET", "/nonexistent");
        let resp = api.handle(&req);
        assert_eq!(resp.status, 404);
    }
}
