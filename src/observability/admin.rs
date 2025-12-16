//! Admin HTTP API

use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct AdminConfig {
    pub port: u16,
    pub enabled: bool,
    pub require_auth: bool,
    pub api_key: Option<String>,
}

impl Default for AdminConfig {
    fn default() -> Self {
        Self { port: 9090, enabled: true, require_auth: false, api_key: None }
    }
}

impl AdminConfig {
    pub fn with_auth(mut self, key: &str) -> Self {
        self.require_auth = true;
        self.api_key = Some(key.to_string());
        self
    }
}

#[derive(Debug, Clone)]
pub struct AdminResponse {
    pub status: u16,
    pub body: String,
}

impl AdminResponse {
    pub fn ok(body: &str) -> Self { Self { status: 200, body: body.to_string() } }
    pub fn error(status: u16, msg: &str) -> Self { Self { status, body: format!(r#"{{"error":"{}"}}"#, msg) } }
    pub fn not_found() -> Self { Self::error(404, "Not found") }
    pub fn unauthorized() -> Self { Self::error(401, "Unauthorized") }
}

pub type AdminHandler = Box<dyn Fn(&AdminRequest) -> AdminResponse + Send + Sync>;

#[derive(Debug, Clone)]
pub struct AdminRequest {
    pub method: String,
    pub path: String,
    pub headers: HashMap<String, String>,
}

impl AdminRequest {
    pub fn new(method: &str, path: &str) -> Self {
        Self { method: method.to_uppercase(), path: path.to_string(), headers: HashMap::new() }
    }
    pub fn with_header(mut self, k: &str, v: &str) -> Self {
        self.headers.insert(k.to_lowercase(), v.to_string());
        self
    }
}

pub struct AdminApi {
    config: AdminConfig,
    handlers: HashMap<String, AdminHandler>,
}

impl AdminApi {
    pub fn new(config: AdminConfig) -> Self {
        let mut api = Self { config, handlers: HashMap::new() };
        api.register("GET /health", Box::new(|_| AdminResponse::ok(r#"{"status":"ok"}"#)));
        api.register("GET /info", Box::new(|_| AdminResponse::ok(&format!(r#"{{"version":"{}"}}"#, env!("CARGO_PKG_VERSION")))));
        api
    }

    pub fn register(&mut self, route: &str, handler: AdminHandler) {
        self.handlers.insert(route.to_string(), handler);
    }

    pub fn handle(&self, req: &AdminRequest) -> AdminResponse {
        if self.config.require_auth {
            if req.headers.get("authorization") != self.config.api_key.as_ref().map(|k| format!("Bearer {}", k)).as_ref() {
                return AdminResponse::unauthorized();
            }
        }
        let route = format!("{} {}", req.method, req.path);
        self.handlers.get(&route).map(|h| h(req)).unwrap_or_else(AdminResponse::not_found)
    }
}

impl Default for AdminApi {
    fn default() -> Self { Self::new(AdminConfig::default()) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_admin_api() {
        let api = AdminApi::default();
        let resp = api.handle(&AdminRequest::new("GET", "/health"));
        assert_eq!(resp.status, 200);
    }
}
