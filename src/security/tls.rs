//! TLS Configuration
//!
//! TLS 1.3 encryption support for client and cluster connections.

use std::path::PathBuf;

/// TLS configuration
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// Enable TLS
    pub enabled: bool,
    /// Certificate file path
    pub cert_file: PathBuf,
    /// Key file path
    pub key_file: PathBuf,
    /// CA file for client verification (mTLS)
    pub ca_file: Option<PathBuf>,
    /// Require client certificates (mTLS)
    pub require_client_cert: bool,
    /// Minimum TLS version (1.2 or 1.3)
    pub min_version: TlsVersion,
}

/// TLS version
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TlsVersion {
    Tls12,
    Tls13,
}

impl Default for TlsVersion {
    fn default() -> Self {
        Self::Tls13
    }
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            cert_file: PathBuf::from("certs/server.crt"),
            key_file: PathBuf::from("certs/server.key"),
            ca_file: None,
            require_client_cert: false,
            min_version: TlsVersion::Tls13,
        }
    }
}

impl TlsConfig {
    pub fn enabled(mut self) -> Self {
        self.enabled = true;
        self
    }

    pub fn with_cert(mut self, cert: PathBuf, key: PathBuf) -> Self {
        self.cert_file = cert;
        self.key_file = key;
        self
    }

    pub fn with_mtls(mut self, ca: PathBuf) -> Self {
        self.ca_file = Some(ca);
        self.require_client_cert = true;
        self
    }
}

/// TLS acceptor wrapper
pub struct TlsAcceptor {
    config: TlsConfig,
}

impl TlsAcceptor {
    pub fn new(config: TlsConfig) -> Self {
        Self { config }
    }

    pub fn config(&self) -> &TlsConfig {
        &self.config
    }

    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    pub fn requires_client_cert(&self) -> bool {
        self.config.require_client_cert
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tls_config() {
        let config = TlsConfig::default()
            .enabled()
            .with_cert(PathBuf::from("cert.pem"), PathBuf::from("key.pem"));

        assert!(config.enabled);
        assert_eq!(config.min_version, TlsVersion::Tls13);
    }

    #[test]
    fn test_mtls_config() {
        let config = TlsConfig::default()
            .enabled()
            .with_mtls(PathBuf::from("ca.pem"));

        assert!(config.require_client_cert);
        assert!(config.ca_file.is_some());
    }
}
