use std::collections::{BTreeMap, HashSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use pgsteward_core::admission::ClientLimit;
use pgsteward_core::auth::{AuthMethod, ClientCredentials};
use pgsteward_core::scram::ScramVerifier;
use serde::Deserialize;

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("config syntax error: {0}")]
    Syntax(#[from] toml::de::Error),
    #[error("invalid config at `{path}`: {message}")]
    Invalid { path: String, message: String },
}

impl ConfigError {
    fn invalid(path: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Invalid {
            path: path.into(),
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Proxy,
    Coordinator,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeConfig {
    pub node: NodeSection,
    #[serde(default)]
    pub client: BTreeMap<String, ClientSection>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientSection {
    pub verifier: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeSection {
    pub role: Role,
    pub listen: SocketAddr,
    pub coordinator: String,
    pub max_client_connections: u32,
    pub tls: Option<TlsSection>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsSection {
    pub cert: PathBuf,
    pub key: PathBuf,
}

impl NodeConfig {
    pub fn parse(text: &str) -> Result<Self, ConfigError> {
        let config: Self = toml::from_str(text)?;
        config.validate()?;
        Ok(config)
    }

    /// The users this node authenticates, read from the `[client."…"]` sections.
    /// Verifiers are what PostgreSQL stores in `pg_authid.rolpassword`, so that
    /// no plain password is written into a file this node reads.
    pub fn client_credentials(&self) -> Result<ClientCredentials, ConfigError> {
        let mut credentials = ClientCredentials::new();
        for (user, client) in &self.client {
            let verifier = client.verifier.parse::<ScramVerifier>().map_err(|source| {
                ConfigError::invalid(format!("client.\"{user}\".verifier"), source.to_string())
            })?;
            credentials.insert(user.clone(), AuthMethod::ScramSha256(verifier));
        }
        Ok(credentials)
    }

    /// How many client connections this node accepts at once.
    #[must_use]
    pub fn client_limit(&self) -> ClientLimit {
        ClientLimit::new(self.node.max_client_connections as usize)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        if self.node.max_client_connections == 0 {
            return Err(ConfigError::invalid(
                "node.max_client_connections",
                "must be at least 1",
            ));
        }
        if self.node.coordinator.trim().is_empty() {
            return Err(ConfigError::invalid(
                "node.coordinator",
                "must name one coordinator entry point",
            ));
        }
        if self.client.keys().any(|user| user.trim().is_empty()) {
            return Err(ConfigError::invalid(
                "client",
                "a client section must name the user it holds a verifier for",
            ));
        }
        self.client_credentials()?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PoolMode {
    Session,
    Transaction,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterConfig {
    pub cluster: ClusterSection,
    #[serde(default)]
    pub instance: Vec<InstanceSection>,
    #[serde(default)]
    pub tenant: BTreeMap<String, TenantSection>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterSection {
    pub pool_mode: PoolMode,
    #[serde(with = "humantime_serde")]
    pub grant_ttl: Duration,
    #[serde(with = "humantime_serde")]
    pub arbitration_interval: Duration,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstanceSection {
    pub name: String,
    #[serde(default = "default_weight")]
    pub weight: u32,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TenantSection {
    pub instances: Vec<String>,
    #[serde(default)]
    pub min: u32,
    pub max: Option<u32>,
    #[serde(default = "default_weight")]
    pub weight: u32,
}

fn default_weight() -> u32 {
    1
}

impl ClusterConfig {
    pub fn parse(text: &str) -> Result<Self, ConfigError> {
        let config: Self = toml::from_str(text)?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        self.validate_cluster_section()?;
        let instance_names = self.validate_instances()?;
        for (name, tenant) in &self.tenant {
            validate_tenant(name, tenant, &instance_names)?;
        }
        Ok(())
    }

    fn validate_cluster_section(&self) -> Result<(), ConfigError> {
        if self.cluster.grant_ttl.is_zero() {
            return Err(ConfigError::invalid(
                "cluster.grant_ttl",
                "must be longer than 0",
            ));
        }
        if self.cluster.arbitration_interval.is_zero() {
            return Err(ConfigError::invalid(
                "cluster.arbitration_interval",
                "must be longer than 0",
            ));
        }
        if self.cluster.arbitration_interval >= self.cluster.grant_ttl {
            return Err(ConfigError::invalid(
                "cluster.arbitration_interval",
                format!(
                    "must be shorter than cluster.grant_ttl ({:?})",
                    self.cluster.grant_ttl
                ),
            ));
        }
        Ok(())
    }

    fn validate_instances(&self) -> Result<HashSet<&str>, ConfigError> {
        if self.instance.is_empty() {
            return Err(ConfigError::invalid(
                "instance",
                "at least one [[instance]] is required",
            ));
        }
        let mut names = HashSet::new();
        for (index, instance) in self.instance.iter().enumerate() {
            let path = format!("instance[{index}]");
            if instance.name.trim().is_empty() {
                return Err(ConfigError::invalid(
                    format!("{path}.name"),
                    "must not be empty",
                ));
            }
            if instance.weight == 0 {
                return Err(ConfigError::invalid(
                    format!("{path}.weight"),
                    "must be at least 1",
                ));
            }
            if !names.insert(instance.name.as_str()) {
                return Err(ConfigError::invalid(
                    format!("{path}.name"),
                    format!("instance `{}` is declared more than once", instance.name),
                ));
            }
        }
        Ok(names)
    }
}

fn validate_tenant(
    name: &str,
    tenant: &TenantSection,
    instance_names: &HashSet<&str>,
) -> Result<(), ConfigError> {
    let path = format!("tenant.\"{name}\"");
    if name.is_empty() {
        return Err(ConfigError::invalid(
            "tenant",
            "tenant name must not be empty",
        ));
    }
    if tenant.instances.is_empty() {
        return Err(ConfigError::invalid(
            format!("{path}.instances"),
            "must list at least one instance",
        ));
    }
    let mut seen = HashSet::new();
    for instance in &tenant.instances {
        if !instance_names.contains(instance.as_str()) {
            return Err(ConfigError::invalid(
                format!("{path}.instances"),
                format!("instance `{instance}` is not declared in [[instance]]"),
            ));
        }
        if !seen.insert(instance.as_str()) {
            return Err(ConfigError::invalid(
                format!("{path}.instances"),
                format!("instance `{instance}` is listed more than once"),
            ));
        }
    }
    if tenant.weight == 0 {
        return Err(ConfigError::invalid(
            format!("{path}.weight"),
            "must be at least 1",
        ));
    }
    if let Some(max) = tenant.max
        && tenant.min > max
    {
        return Err(ConfigError::invalid(
            format!("{path}.min"),
            format!("min ({}) must not exceed max ({max})", tenant.min),
        ));
    }
    Ok(())
}
