use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::time::Duration;

use pgsteward_core::admission::ClientLimit;
use pgsteward_core::allocation::InstanceId;
use pgsteward_core::auth::{AuthMethod, ClientCredentials};
use pgsteward_core::grant::TenantPolicy;
use pgsteward_core::policy::{Policies, TenantRule};
use pgsteward_core::scram::ScramVerifier;
use pgsteward_core::server::ServerCredentials;
use pgsteward_core::tenant::TenantId;
use pgsteward_core::tls::{ClientTls, ServerTls, SslMode, TlsError};
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
    #[serde(default)]
    pub server: BTreeMap<String, ServerSection>,
    pub monitor: Option<MonitorSection>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerSection {
    pub password: String,
}

impl fmt::Debug for ServerSection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServerSection")
            .field("password", &"<redacted>")
            .finish()
    }
}

/// The login this node uses to read `max_connections` and to count the foreign
/// connections of each instance. Counting other users' backends needs the
/// `pg_monitor` role or a superuser.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MonitorSection {
    pub user: String,
    #[serde(default = "default_monitor_database")]
    pub database: String,
}

fn default_monitor_database() -> String {
    "postgres".to_owned()
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
    pub server_tls: Option<ServerTlsSection>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsSection {
    pub cert: PathBuf,
    pub key: PathBuf,
}

/// How far this node goes to encrypt the connections it opens to an instance.
/// The modes are libpq's `sslmode`, and `root_cert` is what `verify-ca` and
/// `verify-full` check the certificate against.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerTlsSection {
    pub mode: SslModeSection,
    pub root_cert: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SslModeSection {
    Disable,
    Prefer,
    Require,
    VerifyCa,
    VerifyFull,
}

impl From<SslModeSection> for SslMode {
    fn from(mode: SslModeSection) -> Self {
        match mode {
            SslModeSection::Disable => Self::Disable,
            SslModeSection::Prefer => Self::Prefer,
            SslModeSection::Require => Self::Require,
            SslModeSection::VerifyCa => Self::VerifyCa,
            SslModeSection::VerifyFull => Self::VerifyFull,
        }
    }
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

    /// What this node terminates client TLS with, or none when `[node.tls]` is
    /// absent and clients are told this node does not encrypt.
    pub fn client_tls(&self) -> Result<Option<ClientTls>, ConfigError> {
        let Some(tls) = &self.node.tls else {
            return Ok(None);
        };
        let certificates = read_pem(&tls.cert, "node.tls.cert")?;
        let key = read_pem(&tls.key, "node.tls.key")?;
        ClientTls::from_pem(&certificates, &key)
            .map(Some)
            .map_err(|source| match source {
                TlsError::Certificate(_) | TlsError::NoCertificate => {
                    ConfigError::invalid("node.tls.cert", source.to_string())
                }
                TlsError::PrivateKey(_) => ConfigError::invalid("node.tls.key", source.to_string()),
                other => ConfigError::invalid("node.tls", other.to_string()),
            })
    }

    /// How this node encrypts the connections it opens to an instance. Without
    /// a `[node.server_tls]` section it asks for encryption and carries on in
    /// the clear when the instance does not offer it, as libpq does.
    pub fn server_tls(&self) -> Result<ServerTls, ConfigError> {
        let Some(section) = &self.node.server_tls else {
            return ServerTls::new(SslMode::default(), None).map_err(server_tls_error);
        };
        let mode = SslMode::from(section.mode);
        let root = match (&section.root_cert, mode) {
            (Some(path), SslMode::VerifyCa | SslMode::VerifyFull) => {
                Some(read_pem(path, "node.server_tls.root_cert")?)
            }
            (Some(_), mode) => {
                return Err(ConfigError::invalid(
                    "node.server_tls.root_cert",
                    format!(
                        "`{mode}` never reads a root certificate; use verify-ca or verify-full"
                    ),
                ));
            }
            (None, SslMode::VerifyCa | SslMode::VerifyFull) => {
                return Err(ConfigError::invalid(
                    "node.server_tls.root_cert",
                    format!("`{mode}` verifies the certificate and needs a root certificate"),
                ));
            }
            (None, _) => None,
        };
        ServerTls::new(mode, root.as_deref()).map_err(server_tls_error)
    }

    /// A server connection for a tenant logs in as the tenant's user to the
    /// tenant's database, with the password of that user's `[server."…"]`
    /// section, or none when the user has no section.
    #[must_use]
    pub fn server_credentials(&self, tenant: &TenantId) -> ServerCredentials {
        ServerCredentials {
            user: tenant.user().to_owned(),
            database: tenant.database().to_owned(),
            password: self.password(tenant.user()),
        }
    }

    #[must_use]
    pub fn monitor_credentials(&self) -> Option<ServerCredentials> {
        let monitor = self.monitor.as_ref()?;
        Some(ServerCredentials {
            user: monitor.user.clone(),
            database: monitor.database.clone(),
            password: self.password(&monitor.user),
        })
    }

    fn password(&self, user: &str) -> Option<String> {
        self.server.get(user).map(|server| server.password.clone())
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
        if self.server.keys().any(|user| user.trim().is_empty()) {
            return Err(ConfigError::invalid(
                "server",
                "a server section must name the user it holds a password for",
            ));
        }
        if let Some(monitor) = &self.monitor
            && monitor.user.trim().is_empty()
        {
            return Err(ConfigError::invalid("monitor.user", "must not be empty"));
        }
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

fn server_tls_error(source: TlsError) -> ConfigError {
    match source {
        TlsError::RootCertificate(_) | TlsError::NoRootCertificate | TlsError::Verifier(_) => {
            ConfigError::invalid("node.server_tls.root_cert", source.to_string())
        }
        other => ConfigError::invalid("node.server_tls", other.to_string()),
    }
}

fn read_pem(path: &Path, key: &str) -> Result<Vec<u8>, ConfigError> {
    std::fs::read(path).map_err(|source| {
        ConfigError::invalid(key, format!("cannot read `{}`: {source}", path.display()))
    })
}

fn default_weight() -> u32 {
    1
}

const DEFAULT_PORT: u16 = 5432;

impl InstanceSection {
    #[must_use]
    pub fn id(&self) -> InstanceId {
        InstanceId::new(&self.name)
    }

    /// The name is resolved as the address to connect to, on port 5432 unless
    /// it names another one.
    #[must_use]
    pub fn address(&self) -> String {
        if self
            .name
            .rsplit_once(':')
            .is_some_and(|(_, port)| port.parse::<u16>().is_ok())
        {
            self.name.clone()
        } else {
            format!("{}:{DEFAULT_PORT}", self.name)
        }
    }
}

impl ClusterConfig {
    pub fn parse(text: &str) -> Result<Self, ConfigError> {
        let config: Self = toml::from_str(text)?;
        config.validate()?;
        Ok(config)
    }

    /// The tenant rules as the coordinator applies them. A tenant without `max`
    /// may take up to the whole total budget of an instance, which the
    /// allocator enforces on its own.
    #[must_use]
    pub fn policies(&self) -> Policies {
        let policies = self
            .instance
            .iter()
            .fold(Policies::new(), |policies, instance| {
                policies.instance(instance.id(), nonzero(instance.weight))
            });
        self.tenant
            .iter()
            .fold(policies, |policies, (key, tenant)| {
                policies.tenant(
                    key.clone(),
                    TenantRule {
                        instances: tenant.instances.iter().map(InstanceId::new).collect(),
                        policy: TenantPolicy {
                            min: tenant.min,
                            max: tenant.max.unwrap_or(u32::MAX),
                            weight: nonzero(tenant.weight),
                        },
                    },
                )
            })
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

fn nonzero(weight: u32) -> NonZeroU32 {
    NonZeroU32::new(weight).expect("weights are validated to be at least 1")
}
