use std::time::Duration;

pub struct TransportConfig {
    pub tcp_port: u16,
    pub quic_port: u16,
    pub idle_connection_timeout: Duration,
}

impl TransportConfig {
    pub fn default_config() -> Self {
        Self {
            tcp_port: 9000,
            quic_port: 9000,
            idle_connection_timeout: Duration::from_secs(1500),
        }
    }

    pub fn with_ports(tcp_port: u16, quic_port: u16) -> Self {
        Self {
            tcp_port,
            quic_port,
            ..Self::default_config()
        }
    }

    pub fn tcp_listen_addr(&self) -> String {
        format!("/ip4/0.0.0.0/tcp/{}", self.tcp_port)
    }

    pub fn quic_listen_addr(&self) -> String {
        format!("/ip4/0.0.0.0/udp/{}/quic-v1", self.quic_port)
    }
}
