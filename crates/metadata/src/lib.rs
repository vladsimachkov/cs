// Metadata mutations and the Raft proposal path return `tonic::Status`, which is large
// enough to trip `clippy::result_large_err`; it cannot be shrunk from our side.
#![allow(clippy::result_large_err)]

pub mod command;
pub mod service;
pub mod state_machine;

#[cfg(feature = "metadata-raft")]
pub mod raft;
#[cfg(feature = "metadata-raft-rocksdb")]
pub mod raft_storage;

use anyhow::Result;
use coldstore_common::config::MetadataConfig;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use tonic::transport::Server;
use tracing::info;

pub async fn run(config: MetadataConfig) -> Result<()> {
    let addr = config.listen.parse()?;
    let cluster_nodes = parse_cluster_nodes(&config.cluster)?;
    let consensus_mode = config.consensus_mode.clone();

    let effective_config = MetadataConfig {
        consensus_mode: consensus_mode.clone(),
        ..config.clone()
    };
    enforce_consensus_mode(&effective_config, &cluster_nodes)?;

    let snapshot_path = default_snapshot_path(&config);
    let metadata_service = match consensus_mode {
        coldstore_common::config::MetadataConsensusMode::Standalone => {
            #[cfg(feature = "metadata-raft")]
            {
                let raft_backend = std::sync::Arc::new(crate::raft::RaftMetadataBackend::new());
                service::MetadataServiceImpl::new_with_snapshot_and_raft_backend(
                    &effective_config,
                    snapshot_path.clone(),
                    raft_backend,
                )
                .await?
            }

            #[cfg(not(feature = "metadata-raft"))]
            {
                anyhow::bail!(
                    "consensus_mode=standalone requires `metadata-raft` feature (enable via metadata crate default feature)",
                )
            }
        }
        coldstore_common::config::MetadataConsensusMode::LocalRaft => {
            #[cfg(feature = "metadata-raft")]
            {
                let raft_backend = std::sync::Arc::new(crate::raft::RaftMetadataBackend::new());
                service::MetadataServiceImpl::new_with_snapshot_and_raft_backend(
                    &effective_config,
                    snapshot_path.clone(),
                    raft_backend,
                )
                .await?
            }

            #[cfg(not(feature = "metadata-raft"))]
            {
                anyhow::bail!(
                    "consensus_mode=local_raft requires `metadata-raft` feature (enable via metadata crate default feature)",
                )
            }
        }
        coldstore_common::config::MetadataConsensusMode::PersistentRaft => {
            #[cfg(feature = "metadata-raft-rocksdb")]
            {
                let raft_storage_path = effective_config
                    .raft_state_path
                    .clone()
                    .unwrap_or_else(|| default_raft_path(&effective_config));
                let raft_backend =
                    std::sync::Arc::new(crate::raft::RaftMetadataBackend::new_with_storage(
                        Path::new(&raft_storage_path),
                    )?);
                service::MetadataServiceImpl::new_with_snapshot_and_raft_backend(
                    &effective_config,
                    snapshot_path.clone(),
                    raft_backend,
                )
                .await?
            }
            #[cfg(not(feature = "metadata-raft-rocksdb"))]
            {
                anyhow::bail!(
                    "metadata consensus_mode=persistent_raft requires metadata-raft-rocksdb feature"
                )
            }
        }
    };

    info!(
        "Metadata 节点 {} 启动在 {}，snapshot={}",
        config.node_id,
        addr,
        snapshot_path.display()
    );
    info!(
        "Metadata raft consensus mode: {:?}, cluster_size={}, cluster_nodes={:?}",
        effective_config.consensus_mode,
        cluster_nodes.len(),
        cluster_nodes
    );

    Server::builder()
        .add_service(
            coldstore_proto::metadata::metadata_service_server::MetadataServiceServer::new(
                metadata_service,
            ),
        )
        .serve(addr)
        .await?;

    Ok(())
}

fn default_snapshot_path(config: &MetadataConfig) -> PathBuf {
    PathBuf::from(&config.data_path).join(format!("node-{}-snapshot.bin", config.node_id))
}

fn default_raft_path(config: &MetadataConfig) -> String {
    Path::new(&config.data_path)
        .join(format!("node-{}/raft", config.node_id))
        .to_string_lossy()
        .to_string()
}

#[derive(Debug)]
struct MetadataClusterNode {
    node_id: u64,
    listen: String,
}

fn parse_cluster_nodes(cluster: &str) -> Result<Vec<MetadataClusterNode>> {
    let mut parsed = Vec::new();
    let mut node_ids = HashSet::new();
    let mut listen_addrs = HashSet::new();

    for raw_entry in cluster
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
    {
        let (node_id, listen) = raw_entry.split_once(':').ok_or_else(|| {
            anyhow::anyhow!("invalid cluster entry '{raw_entry}', expected node_id:listen_addr")
        })?;
        let node_id = node_id
            .trim()
            .parse::<u64>()
            .map_err(|_| anyhow::anyhow!("invalid node id in cluster entry '{raw_entry}'"))?;
        let listen = normalize_listen_addr(listen);
        if listen.is_empty() {
            anyhow::bail!("invalid cluster entry '{raw_entry}', listen address is empty");
        }

        if !node_ids.insert(node_id) {
            anyhow::bail!("duplicated metadata node_id '{node_id}' in cluster config");
        }
        if !listen_addrs.insert(listen.clone()) {
            anyhow::bail!("duplicated metadata listen address '{listen}' in cluster config");
        }

        parsed.push(MetadataClusterNode { node_id, listen });
    }

    if parsed.is_empty() {
        anyhow::bail!("metadata cluster configuration is invalid or empty; expected node_id:listen_addr entries");
    }

    Ok(parsed)
}

fn normalize_listen_addr(listen: &str) -> String {
    strip_listen_scheme(listen)
        .trim()
        .trim_end_matches('/')
        .trim()
        .to_string()
}

fn strip_listen_scheme(entry: &str) -> &str {
    entry
        .trim()
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .trim_start_matches("grpc://")
}

fn enforce_consensus_mode(
    config: &MetadataConfig,
    cluster_nodes: &[MetadataClusterNode],
) -> Result<()> {
    let cluster_size = cluster_nodes.len();
    let local_node = cluster_nodes
        .iter()
        .find(|node| node.node_id == config.node_id)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "metadata node_id {} not present in cluster definition",
                config.node_id
            )
        })?;

    let local_listen = normalize_listen_addr(&config.listen);
    if !listen_addrs_match(&local_listen, &local_node.listen) {
        anyhow::bail!(
            "metadata listen mismatch: node_id {} uses {} but cluster entry expects {}",
            config.node_id,
            config.listen,
            local_node.listen,
        );
    }

    let is_clustered = cluster_size > 1;
    if cluster_size == 0 {
        anyhow::bail!("metadata cluster_size must be greater than 0");
    }
    match (config.consensus_mode.clone(), is_clustered) {
        (coldstore_common::config::MetadataConsensusMode::Standalone, true) => {
            anyhow::bail!(
                "standalone mode only supports a single metadata node; distributed metadata clusters require a real Raft runtime"
            )
        }
        (coldstore_common::config::MetadataConsensusMode::LocalRaft, true) => {
            anyhow::bail!(
                "local_raft mode is not valid for multi-node metadata clusters; distributed metadata clusters require a real Raft runtime"
            )
        }
        (coldstore_common::config::MetadataConsensusMode::PersistentRaft, true) => {
            anyhow::bail!(
                "persistent_raft multi-node metadata clusters require the distributed Raft runtime; this build only supports single-node persistent_raft safely"
            )
        }
        _ => Ok(()),
    }
}

fn listen_addrs_match(local_listen: &str, cluster_listen: &str) -> bool {
    let cluster_listen = normalize_listen_addr(cluster_listen);
    if local_listen == cluster_listen {
        return true;
    }

    let Some((local_host, local_port)) = split_host_port(local_listen) else {
        return false;
    };
    let Some((_cluster_host, cluster_port)) = split_host_port(&cluster_listen) else {
        return false;
    };

    local_port == cluster_port && is_wildcard_host(local_host)
}

fn split_host_port(addr: &str) -> Option<(&str, &str)> {
    let (host, port) = addr.rsplit_once(':')?;
    if host.is_empty() || port.is_empty() {
        return None;
    }
    Some((host.trim_matches(['[', ']']), port))
}

fn is_wildcard_host(host: &str) -> bool {
    matches!(host, "0.0.0.0" | "::" | "*")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_snapshot_path_uses_configured_data_path_and_node_id() {
        let config = MetadataConfig {
            node_id: 7,
            data_path: "/tmp/coldstore-meta".into(),
            ..MetadataConfig::default()
        };

        assert_eq!(
            default_snapshot_path(&config),
            PathBuf::from("/tmp/coldstore-meta/node-7-snapshot.bin")
        );
    }

    #[test]
    fn default_raft_path_is_scoped_by_node_id() {
        let config = MetadataConfig {
            node_id: 7,
            data_path: "/tmp/coldstore-meta".into(),
            ..MetadataConfig::default()
        };

        assert_eq!(
            default_raft_path(&config),
            "/tmp/coldstore-meta/node-7/raft"
        );
    }

    #[test]
    fn parse_cluster_nodes_rejects_invalid_entry() {
        let err = parse_cluster_nodes("bad-entry,2:").expect_err("invalid node must fail");
        assert!(err.to_string().contains("invalid cluster entry"),);
    }

    #[test]
    fn parse_cluster_nodes_rejects_duplicate_node_id() {
        assert!(parse_cluster_nodes("1:127.0.0.1:21001,1:127.0.0.1:21002")
            .expect_err("duplicate node id must fail")
            .to_string()
            .contains("duplicated metadata node_id"),);
    }

    #[test]
    fn parse_cluster_nodes_rejects_duplicate_normalized_listen_addr() {
        assert!(
            parse_cluster_nodes("1:grpc://127.0.0.1:21001,2:http://127.0.0.1:21001")
                .expect_err("duplicate normalized listen addr must fail")
                .to_string()
                .contains("duplicated metadata listen address"),
        );
    }

    #[test]
    fn enforce_consensus_mode_requires_local_node_in_cluster() {
        let config = MetadataConfig {
            node_id: 9,
            consensus_mode: coldstore_common::config::MetadataConsensusMode::LocalRaft,
            cluster: "1:127.0.0.1:21001".into(),
            ..MetadataConfig::default()
        };
        let cluster_nodes = parse_cluster_nodes("1:127.0.0.1:21001").expect("cluster parse");
        let err =
            enforce_consensus_mode(&config, &cluster_nodes).expect_err("local node not in cluster");
        assert!(err.to_string().contains("metadata node_id 9 not present"));
    }

    #[test]
    fn enforce_consensus_mode_rejects_cluster_with_localraft() {
        let config = MetadataConfig {
            node_id: 1,
            listen: "127.0.0.1:21001".into(),
            consensus_mode: coldstore_common::config::MetadataConsensusMode::LocalRaft,
            cluster: "1:127.0.0.1:21001,2:127.0.0.1:21002".into(),
            ..MetadataConfig::default()
        };
        let cluster_nodes =
            parse_cluster_nodes("1:127.0.0.1:21001,2:127.0.0.1:21002").expect("cluster parse");
        let err = enforce_consensus_mode(&config, &cluster_nodes)
            .expect_err("local raft in cluster should reject");
        assert!(err.to_string().contains("local_raft mode"));
    }

    #[test]
    fn enforce_consensus_mode_rejects_two_node_persistent_raft_cluster_until_runtime_exists() {
        let config = MetadataConfig {
            node_id: 1,
            listen: "127.0.0.1:21001".into(),
            consensus_mode: coldstore_common::config::MetadataConsensusMode::PersistentRaft,
            cluster: "1:127.0.0.1:21001,2:127.0.0.1:21002".into(),
            ..MetadataConfig::default()
        };
        let cluster_nodes = parse_cluster_nodes(&config.cluster).expect("cluster parse");
        let err = enforce_consensus_mode(&config, &cluster_nodes)
            .expect_err("two-node persistent raft should reject");
        assert!(err.to_string().contains("distributed Raft runtime"));
    }

    #[test]
    fn enforce_consensus_mode_rejects_even_sized_persistent_raft_cluster_until_runtime_exists() {
        let config = MetadataConfig {
            node_id: 1,
            listen: "127.0.0.1:21001".into(),
            consensus_mode: coldstore_common::config::MetadataConsensusMode::PersistentRaft,
            cluster: "1:127.0.0.1:21001,2:127.0.0.1:21002,3:127.0.0.1:21003,4:127.0.0.1:21004"
                .into(),
            ..MetadataConfig::default()
        };
        let cluster_nodes = parse_cluster_nodes(&config.cluster).expect("cluster parse");
        let err = enforce_consensus_mode(&config, &cluster_nodes)
            .expect_err("even-sized persistent raft should reject");
        assert!(err.to_string().contains("distributed Raft runtime"));
    }

    #[test]
    fn enforce_consensus_mode_rejects_persistent_raft_cluster_until_distributed_runtime_exists() {
        let config = MetadataConfig {
            node_id: 1,
            listen: "127.0.0.1:21001".into(),
            consensus_mode: coldstore_common::config::MetadataConsensusMode::PersistentRaft,
            cluster: "1:127.0.0.1:21001,2:127.0.0.1:21002,3:127.0.0.1:21003".into(),
            ..MetadataConfig::default()
        };
        let cluster_nodes = parse_cluster_nodes(&config.cluster).expect("cluster parse");

        let err = enforce_consensus_mode(&config, &cluster_nodes)
            .expect_err("multi-node persistent raft must fail until distributed runtime exists");
        assert!(err.to_string().contains("distributed Raft runtime"));
    }

    #[test]
    fn enforce_consensus_mode_rejects_local_listen_mismatch() {
        let config = MetadataConfig {
            node_id: 1,
            listen: "127.0.0.1:21001".into(),
            consensus_mode: coldstore_common::config::MetadataConsensusMode::PersistentRaft,
            ..MetadataConfig::default()
        };
        let cluster_nodes =
            parse_cluster_nodes("1:127.0.0.1:31001,2:127.0.0.1:31002").expect("cluster parse");
        let err = enforce_consensus_mode(&config, &cluster_nodes)
            .expect_err("listen address mismatch must fail");
        assert!(err.to_string().contains("metadata listen mismatch"));
    }

    #[test]
    fn enforce_consensus_mode_normalizes_cluster_listen_prefix() {
        let config = MetadataConfig {
            node_id: 1,
            listen: "http://127.0.0.1:21001".into(),
            consensus_mode: coldstore_common::config::MetadataConsensusMode::PersistentRaft,
            ..MetadataConfig::default()
        };
        let cluster_nodes = parse_cluster_nodes("1:grpc://127.0.0.1:21001").expect("cluster parse");
        enforce_consensus_mode(&config, &cluster_nodes).expect("consistency accepted");
    }

    #[test]
    fn enforce_consensus_mode_allows_wildcard_bind_with_cluster_advertise_addr() {
        let config = MetadataConfig {
            cluster: "1:127.0.0.1:21001".into(),
            ..MetadataConfig::default()
        };
        let cluster_nodes = parse_cluster_nodes(&config.cluster).expect("cluster parse");

        enforce_consensus_mode(&config, &cluster_nodes)
            .expect("wildcard listen should match local advertised cluster address");
    }
}
