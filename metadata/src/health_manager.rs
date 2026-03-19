use std::time::Duration;

use reqwest::Client;

use crate::repo::MetadataRepo;

pub fn spawn_chunker_health_manager(repo: MetadataRepo, interval: Duration) {
    tokio::spawn(async move {
        let client = Client::new();
        loop {
            match repo.list_chunker_nodes().await {
                Ok(nodes) => {
                    for node in nodes {
                        let health_url = format!("{}/health", node.base_url.trim_end_matches('/'));
                        let result = client.get(&health_url).send().await;
                        match result {
                            Ok(response) if response.status().is_success() => {
                                if let Err(err) =
                                    repo.update_chunker_health(&node.node_id, true, None).await
                                {
                                    tracing::warn!(
                                        "failed to update healthy status for {}: {}",
                                        node.node_id,
                                        err
                                    );
                                }
                            }
                            Ok(response) => {
                                let msg = format!("health check returned {}", response.status());
                                if let Err(err) = repo
                                    .update_chunker_health(&node.node_id, false, Some(&msg))
                                    .await
                                {
                                    tracing::warn!(
                                        "failed to update unhealthy status for {}: {}",
                                        node.node_id,
                                        err
                                    );
                                }
                            }
                            Err(err) => {
                                let msg = err.to_string();
                                if let Err(update_err) = repo
                                    .update_chunker_health(&node.node_id, false, Some(&msg))
                                    .await
                                {
                                    tracing::warn!(
                                        "failed to update unhealthy status for {}: {}",
                                        node.node_id,
                                        update_err
                                    );
                                }
                            }
                        }
                    }
                }
                Err(err) => tracing::warn!("chunker health manager list failed: {}", err),
            }
            tokio::time::sleep(interval).await;
        }
    });
}
