//! A versioned, per-node bundle of xDS resources.
//!
//! Mirrors `pkg/cache/v3/snapshot.go`. A `Snapshot` carries one resource
//! map per type URL, each tagged with a single version string. The
//! `SnapshotCache` stores one `Snapshot` per node-hash and serves it to
//! every stream that node opens.

use std::collections::HashMap;
use std::sync::Arc;

use crate::resource::{DynResource, XdsResource};

/// A resource map for a single type URL, plus that type's version.
#[derive(Clone, Default)]
pub struct ResourceMap {
    pub items: HashMap<String, Arc<dyn DynResource>>,
    pub version: String,
}

/// Versioned bundle of resources for a single Envoy node-hash.
#[derive(Clone, Default)]
pub struct Snapshot {
    by_type: HashMap<&'static str, ResourceMap>,
}

impl Snapshot {
    pub fn builder() -> SnapshotBuilder {
        SnapshotBuilder::default()
    }

    pub fn resources(&self, type_url: &str) -> Option<&ResourceMap> {
        self.by_type.get(type_url)
    }

    pub fn version(&self, type_url: &str) -> &str {
        self.by_type
            .get(type_url)
            .map(|m| m.version.as_str())
            .unwrap_or("")
    }

    /// All resources for `type_url`, in arbitrary order, packed as Anys.
    /// Used by the SotW server to build a `DiscoveryResponse`.
    pub(crate) fn dyn_resources(&self, type_url: &str) -> Vec<Arc<dyn DynResource>> {
        self.by_type
            .get(type_url)
            .map(|m| m.items.values().cloned().collect())
            .unwrap_or_default()
    }
}

#[derive(Default)]
pub struct SnapshotBuilder {
    by_type: HashMap<&'static str, ResourceMap>,
}

impl SnapshotBuilder {
    /// Add one resource to the snapshot.
    pub fn with<T: XdsResource>(mut self, resource: T) -> Self {
        let entry = self.by_type.entry(T::TYPE_URL).or_default();
        let name = XdsResource::name(&resource).to_string();
        entry.items.insert(name, Arc::new(resource));
        self
    }

    /// Set the version string for a given type URL. Must be called for
    /// every type URL that has resources; uncalled types default to "0".
    pub fn version(mut self, type_url: &'static str, version: impl Into<String>) -> Self {
        let entry = self.by_type.entry(type_url).or_default();
        entry.version = version.into();
        self
    }

    /// Convenience: set the same version for every populated type URL.
    pub fn version_all(mut self, version: impl Into<String>) -> Self {
        let v = version.into();
        for entry in self.by_type.values_mut() {
            entry.version = v.clone();
        }
        self
    }

    pub fn build(mut self) -> Snapshot {
        // Default any missing version to "0" so caches don't have to
        // special-case it later.
        for entry in self.by_type.values_mut() {
            if entry.version.is_empty() {
                entry.version = "0".to_string();
            }
        }
        Snapshot {
            by_type: self.by_type,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resource::type_url;
    use envoy_proto::envoy::config::cluster::v3::Cluster;

    #[test]
    fn builds_and_serves() {
        let snap = Snapshot::builder()
            .with(Cluster {
                name: "a".into(),
                ..Default::default()
            })
            .with(Cluster {
                name: "b".into(),
                ..Default::default()
            })
            .version_all("v1")
            .build();

        let map = snap.resources(type_url::CLUSTER).expect("cluster map");
        assert_eq!(map.items.len(), 2);
        assert_eq!(map.version, "v1");
        assert_eq!(snap.version(type_url::CLUSTER), "v1");
        assert_eq!(snap.version(type_url::LISTENER), "");
    }
}
