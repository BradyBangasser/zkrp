//! Community keys: one shared key per community (an allowed email domain) per
//! epoch, handed out by auth with the credential (`zkcred::community_key`).
//! Apps seal what only their community should read with them, usually as the
//! key of a sealed topic (`crate::sealed`).

use zeroize::Zeroizing;

use crate::store::GhostStore;

const PREFIX: &str = "zkrp/community/";

fn key_for(epoch: u64) -> String {
    format!("{PREFIX}{epoch:020}")
}

/// Keep the keys from an enrollment, dropping ones whose epoch has passed.
pub fn save(store: &dyn GhostStore, keys: impl IntoIterator<Item = (u64, [u8; 32])>) {
    for (epoch, key) in keys {
        store.set(&key_for(epoch), key.to_vec());
    }
    let current = current_epoch(store);
    for (epoch, _) in all(store) {
        if epoch + 1 < current {
            store.delete(&key_for(epoch));
        }
    }
}

/// Every stored key, oldest epoch first.
pub fn all(store: &dyn GhostStore) -> Vec<(u64, Zeroizing<[u8; 32]>)> {
    store
        .list(PREFIX)
        .into_iter()
        .filter_map(|k| {
            let epoch = k.strip_prefix(PREFIX)?.parse().ok()?;
            let key: [u8; 32] = store.get(&k)?.try_into().ok()?;
            Some((epoch, Zeroizing::new(key)))
        })
        .collect()
}

/// The epoch it is now, by the stored credential's epoch length.
pub fn current_epoch(store: &dyn GhostStore) -> u64 {
    zkcred::epoch_at(crate::admit::now_unix(), crate::admit::epoch_secs(store))
}

/// The key to seal with now, if this device holds it.
pub fn current(store: &dyn GhostStore) -> Option<(u64, Zeroizing<[u8; 32]>)> {
    let now = current_epoch(store);
    all(store).into_iter().find(|(e, _)| *e == now)
}

/// Keys worth listening with now: last epoch's (for peers whose clock is just
/// behind), this one's and the next (for peers just ahead).
pub fn active(store: &dyn GhostStore) -> Vec<(u64, Zeroizing<[u8; 32]>)> {
    let now = current_epoch(store);
    all(store)
        .into_iter()
        .filter(|(e, _)| e + 1 >= now && *e <= now + 1)
        .collect()
}

pub fn clear(store: &dyn GhostStore) {
    for k in store.list(PREFIX) {
        store.delete(&k);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::MemoryStore;

    #[test]
    fn current_and_active_follow_the_clock() {
        let store = MemoryStore::new();
        let now = current_epoch(&store);
        save(&store, [(now - 3, [1; 32]), (now, [2; 32]), (now + 1, [3; 32])]);
        // Long-past epochs are pruned on save.
        assert_eq!(all(&store).len(), 2);
        assert_eq!(*current(&store).unwrap().1, [2; 32]);
        assert_eq!(active(&store).len(), 2);
        clear(&store);
        assert!(current(&store).is_none());
    }
}
