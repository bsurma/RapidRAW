use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, UNIX_EPOCH};
use tauri::{AppHandle, Emitter};

use super::client::{ImmichClient, MetadataEntry, is_asset_id};
use super::{Session, current_session};

const EDITS_KEY: &str = "rapidraw";
const STATE_FILE: &str = ".immich-sync.json";
const INTERVAL: Duration = Duration::from_secs(5);

#[derive(Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
struct SyncState {
    local_modified_ms: Option<u64>,
    local_hash: Option<String>,
    remote_updated_at: Option<String>,
    remote_rating: Option<u8>,
}

#[derive(Serialize, Clone, Copy, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct SyncStatus {
    pub offline: bool,
    pub pending: usize,
}

static STATUS: Lazy<Mutex<SyncStatus>> = Lazy::new(Default::default);
static WAKE: Lazy<tokio::sync::Notify> = Lazy::new(tokio::sync::Notify::new);

pub fn status() -> SyncStatus {
    *STATUS.lock().unwrap()
}

pub fn wake() {
    WAKE.notify_one();
}

pub fn mark_offline(app_handle: &AppHandle) {
    let pending = status().pending;
    report(
        app_handle,
        SyncStatus {
            offline: true,
            pending,
        },
    );
}

fn report(app_handle: &AppHandle, status: SyncStatus) {
    let changed = {
        let mut current = STATUS.lock().unwrap();
        let changed = *current != status;
        *current = status;
        changed
    };
    if changed {
        let _ = app_handle.emit("immich-sync", status);
    }
}

fn sidecar_of(source: &Path) -> PathBuf {
    crate::exif_processing::get_primary_sidecar_path(source)
}

fn state_file(source: &Path) -> PathBuf {
    source.with_file_name(STATE_FILE)
}

fn load_state(source: &Path) -> SyncState {
    fs::read_to_string(state_file(source))
        .ok()
        .and_then(|content| serde_json::from_str(&content).ok())
        .unwrap_or_default()
}

fn save_state(source: &Path, state: &SyncState) {
    if let Ok(json) = serde_json::to_string(state) {
        let _ = fs::write(state_file(source), json);
    }
}

fn modified_ms(path: &Path) -> Option<u64> {
    let modified = fs::metadata(path).ok()?.modified().ok()?;
    Some(modified.duration_since(UNIX_EPOCH).ok()?.as_millis() as u64)
}

fn read_sidecar(source: &Path) -> Option<Value> {
    serde_json::from_str(&fs::read_to_string(sidecar_of(source)).ok()?).ok()
}

fn shared(mut value: Value) -> Value {
    if let Some(map) = value.as_object_mut() {
        map.remove("exif");
    }
    value
}

fn rating_of(value: &Value) -> u8 {
    value
        .get("rating")
        .and_then(Value::as_u64)
        .unwrap_or(0)
        .min(5) as u8
}

fn with_rating(mut value: Value, rating: u8) -> Value {
    if let Some(map) = value.as_object_mut() {
        map.insert("rating".to_string(), json!(rating));
    }
    value
}

fn target_asset_ids(asset_id: &str, source: &Path) -> Vec<String> {
    let mut ids = vec![asset_id.to_string()];
    if let Some(entry) = super::registry::get(source)
        && entry.listed_asset_id != asset_id
    {
        ids.push(entry.listed_asset_id);
    }
    ids
}

fn fingerprint(value: &Value) -> String {
    blake3::hash(value.to_string().as_bytes())
        .to_hex()
        .to_string()
}

fn is_blank(value: &Value) -> bool {
    ["adjustments", "rating", "flag", "tags"].iter().all(|key| {
        value.get(key).is_none_or(|v| {
            v.is_null()
                || v.as_u64() == Some(0)
                || v.as_array().is_some_and(|a| a.is_empty())
                || v.as_object().is_some_and(|o| o.is_empty())
        })
    })
}

fn remember(
    source: &Path,
    modified: Option<u64>,
    local: &Value,
    remote_updated_at: Option<String>,
    remote_rating: Option<u8>,
) {
    let mut state = load_state(source);
    state.local_modified_ms = modified;
    state.local_hash = Some(fingerprint(local));
    if remote_updated_at.is_some() {
        state.remote_updated_at = remote_updated_at;
    }
    if remote_rating.is_some() {
        state.remote_rating = remote_rating;
    }
    save_state(source, &state);
}

fn local_changes(source: &Path, state: &SyncState) -> Option<Value> {
    let modified = modified_ms(&sidecar_of(source));
    if state.local_modified_ms == modified {
        return None;
    }
    let local = match modified {
        Some(_) => shared(read_sidecar(source)?),
        None => json!({}),
    };
    let unchanged = state.local_hash.as_deref() == Some(fingerprint(&local).as_str());
    let never_edited = state.local_hash.is_none() && is_blank(&local);
    if unchanged || never_edited {
        remember(source, modified, &local, None, None);
        return None;
    }
    Some(local)
}

fn write_sidecar(source: &Path, value: &Value) -> Result<(), String> {
    let mut value = value.clone();
    if let (Some(map), Some(exif)) = (
        value.as_object_mut(),
        read_sidecar(source).and_then(|old| old.get("exif").cloned()),
    ) {
        map.insert("exif".to_string(), exif);
    }
    let json = serde_json::to_string_pretty(&value).map_err(|e| e.to_string())?;
    let _layout = crate::exif_processing::SIDECAR_LAYOUT_LOCK.lock().unwrap();
    crate::file_management::write_file_atomic(&sidecar_of(source), json.as_bytes())
        .map_err(|e| e.to_string())
}

fn keep_as_virtual_copy(
    app_handle: &AppHandle,
    source: &Path,
    value: &Value,
) -> Result<(), String> {
    let file_name = source
        .file_name()
        .ok_or("Invalid file name")?
        .to_string_lossy()
        .into_owned();
    let copy_id: String = uuid::Uuid::new_v4()
        .simple()
        .to_string()
        .chars()
        .take(6)
        .collect();
    let json = serde_json::to_string_pretty(value).map_err(|e| e.to_string())?;
    {
        let _layout = crate::exif_processing::SIDECAR_LAYOUT_LOCK.lock().unwrap();
        crate::file_management::write_file_atomic(
            &source.with_file_name(format!("{file_name}.{copy_id}.rrdata")),
            json.as_bytes(),
        )
        .map_err(|e| e.to_string())?;
    }
    let _ = app_handle.emit(
        "immich-edits-conflict",
        json!({ "path": source, "fileName": file_name }),
    );
    Ok(())
}

fn resolve_remote_rating(
    state: &SyncState,
    edits_rating: Option<u8>,
    asset_rating: Option<u8>,
) -> Option<u8> {
    match (state.remote_rating, edits_rating, asset_rating) {
        (Some(prev), Some(er), Some(ar)) => {
            if ar != prev {
                Some(ar)
            } else {
                Some(er)
            }
        }
        (None, Some(er), Some(ar)) => {
            if ar > 0 {
                Some(ar)
            } else {
                Some(er)
            }
        }
        (_, None, Some(ar)) => Some(ar),
        (_, Some(er), None) => Some(er),
        (_, None, None) => None,
    }
}

async fn fetch_remote_rating(
    client: &ImmichClient,
    asset_id: &str,
    source: &Path,
    state: &SyncState,
) -> Option<u8> {
    let rating_of_asset = |id: String| async move {
        client
            .asset(&id)
            .await
            .map_err(|e| log::warn!("Could not read the rating of {id} from Immich: {e}"))
            .ok()?
            .rating()
    };
    if let Some(entry) = super::registry::get(source)
        && entry.listed_asset_id != asset_id
    {
        let listed_rating = rating_of_asset(entry.listed_asset_id).await;
        if state.remote_rating.unwrap_or(0) > 0 || listed_rating.unwrap_or(0) > 0 {
            return listed_rating;
        }
    }
    rating_of_asset(asset_id.to_string()).await
}

pub fn sync_listed_rating(source: &Path, rating: u8) -> u8 {
    let _guard = crate::exif_processing::lock_sidecar(&sidecar_of(source));
    let mut state = load_state(source);
    if let Some(local) = local_changes(source, &state) {
        return rating_of(&local);
    }
    let current_sidecar = read_sidecar(source).map(shared);
    let current_rating = current_sidecar.as_ref().map(rating_of).unwrap_or(0);
    if current_rating == rating {
        if state.remote_rating != Some(rating) && (rating > 0 || state_file(source).exists()) {
            state.remote_rating = Some(rating);
            save_state(source, &state);
        }
        return rating;
    }
    let updated = with_rating(current_sidecar.unwrap_or_else(|| json!({})), rating);
    if let Some(parent) = source.parent() {
        let _ = fs::create_dir_all(parent);
    }
    if write_sidecar(source, &updated).is_ok() {
        remember(
            source,
            modified_ms(&sidecar_of(source)),
            &updated,
            None,
            Some(rating),
        );
    }
    rating
}

pub async fn sync_rating_to_asset(client: &ImmichClient, source: &Path, target_asset_id: &str) {
    let rating = read_sidecar(source)
        .as_ref()
        .map(rating_of)
        .unwrap_or_else(|| load_state(source).remote_rating.unwrap_or(0));
    if rating > 0
        && let Err(e) = client
            .set_rating(&[target_asset_id.to_string()], rating)
            .await
    {
        log::warn!("Could not set rating on exported asset {target_asset_id}: {e}");
    }
}

pub async fn pull(
    app_handle: &AppHandle,
    client: &ImmichClient,
    asset_id: &str,
    source: &Path,
) -> Result<(), String> {
    let remote = client.metadata(asset_id, EDITS_KEY).await?;
    let state = load_state(source);
    let remote_rating = fetch_remote_rating(client, asset_id, source, &state).await;
    let Some((rating, edits_changed)) =
        apply_remote(app_handle, source, remote.as_ref(), remote_rating)?
    else {
        return Ok(());
    };
    if edits_changed {
        let app = app_handle.clone();
        let path = source.to_string_lossy().into_owned();
        tauri::async_runtime::spawn_blocking(move || {
            let settings = crate::app_settings::load_settings(app.clone()).unwrap_or_default();
            crate::file_management::regenerate_thumbnails(
                std::iter::once(path),
                &app,
                &settings,
                false,
            );
        });
    }
    let _ = app_handle.emit(
        "immich-edits-fetched",
        json!({
            "path": source,
            "rating": rating,
        }),
    );
    Ok(())
}

fn apply_remote(
    app_handle: &AppHandle,
    source: &Path,
    remote: Option<&MetadataEntry>,
    remote_rating: Option<u8>,
) -> Result<Option<(u8, bool)>, String> {
    let _guard = crate::exif_processing::lock_sidecar(&sidecar_of(source));
    let mut state = load_state(source);
    let edits_changed =
        remote.is_some_and(|r| state.remote_updated_at.as_deref() != Some(r.updated_at.as_str()));
    let effective_rating =
        resolve_remote_rating(&state, remote.map(|r| rating_of(&r.value)), remote_rating);
    let rating_changed = effective_rating.is_some_and(|r| state.remote_rating != Some(r));

    if !edits_changed && !rating_changed {
        return Ok(None);
    }

    if edits_changed && let Some(remote) = remote {
        let mut target = remote.value.clone();
        if let Some(r) = effective_rating {
            target = with_rating(target, r);
        }
        return match local_changes(source, &state) {
            Some(local) if local != target => {
                keep_as_virtual_copy(app_handle, source, &target)?;
                state.remote_updated_at = Some(remote.updated_at.clone());
                if let Some(r) = effective_rating {
                    state.remote_rating = Some(r);
                }
                save_state(source, &state);
                Ok(None)
            }
            _ => {
                if let Some(parent) = source.parent() {
                    fs::create_dir_all(parent).map_err(|e| e.to_string())?;
                }
                write_sidecar(source, &target)?;
                let final_rating = rating_of(&target);
                remember(
                    source,
                    modified_ms(&sidecar_of(source)),
                    &target,
                    Some(remote.updated_at.clone()),
                    Some(final_rating),
                );
                Ok(Some((final_rating, true)))
            }
        };
    }

    let Some(r) = effective_rating else {
        return Ok(None);
    };
    if local_changes(source, &state).is_some() {
        return Ok(None);
    }
    let current_sidecar = read_sidecar(source).map(shared);
    let current_rating = current_sidecar.as_ref().map(rating_of).unwrap_or(0);
    if current_rating == r {
        if r > 0 || state_file(source).exists() {
            state.remote_rating = Some(r);
            save_state(source, &state);
        }
        return Ok(None);
    }
    let updated = with_rating(current_sidecar.unwrap_or_else(|| json!({})), r);
    if let Some(parent) = source.parent() {
        fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    write_sidecar(source, &updated)?;
    remember(
        source,
        modified_ms(&sidecar_of(source)),
        &updated,
        None,
        Some(r),
    );
    Ok(Some((r, false)))
}

pub async fn push(
    app_handle: &AppHandle,
    client: &ImmichClient,
    asset_id: &str,
    source: &Path,
) -> Result<bool, String> {
    let state = load_state(source);
    let modified = modified_ms(&sidecar_of(source));
    let Some(local) = local_changes(source, &state) else {
        return Ok(false);
    };
    let local_rating = rating_of(&local);

    if let Some(remote) = client.metadata(asset_id, EDITS_KEY).await?
        && state.remote_updated_at.as_deref() != Some(remote.updated_at.as_str())
    {
        if remote.value == local && state.remote_rating == Some(local_rating) {
            remember(
                source,
                modified,
                &local,
                Some(remote.updated_at),
                Some(local_rating),
            );
            return Ok(false);
        }
        if remote.value != local {
            keep_as_virtual_copy(app_handle, source, &remote.value)?;
        }
    }

    if state.remote_rating != Some(local_rating) {
        let ids = target_asset_ids(asset_id, source);
        client.set_rating(&ids, local_rating).await?;
    }

    let updated_at = client.set_metadata(asset_id, EDITS_KEY, &local).await?;
    remember(
        source,
        modified,
        &local,
        Some(updated_at),
        Some(local_rating),
    );
    Ok(true)
}

fn unsent_edits(cache_dir: &Path) -> Vec<(String, PathBuf)> {
    let Ok(folders) = fs::read_dir(cache_dir) else {
        return Vec::new();
    };
    folders
        .flatten()
        .filter_map(|folder| {
            let asset_id = folder.file_name().to_str()?.to_string();
            if !is_asset_id(&asset_id) {
                return None;
            }
            let files: Vec<PathBuf> = fs::read_dir(folder.path())
                .ok()?
                .flatten()
                .map(|file| file.path())
                .collect();
            let source = files
                .iter()
                .find(|path| crate::formats::is_supported_image_file(path))
                .cloned()
                .or_else(|| {
                    files.iter().find_map(|path| {
                        let name = path.file_name()?.to_str()?;
                        match crate::file_management::parse_sidecar_name(name) {
                            Some((base, None)) => Some(folder.path().join(base)),
                            _ => None,
                        }
                    })
                })?;
            let changed =
                modified_ms(&sidecar_of(&source)) != load_state(&source).local_modified_ms;
            changed.then_some((asset_id, source))
        })
        .collect()
}

pub async fn send_unsent(app_handle: &AppHandle, session: &Session) {
    let cache_dir = session.cache_dir.clone();
    let unsent = tauri::async_runtime::spawn_blocking(move || unsent_edits(&cache_dir))
        .await
        .unwrap_or_default();
    let mut pending = unsent.len();
    let mut offline =
        unsent.is_empty() && status().offline && session.client.version_numbers().await.is_err();
    for (asset_id, source) in unsent {
        match push(app_handle, &session.client, &asset_id, &source).await {
            Ok(_) => pending -= 1,
            Err(e) => {
                log::warn!("Could not send edits of {}: {e}", source.display());
                if session.client.version_numbers().await.is_err() {
                    offline = true;
                    break;
                }
            }
        }
    }
    report(app_handle, SyncStatus { offline, pending });
}

pub fn start(app_handle: &AppHandle) {
    static STARTED: AtomicBool = AtomicBool::new(false);
    if STARTED.swap(true, Ordering::SeqCst) {
        return;
    }
    let app_handle = app_handle.clone();
    tauri::async_runtime::spawn(async move {
        loop {
            if tokio::time::timeout(INTERVAL, WAKE.notified())
                .await
                .is_ok()
            {
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            if let Some(session) = current_session() {
                send_unsent(&app_handle, &session).await;
            }
        }
    });
}
