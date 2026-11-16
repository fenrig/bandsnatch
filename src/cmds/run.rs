use clap::{builder::PossibleValuesParser, Args as ClapArgs};
use crossbeam_utils::thread;
use indicatif::MultiProgress;
use std::{
    fs, io,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use crate::{
    api,
    cache::{self, CacheEntry},
    cookies, storage, util,
};

const FORMATS: &[&str] = &[
    "flac",
    "wav",
    "aac-hi",
    "mp3-320",
    "aiff-lossless",
    "vorbis",
    "mp3-v0",
    "alac",
];

macro_rules! skip_err {
    ($res:expr) => {
        match $res {
            Ok(val) => val,
            Err(e) => {
                warn!("An error: {}; skipped.", e);
                continue;
            }
        }
    };
}

#[derive(Debug, ClapArgs)]
pub struct Args {
    #[arg(long, env = "BS_ALBUM")]
    album: Option<String>,

    #[arg(long, env = "BS_ARTIST")]
    artist: Option<String>,

    /// The audio format to download the files in.
    #[arg(short = 'f', long = "format", value_parser = PossibleValuesParser::new(FORMATS), env = "BS_FORMAT")]
    audio_format: String,

    #[arg(short, long, value_name = "COOKIES_FILE", env = "BS_COOKIES")]
    cookies: Option<String>,

    /// Enables some extra debug output in certain scenarios.
    #[arg(long, env = "BS_DEBUG")]
    debug: bool,

    /// Return a list of all tracks to be downloaded, without actually downloading them.
    #[arg(short = 'd', long = "dry-run")]
    dry_run: bool,

    /// Ignores any found cache file and instead does a from-scratch download run.
    #[arg(short = 'F', long, env = "BS_FORCE")]
    force: bool,

    /// The amount of parallel jobs (threads) to use.
    #[arg(
        short = 'j',
        long = "download-jobs",
        alias = "jobs",
        default_value_t = 4,
        env = "BS_DOWNLOAD_JOBS"
    )]
    jobs: u8,

    /// The amount of parallel S3 upload jobs (threads) to use.
    #[arg(long = "upload-jobs", default_value_t = 4, env = "BS_UPLOAD_JOBS")]
    upload_jobs: u8,

    /// Maximum number of releases to download. Useful for testing.
    #[arg(short = 'n', long, env = "BS_LIMIT")]
    limit: Option<usize>,

    /// The folder to extract downloaded releases to.
    #[arg(
        short,
        long = "output-folder",
        value_name = "FOLDER",
        default_value = "./",
        env = "BS_OUTPUT_FOLDER"
    )]
    output_folder: String,

    /// S3 endpoint URL to use when `--output-folder` points at S3.
    #[arg(long, env = "BS_S3_ENDPOINT")]
    s3_endpoint: Option<String>,

    /// S3 region used for signing requests.
    #[arg(long, env = "BS_S3_REGION")]
    s3_region: Option<String>,

    /// S3 access key id.
    #[arg(long, env = "BS_S3_ACCESS_KEY_ID")]
    s3_access_key_id: Option<String>,

    /// S3 secret access key.
    #[arg(long, env = "BS_S3_SECRET_ACCESS_KEY")]
    s3_secret_access_key: Option<String>,

    /// Optional S3 session token.
    #[arg(long, env = "BS_S3_SESSION_TOKEN")]
    s3_session_token: Option<String>,

    /// Only download releases sold through this Bandcamp subdomain, e.g. a label's.
    #[arg(long, env = "BS_SUBDOMAIN")]
    subdomain: Option<String>,

    /// Bandcamp username. If omitted, detect it from your authentication cookies.
    #[arg(env = "BS_USER")]
    user: Option<String>,
}

pub fn command(
    Args {
        album,
        artist,
        audio_format,
        cookies,
        debug,
        dry_run,
        force,
        jobs,
        limit,
        output_folder,
        upload_jobs,
        s3_endpoint,
        s3_region,
        s3_access_key_id,
        s3_secret_access_key,
        s3_session_token,
        subdomain,
        user,
    }: Args,
) -> Result<(), Box<dyn std::error::Error>> {
    let _temp_cleanup = storage::TempReleaseCleanup::new();

    let cookies_file = cookies.map(|p| {
        let expanded = shellexpand::tilde(&p);
        expanded.into_owned()
    });
    let limit = limit.unwrap_or(usize::MAX);

    let s3_config = storage::S3Config {
        endpoint: s3_endpoint,
        region: s3_region,
        access_key_id: s3_access_key_id,
        secret_access_key: s3_secret_access_key,
        session_token: s3_session_token,
        upload_jobs: upload_jobs.max(1) as usize,
    };
    let output_target = Arc::new(storage::OutputTarget::parse(&output_folder, s3_config)?);
    output_target.ensure_ready()?;

    let cache_path = storage::OutputTarget::cache_path_for_output_folder(&output_folder)?;
    output_target.sync_cache_from_remote(&cache_path)?;

    let cookies = cookies::get_bandcamp_cookies(cookies_file.as_deref())?;
    let api = Arc::new(api::Api::new(cookies));
    let user = match user {
        Some(user) => user,
        None => {
            let user = api.get_current_username()?;
            info!("Detected Bandcamp user: {user}");
            user
        }
    };
    let cache = Arc::new(Mutex::new(cache::Cache::new(cache_path.clone())));

    let filters = api::structs::Filters {
        album: album.as_ref(),
        artist: artist.as_ref(),
        subdomain: subdomain.as_ref(),
    };
    let download_urls = api.get_download_urls(&user, &filters)?.download_urls;
    let items = {
        // Lock gets freed after this block.
        let cache = cache
            .lock()
            .map_err(|_| io::Error::other("cache lock poisoned"))?;

        download_urls
            .into_iter()
            .filter(|(id, download)| {
                force
                    || cache
                        .content()
                        .get(id)
                        .is_none_or(|entry| entry.needs_download(download.is_preorder))
            })
            .take(limit)
            .collect::<Vec<_>>()
    };

    if dry_run {
        println!("Fetching information for {} found releases", items.len());
    } else {
        println!("Trying to download {} releases", items.len());
    }

    let queue = util::WorkQueue::from_vec(items);
    let temp_space = Arc::new(storage::TempSpaceState::default());
    let m = Arc::new(MultiProgress::new());
    let dry_run_results = Arc::new(Mutex::new(Vec::<String>::new()));

    thread::scope(|scope| {
        for i in 0..jobs {
            let api = api.clone();
            let cache = cache.clone();
            let output_target = output_target.clone();
            let m = m.clone();
            let queue = queue.clone();
            let audio_format = audio_format.clone();
            let temp_space = temp_space.clone();
            let dry_run_results = dry_run_results.clone();

            // somehow re-create thread if it panics
            scope.spawn(move |_| {
                while let Some((id, download)) = queue.get_work() {
                    if temp_space.should_stop_downloads() {
                        break;
                    }
                    m.suspend(|| debug!("thread {i} taking {id}"));
                    let cache_entry = if download.is_preorder {
                        CacheEntry::Preorder
                    } else {
                        CacheEntry::Complete
                    };

                    // skip_err!
                    let item = match api.get_digital_item(&download.url, &debug) {
                        Ok(Some(item)) => item,
                        Ok(None) => {
                            let mut cache = skip_err!(cache
                                .lock()
                                .map_err(|_| io::Error::other("cache lock poisoned")));
                            warn!("Could not find digital item for {id}");
                            skip_err!(cache.add(&id, "UNKNOWN", cache_entry));
                            continue;
                        }
                        Err(_) => continue,
                    };

                    if let None = item.downloads {
                        let mut cache = skip_err!(cache
                            .lock()
                            .map_err(|_| io::Error::other("cache lock poisoned")));
                        warn!("Skipping {id}, does not have any downloads");
                        skip_err!(cache.add(&id, "No downloads", cache_entry));
                        continue;
                    }

                    if dry_run {
                        let mut results = skip_err!(dry_run_results
                            .lock()
                            .map_err(|_| io::Error::other("dry-run results lock poisoned")));
                        results.push(format!("{id}, {} - {}", item.title, item.artist));
                        continue;
                    }

                    // TODO: intialise progressbar with this, and then pass that + m to download
                    m.println(format!(
                        "Trying {id}, {} - {} ({:?})",
                        item.title,
                        item.artist,
                        item.is_single(),
                    ))
                    .unwrap();

                    let (path, release_key, _staging_dir): (
                        PathBuf,
                        Option<String>,
                        Option<storage::TempReleaseDir>,
                    ) = match output_target.as_ref() {
                        storage::OutputTarget::Local { root } => {
                            let path = PathBuf::from(item.destination_path(root, &id));
                            skip_err!(fs::create_dir_all(&path));
                            (path, None, None)
                        }
                        storage::OutputTarget::S3(target) => {
                            let _staging_dir = skip_err!(storage::make_temp_release_dir(
                                &id,
                                &temp_space
                            ));
                            let path = _staging_dir.path().to_path_buf();
                            let release_key = PathBuf::from(item.destination_path("", &id))
                                .to_string_lossy()
                                .replace('\\', "/");
                            let release_key = if target.prefix.is_empty() {
                                release_key
                            } else {
                                format!("{}/{}", target.prefix, release_key)
                            };
                            (path, Some(release_key), Some(_staging_dir))
                        }
                    };

                    // TODO: separate cache for failed downloads.
                    // TODO: retries
                    if let Err(e) = api.download_item(&item, &path, &audio_format, &m) {
                        if storage::is_storage_full(e.as_ref()) {
                            warn!(
                                "Download skipped because temporary storage is full; free space in `{}` and retry.",
                                std::env::temp_dir().display()
                            );
                        } else {
                            warn!("An error: {}; skipped.", e);
                        }
                        continue;
                    }

                    if let Some(key) = release_key.as_ref() {
                        if let Err(e) = output_target.upload_release_dir(&path, key) {
                            warn!("An error: {}; skipped.", e);
                            continue;
                        }
                    }

                    let mut cache = skip_err!(cache
                        .lock()
                        .map_err(|_| io::Error::other("cache lock poisoned")));
                    skip_err!(cache.add(
                        &id,
                        &format!(
                            "{} ({}) by {}",
                            item.title,
                            item.release_year(),
                            item.artist
                        ),
                        cache_entry,
                    ));
                }
            });
        }
    })
    .unwrap();

    if !dry_run && cache_path.exists() {
        output_target.sync_cache_to_remote(&cache_path)?;
    }

    if dry_run {
        let results = dry_run_results
            .lock()
            .map_err(|_| io::Error::other("dry-run results lock poisoned"))?;
        println!("{}", results.join("\n"));
        return Ok(());
    }

    println!("Finished!");

    Ok(())
}
