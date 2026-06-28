use clap::{builder::PossibleValuesParser, Args as ClapArgs};
use crossbeam_utils::thread;
use indicatif::MultiProgress;
use std::{
    fs,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use crate::{api, cache, cookies, storage, util};

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

fn add_cache_entry(
    cache: &Arc<Mutex<cache::Cache<PathBuf>>>,
    cache_dirty: &Arc<Mutex<bool>>,
    id: &str,
    description: &str,
) {
    let added = match cache.lock().unwrap().add_if_missing(id, description) {
        Ok(added) => added,
        Err(e) => {
            warn!("An error: {}; skipped.", e);
            return;
        }
    };
    if added {
        *cache_dirty.lock().unwrap() = true;
    }
}

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

    /// The amount of parallel download jobs (threads) to use.
    #[arg(
        short = 'j',
        long = "download-jobs",
        alias = "jobs",
        default_value_t = 4,
        env = "BS_DOWNLOAD_JOBS"
    )]
    download_jobs: u8,

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

    /// Name of the user to download releases from (must be logged in through cookies).
    #[clap(env = "BS_USER")]
    user: String,
}

pub fn command(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    let Args {
        album,
        artist,
        audio_format,
        cookies,
        debug,
        dry_run,
        force,
        download_jobs,
        limit,
        output_folder,
        upload_jobs,
        s3_endpoint,
        s3_region,
        s3_access_key_id,
        s3_secret_access_key,
        s3_session_token,
        user,
    } = args;

    let cookies_file = cookies.map(|p| {
        let expanded = shellexpand::tilde(&p);
        expanded.into_owned()
    });
    let download_jobs = download_jobs.max(1);
    let upload_jobs = upload_jobs.max(1);

    let s3_config = storage::S3Config {
        endpoint: s3_endpoint,
        region: s3_region,
        access_key_id: s3_access_key_id,
        secret_access_key: s3_secret_access_key,
        session_token: s3_session_token,
        upload_jobs: upload_jobs as usize,
    };
    let output_target = Arc::new(storage::OutputTarget::parse(&output_folder, s3_config)?);
    output_target.ensure_ready()?;

    let cache_path = storage::OutputTarget::cache_path_for_output_folder(&output_folder)?;
    output_target.sync_cache_from_remote(&cache_path)?;
    let cache = Arc::new(Mutex::new(cache::Cache::new(cache_path.clone())));
    let cache_dirty = Arc::new(Mutex::new(false));

    let cookies = cookies::get_bandcamp_cookies(cookies_file.as_deref())?;
    let api = Arc::new(api::Api::new(cookies));

    let download_urls = api
        .get_download_urls(&user, artist.as_ref(), album.as_ref())?
        .download_urls;
    let items = {
        // Lock gets freed after this block.
        let cache_content = cache.lock().unwrap().content()?;

        download_urls
            .into_iter()
            .filter(|(x, _)| force || !cache_content.contains(x))
            .take(limit.unwrap_or(usize::MAX))
            .collect::<Vec<_>>()
    };

    if items.is_empty() {
        if dry_run {
            println!("Fetching information for 0 found releases");
        } else {
            println!("Trying to download 0 releases");
        }
        println!("Finished!");
        return Ok(());
    }

    if dry_run {
        println!("Fetching information for {} found releases", items.len());
    } else {
        println!("Trying to download {} releases", items.len());
    }

    let queue = util::WorkQueue::from_vec(items);
    let m = Arc::new(MultiProgress::new());
    let dry_run_results = Arc::new(Mutex::new(Vec::<String>::new()));

    thread::scope(|scope| {
        for i in 0..download_jobs {
            let api = api.clone();
            let cache = cache.clone();
            let output_target = output_target.clone();
            let m = m.clone();
            let queue = queue.clone();
            let audio_format = audio_format.clone();
            let dry_run_results = dry_run_results.clone();
            let cache_dirty = cache_dirty.clone();

            // somehow re-create thread if it panics
            scope.spawn(move |_| {
                while let Some((id, info)) = queue.get_work() {
                    m.suspend(|| debug!("thread {i} taking {id}"));

                    // skip_err!
                    let item = match api.get_digital_item(&info.url, &debug) {
                        Ok(Some(item)) => item,
                        Ok(None) => {
                            warn!("Could not find digital item for {id}");
                            continue;
                        }
                        Err(_) => continue,
                    };

                    if let None = item.downloads {
                        warn!("No downloads available for {id}; nothing to download.");
                        continue;
                    }

                    if dry_run {
                        let results_lock = dry_run_results.lock();
                        if let Ok(mut results) = results_lock {
                            results.push(format!("{id}, {} - {}", item.title, item.artist))
                        } else {
                            panic!("dry_run_results is poisoned!!")
                        }
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

                    let (path, release_key, staging_dir): (
                        PathBuf,
                        Option<String>,
                        Option<PathBuf>,
                    ) = match output_target.as_ref() {
                        storage::OutputTarget::Local { root } => {
                            let path = item.destination_path(root);
                            skip_err!(fs::create_dir_all(&path));
                            (path, None, None)
                        }
                        storage::OutputTarget::S3(target) => {
                            let path = skip_err!(storage::make_temp_release_dir(&id));
                            (
                                path.clone(),
                                Some(item.destination_key(&target.prefix)),
                                Some(path),
                            )
                        }
                    };

                    // TODO: separate cache for failed downloads.
                    // TODO: retries
                    if let Err(e) = api.download_item(&item, &path, &audio_format, &m) {
                        warn!("An error: {}; skipped.", e);
                        if let Some(dir) = &staging_dir {
                            let _ = fs::remove_dir_all(dir);
                        }
                        continue;
                    }

                    if let Some(key) = release_key.as_ref() {
                        if let Err(e) = output_target.upload_release_dir(&path, key) {
                            warn!("An error: {}; skipped.", e);
                            if let Some(dir) = &staging_dir {
                                let _ = fs::remove_dir_all(dir);
                            }
                            continue;
                        }
                    }

                    if let Some(dir) = &staging_dir {
                        let _ = fs::remove_dir_all(dir);
                    }

                    add_cache_entry(
                        &cache,
                        &cache_dirty,
                        &id,
                        &format!(
                            "{} ({}) by {}",
                            item.title,
                            item.release_year(),
                            item.artist
                        ),
                    );
                }
            });
        }
    })
    .unwrap();

    if *cache_dirty.lock().unwrap() {
        debug!("Uploading updated cache to storage backend");
        output_target.sync_cache_to_remote(cache_path.as_path())?;
    }

    if args.dry_run {
        println!("{}", dry_run_results.lock().unwrap().join("\n"));
        return Ok(());
    }

    println!("Finished!");

    Ok(())
}
