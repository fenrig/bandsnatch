use crate::util::make_string_fs_safe;

use chrono::Utc;
use crossbeam_utils::thread;
use hmac::{Hmac, Mac};
use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};
use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use sha2::Digest;
use std::{
    collections::VecDeque,
    env,
    error::Error,
    fs,
    io::Read,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
use url::Url;

pub enum OutputTarget {
    Local { root: PathBuf },
    S3(S3Target),
}

#[derive(Clone, Default)]
pub struct S3Config {
    pub endpoint: Option<String>,
    pub region: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub session_token: Option<String>,
    pub upload_jobs: usize,
}

#[derive(Clone)]
pub struct S3Target {
    pub bucket: String,
    pub prefix: String,
    upload_jobs: usize,
    endpoint: Url,
    region: String,
    access_key: String,
    secret_key: String,
    session_token: Option<String>,
    client: Client,
}

const PATH_SEGMENT_ENCODE_SET: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'!')
    .add(b'"')
    .add(b'#')
    .add(b'$')
    .add(b'%')
    .add(b'&')
    .add(b'\'')
    .add(b'(')
    .add(b')')
    .add(b'*')
    .add(b'+')
    .add(b',')
    .add(b':')
    .add(b';')
    .add(b'=')
    .add(b'?')
    .add(b'@')
    .add(b'[')
    .add(b'\\')
    .add(b']')
    .add(b'^')
    .add(b'`')
    .add(b'{')
    .add(b'|')
    .add(b'}')
    .add(b'<')
    .add(b'>');

impl OutputTarget {
    pub fn parse(output_folder: &str, s3_config: S3Config) -> Result<Self, Box<dyn Error>> {
        if output_folder.starts_with("s3://") {
            return Ok(Self::S3(S3Target::new(output_folder, s3_config)?));
        }

        let root = PathBuf::from(shellexpand::tilde(output_folder).into_owned());
        Ok(Self::Local { root })
    }

    pub fn ensure_ready(&self) -> Result<(), Box<dyn Error>> {
        match self {
            Self::Local { root } => {
                if let Ok(metadata) = fs::metadata(root) {
                    if !metadata.is_dir() {
                        bail!(
                            "Cannot use `output-folder`, as it is not a folder. Please delete it and create as a directory, or try a different path."
                        );
                    }
                } else {
                    fs::create_dir_all(root)?;
                }
            }
            Self::S3(_) => (),
        }

        Ok(())
    }

    pub fn sync_cache_from_remote(&self, local_path: &Path) -> Result<(), Box<dyn Error>> {
        match self {
            Self::Local { .. } => Ok(()),
            Self::S3(target) => target.download_cache(local_path),
        }
    }

    pub fn sync_cache_to_remote(&self, local_path: &Path) -> Result<(), Box<dyn Error>> {
        match self {
            Self::Local { .. } => Ok(()),
            Self::S3(target) => target.upload_cache(local_path),
        }
    }

    pub fn cache_path_for_output_folder(output_folder: &str) -> Result<PathBuf, Box<dyn Error>> {
        if output_folder.starts_with("s3://") {
            return Ok(S3Target::cache_path_for_output_folder(output_folder)?);
        }

        let root = PathBuf::from(shellexpand::tilde(output_folder).into_owned());
        Ok(root.join("bandcamp-collection-downloader.cache"))
    }

    pub fn upload_release_dir(
        &self,
        local_dir: &Path,
        release_key_prefix: &str,
    ) -> Result<(), Box<dyn Error>> {
        match self {
            Self::Local { .. } => Ok(()),
            Self::S3(target) => target.upload_release_dir(local_dir, release_key_prefix),
        }
    }
}

impl S3Target {
    fn cache_path_for_output_folder(output_folder: &str) -> Result<PathBuf, Box<dyn Error>> {
        let url = Url::parse(output_folder)?;
        if url.scheme() != "s3" {
            bail!("expected an `s3://bucket/prefix` output folder");
        }

        let bucket = url
            .host_str()
            .ok_or_else(|| format!("missing S3 bucket in `{output_folder}`"))?;
        let prefix = url.path().trim_matches('/');
        let mut name = format!(
            "bandcamp-collection-downloader.{}",
            make_string_fs_safe(bucket)
        );
        if !prefix.is_empty() {
            name.push('.');
            name.push_str(&make_string_fs_safe(prefix));
        }
        name.push_str(".cache");
        Ok(PathBuf::from(name))
    }

    pub fn new(output_folder: &str, config: S3Config) -> Result<Self, Box<dyn Error>> {
        let url = Url::parse(output_folder)?;
        if url.scheme() != "s3" {
            bail!("expected an `s3://bucket/prefix` output folder");
        }

        let bucket = url
            .host_str()
            .ok_or_else(|| format!("missing S3 bucket in `{output_folder}`"))?
            .to_string();
        let prefix = url.path().trim_matches('/').to_string();

        let endpoint = Self::endpoint_url(config.endpoint.as_deref())?;
        let region = config
            .region
            .or_else(|| Self::env_or("AWS_REGION", "BS_S3_REGION"))
            .or_else(|| env::var("AWS_DEFAULT_REGION").ok())
            .unwrap_or_else(|| "us-east-1".to_string());
        let access_key = config
            .access_key_id
            .or_else(|| Self::env_or("AWS_ACCESS_KEY_ID", "BS_S3_ACCESS_KEY_ID"))
            .ok_or("missing S3 access key id (`--s3-access-key-id` or `BS_S3_ACCESS_KEY_ID`)")?;
        let secret_key = config
            .secret_access_key
            .or_else(|| Self::env_or("AWS_SECRET_ACCESS_KEY", "BS_S3_SECRET_ACCESS_KEY"))
            .ok_or(
                "missing S3 secret access key (`--s3-secret-access-key` or `BS_S3_SECRET_ACCESS_KEY`)",
            )?;
        let session_token = config
            .session_token
            .or_else(|| Self::env_or("AWS_SESSION_TOKEN", "BS_S3_SESSION_TOKEN"));
        let client = Client::builder().build()?;

        Ok(Self {
            bucket,
            prefix,
            upload_jobs: config.upload_jobs,
            endpoint,
            region,
            access_key,
            secret_key,
            session_token,
            client,
        })
    }

    fn env_or(primary: &str, fallback: &str) -> Option<String> {
        env::var(primary).ok().or_else(|| env::var(fallback).ok())
    }

    fn endpoint_url(endpoint: Option<&str>) -> Result<Url, Box<dyn Error>> {
        if let Some(endpoint) = endpoint
            .map(str::to_string)
            .or_else(|| Self::env_or("AWS_ENDPOINT_URL", "BS_S3_ENDPOINT"))
            .or_else(|| env::var("AWS_ENDPOINT_URL_S3").ok())
        {
            return Ok(Url::parse(&endpoint)?);
        }

        let region = Self::env_or("AWS_REGION", "AWS_DEFAULT_REGION")
            .unwrap_or_else(|| "us-east-1".to_string());
        let endpoint = if region == "us-east-1" {
            "https://s3.amazonaws.com".to_string()
        } else {
            format!("https://s3.{region}.amazonaws.com")
        };

        Ok(Url::parse(&endpoint)?)
    }

    fn cache_key(&self) -> String {
        let mut key = self.prefix.trim_matches('/').to_string();
        if !key.is_empty() {
            key.push('/');
        }
        key.push_str("bandcamp-collection-downloader.cache");
        key
    }

    fn join_key(prefix: &str, relative: &Path) -> String {
        let relative = relative.to_string_lossy().replace('\\', "/");
        let prefix = prefix.trim_matches('/');
        if prefix.is_empty() {
            relative
        } else {
            format!("{prefix}/{relative}")
        }
    }

    fn object_path(&self, key: &str) -> String {
        let mut path = self.endpoint.path().trim_end_matches('/').to_string();
        if path.is_empty() {
            path.push('/');
        } else if !path.starts_with('/') {
            path.insert(0, '/');
        }

        if !path.ends_with('/') {
            path.push('/');
        }

        path.push_str(&self.bucket);
        path.push('/');
        path.push_str(&Self::encode_key_path(key));
        path
    }

    fn encode_key_path(key: &str) -> String {
        key.trim_start_matches('/')
            .split('/')
            .map(|segment| utf8_percent_encode(segment, PATH_SEGMENT_ENCODE_SET).to_string())
            .collect::<Vec<_>>()
            .join("/")
    }

    fn encode_canonical_uri(path: &str) -> String {
        path.to_string()
    }

    fn hex(bytes: &[u8]) -> String {
        let mut out = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            out.push_str(&format!("{byte:02x}"));
        }
        out
    }

    fn sha256_hex(data: &[u8]) -> String {
        let digest = sha2::Sha256::digest(data);
        Self::hex(&digest)
    }

    fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
        let mut mac = Hmac::<sha2::Sha256>::new_from_slice(key).expect("invalid HMAC key");
        mac.update(data);
        mac.finalize().into_bytes().to_vec()
    }

    fn signing_key(&self, date: &str) -> Vec<u8> {
        let k_secret = format!("AWS4{}", self.secret_key);
        let k_date = Self::hmac_sha256(k_secret.as_bytes(), date.as_bytes());
        let k_region = Self::hmac_sha256(&k_date, self.region.as_bytes());
        let k_service = Self::hmac_sha256(&k_region, b"s3");
        Self::hmac_sha256(&k_service, b"aws4_request")
    }

    fn auth_headers(
        &self,
        method: &str,
        request_path: &str,
        host: &str,
        payload_hash: &str,
        amz_date: &str,
    ) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("host"),
            HeaderValue::from_str(&host).unwrap(),
        );
        headers.insert(
            HeaderName::from_static("x-amz-content-sha256"),
            HeaderValue::from_str(payload_hash).unwrap(),
        );
        headers.insert(
            HeaderName::from_static("x-amz-date"),
            HeaderValue::from_str(amz_date).unwrap(),
        );

        let mut signed_headers = vec!["host", "x-amz-content-sha256", "x-amz-date"];
        if let Some(token) = &self.session_token {
            headers.insert(
                HeaderName::from_static("x-amz-security-token"),
                HeaderValue::from_str(token).unwrap(),
            );
            signed_headers.push("x-amz-security-token");
        }

        let canonical_headers = signed_headers
            .iter()
            .map(|name| {
                let value = headers.get(*name).unwrap().to_str().unwrap().trim();
                format!("{name}:{value}\n")
            })
            .collect::<String>();

        let signed_headers = signed_headers.join(";");
        let canonical_request = format!(
            "{method}\n{}\n\n{canonical_headers}\n{signed_headers}\n{payload_hash}",
            Self::encode_canonical_uri(request_path),
        );
        let canonical_request_hash = Self::sha256_hex(canonical_request.as_bytes());

        let scope_date = &amz_date[..8];
        let credential_scope = format!("{scope_date}/{}/s3/aws4_request", self.region);
        let string_to_sign =
            format!("AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n{canonical_request_hash}");
        let signature = Self::hex(&Self::hmac_sha256(
            &self.signing_key(scope_date),
            string_to_sign.as_bytes(),
        ));
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}",
            self.access_key
        );

        headers.insert(
            HeaderName::from_static("authorization"),
            HeaderValue::from_str(&authorization).unwrap(),
        );

        headers
    }

    fn put_file(&self, file_path: &Path, key: &str) -> Result<(), Box<dyn Error>> {
        let request_path = self.object_path(key);
        let mut url = format!(
            "{}://{}",
            self.endpoint.scheme(),
            self.endpoint.host_str().unwrap()
        );
        if let Some(port) = self.endpoint.port() {
            url.push_str(&format!(":{port}"));
        }
        url.push_str(&request_path);
        let url = Url::parse(&url)?;

        let host = match url.port_or_known_default() {
            Some(port) if !matches!((url.scheme(), port), ("https", 443) | ("http", 80)) => {
                format!("{}:{port}", url.host_str().unwrap())
            }
            _ => url.host_str().unwrap().to_string(),
        };

        let mut file = fs::File::open(file_path)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;

        let payload_hash = Self::sha256_hex(&bytes);
        let amz_date = Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
        let headers = self.auth_headers("PUT", &request_path, &host, &payload_hash, &amz_date);

        let response = self.client.put(url).headers(headers).body(bytes).send()?;

        if !response.status().is_success() {
            bail!(
                "S3 upload failed for `{key}` with status {}",
                response.status()
            );
        }

        Ok(())
    }

    fn download_cache(&self, local_path: &Path) -> Result<(), Box<dyn Error>> {
        let key = self.cache_key();
        let request_path = self.object_path(&key);
        let mut url = format!(
            "{}://{}",
            self.endpoint.scheme(),
            self.endpoint.host_str().unwrap()
        );
        if let Some(port) = self.endpoint.port() {
            url.push_str(&format!(":{port}"));
        }
        url.push_str(&request_path);
        let url = Url::parse(&url)?;

        let host = match url.port_or_known_default() {
            Some(port) if !matches!((url.scheme(), port), ("https", 443) | ("http", 80)) => {
                format!("{}:{port}", url.host_str().unwrap())
            }
            _ => url.host_str().unwrap().to_string(),
        };

        let payload_hash = Self::sha256_hex(b"");
        let amz_date = Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
        let headers = self.auth_headers("GET", &request_path, &host, &payload_hash, &amz_date);

        let response = self.client.get(url).headers(headers).send()?;
        if response.status().is_success() {
            let bytes = response.bytes()?;
            fs::write(local_path, &bytes)?;
            return Ok(());
        }

        if response.status().as_u16() == 404 {
            if local_path.exists() {
                let _ = fs::remove_file(local_path);
            }
            return Ok(());
        }

        bail!(
            "S3 cache download failed for `{key}` with status {}",
            response.status()
        );
    }

    fn upload_cache(&self, local_path: &Path) -> Result<(), Box<dyn Error>> {
        self.put_file(local_path, &self.cache_key())
    }

    fn upload_release_dir(
        &self,
        local_dir: &Path,
        release_key_prefix: &str,
    ) -> Result<(), Box<dyn Error>> {
        let mut files = Vec::new();
        Self::collect_files(local_dir, local_dir, release_key_prefix, &mut files)?;

        let jobs = self.upload_jobs.max(1).min(files.len().max(1));
        if jobs == 1 {
            for (path, key) in files {
                self.put_file(&path, &key)?;
            }
            return Ok(());
        }

        let queue = Arc::new(Mutex::new(VecDeque::from(files)));
        let first_error = Arc::new(Mutex::new(None::<String>));

        thread::scope(|scope| {
            for _ in 0..jobs {
                let target = self.clone();
                let queue = queue.clone();
                let first_error = first_error.clone();

                scope.spawn(move |_| loop {
                    if first_error.lock().unwrap().is_some() {
                        break;
                    }

                    let next = {
                        let mut queue = queue.lock().unwrap();
                        queue.pop_front()
                    };

                    let Some((path, key)) = next else {
                        break;
                    };

                    if let Err(e) = target.put_file(&path, &key) {
                        let mut first_error = first_error.lock().unwrap();
                        if first_error.is_none() {
                            *first_error = Some(e.to_string());
                        }
                        break;
                    }
                });
            }
        })
        .unwrap();

        if let Some(err) = first_error.lock().unwrap().take() {
            bail!(err);
        }

        Ok(())
    }

    fn collect_files(
        root: &Path,
        current: &Path,
        release_key_prefix: &str,
        files: &mut Vec<(PathBuf, String)>,
    ) -> Result<(), Box<dyn Error>> {
        for entry in fs::read_dir(current)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                Self::collect_files(root, &path, release_key_prefix, files)?;
                continue;
            }

            if !path.is_file() {
                continue;
            }

            let relative = path.strip_prefix(root)?;
            let key = Self::join_key(release_key_prefix, relative);
            files.push((path, key));
        }

        Ok(())
    }
}

pub fn make_temp_release_dir(release_id: &str) -> Result<PathBuf, Box<dyn Error>> {
    let mut path = std::env::temp_dir();
    let suffix: u64 = rand::random();
    path.push(format!(
        "bandsnatch-{}-{suffix}",
        make_string_fs_safe(release_id)
    ));
    fs::create_dir_all(&path)?;
    Ok(path)
}
