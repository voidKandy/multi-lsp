use clap::Parser;
use serde_json::Value;
use std::{
    collections::HashMap,
    fs,
    path::PathBuf,
    pin::Pin,
    process::Stdio,
    sync::{atomic::AtomicBool, Arc},
};
use tokio::process::Child;
use tokio::{
    io::{self, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{ChildStdin, ChildStdout, Command},
    sync::{broadcast, mpsc},
};
use tokio_stream::{Stream, StreamExt, StreamMap};
use tracing::debug;
use tracing_subscriber::filter::{EnvFilter, LevelFilter};

use self::config::LspConfig;

mod config;

type MainErr = Box<dyn std::error::Error + Send + Sync + 'static>;
type MainResult<T> = std::result::Result<T, MainErr>;

macro_rules! other_err {
    ($($arg:tt)*) => ({
        Into::<MainErr>::into(std::io::Error::other(format!($($arg)*)))
    });
}

async fn read_content_length<T>(reader: &mut BufReader<T>) -> MainResult<usize>
where
    BufReader<T>: AsyncBufReadExt,
    T: Unpin,
{
    let mut content_length = 0;
    loop {
        let mut line = String::new();
        reader.read_line(&mut line).await?;
        tracing::warn!("read line: {}", line);
        if let Some(content) = line.strip_prefix("Content-Length: ") {
            content_length = content
                .trim()
                .parse()
                .map_err(|err| other_err!("Failed to parse Content-Length: {err:#?}"))?;
        } else if line.strip_prefix("Content-Type: ").is_some() {
            // ignored.
        } else if line == "\r\n" {
            break;
        } else {
            return Err(other_err!("Failed to get Content-Length from LSP data."));
        }
    }
    Ok(content_length)
}

async fn read_message<T>(reader: &mut BufReader<T>) -> MainResult<Value>
where
    BufReader<T>: AsyncBufReadExt,
    T: Unpin,
{
    let content_length = read_content_length(reader).await?;
    let mut body = vec![0u8; content_length];
    reader.read_exact(&mut body).await.unwrap();
    tracing::warn!("read body: {}", String::from_utf8_lossy(&body));
    serde_json::from_slice(&body)
        .map_err(|err| other_err!("Failed to parse input as LSP data: {err:#?}"))
}

async fn proxy_stdin(
    mut stdin: ChildStdin,
    mut input: broadcast::Receiver<String>,
) -> MainResult<()> {
    while let Ok(message) = input.recv().await {
        stdin
            .write_all(format!("Content-Length: {}\r\n\r\n", message.len()).as_bytes())
            .await?;
        stdin.write_all(message.as_bytes()).await?;
    }
    Ok(())
}

async fn proxy_stdout(
    mut stdout: BufReader<ChildStdout>,
    tx: mpsc::Sender<Value>,
) -> MainResult<()> {
    loop {
        let message = read_message(&mut stdout).await?;
        tx.send(message).await?;
    }
}

async fn run(config: LspConfig) -> MainResult<()> {
    tracing::warn!("RUNG");
    // keep tracing_appender guard alive
    let mut _tracing_guard = None;
    if let Some(log_file) = config.log_file.as_ref() {
        // setup tracing
        let directory = log_file.parent().unwrap();
        let file_name = log_file.file_name().unwrap();
        let file_appender = tracing_appender::rolling::never(directory, file_name);
        let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);
        _tracing_guard = Some(guard);

        let env_filter = EnvFilter::builder()
            .with_default_directive(LevelFilter::DEBUG.into())
            .from_env_lossy();
        tracing_subscriber::fmt()
            .with_writer(non_blocking)
            .with_env_filter(env_filter)
            .init();
    }

    let (tx, _rx) = broadcast::channel(100);
    let mut child_processes: HashMap<u32, (Child, Arc<AtomicBool>)> = HashMap::new();
    let mut child_rxs = Vec::with_capacity(config.languages.len());
    for lang in &config.languages {
        // spawn LSP server command
        let mut cmd = Command::new(&lang.command);
        cmd.args(&lang.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped());
        if let Some(mut child) = match cmd.spawn() {
            Ok(child) => Some(child),
            Err(err) => {
                let errmsg = format!(
                    "Failed to spawn {} binary: {err:#?}",
                    &lang.command.display()
                );
                if config.abort {
                    return Err(other_err!("{errmsg}"));
                } else {
                    tracing::warn!(errmsg);
                    None
                }
            }
        } {
            tracing::warn!("spawned {}", lang.command.display());

            let child_stdin = child.stdin.take().unwrap();
            let child_stdout = BufReader::new(child.stdout.take().unwrap());

            let (child_tx, child_rx) = mpsc::channel(100);
            child_rxs.push(child_rx);
            // this will return none if the child has stopped running
            let id = match child.id() {
                Some(id) => id,
                None => {
                    if config.abort {
                        return Err(other_err!("{} exited early!", lang.command.display()));
                    } else {
                        continue;
                    }
                }
            };
            // marks whether an error has occured with the child,
            // if so, it should be dropped from the map
            let keep_child_in_map = Arc::new(AtomicBool::new(true));
            child_processes.insert(id, (child, keep_child_in_map.clone()));

            let rx = tx.subscribe();
            let keep_child = keep_child_in_map.clone();
            tokio::spawn(async move {
                if let Err(e) = proxy_stdin(child_stdin, rx).await {
                    tracing::error!("proxy stdin error: {e:#?}");
                    keep_child.store(false, std::sync::atomic::Ordering::Relaxed);
                }
            });

            tokio::spawn(async move {
                if let Err(e) = proxy_stdout(child_stdout, child_tx).await {
                    tracing::error!("proxy stdout error: {e:#?}");
                    keep_child_in_map.store(false, std::sync::atomic::Ordering::Relaxed);
                }
            });

            // Keep child process alive
            // child_processes.push(child);
        }
    }

    tokio::spawn(async move {
        let mut should_loop = !child_processes.is_empty();
        while should_loop {
            let mut remove_ids = vec![];
            for (id, (_child, keep)) in child_processes.iter() {
                if !keep.load(std::sync::atomic::Ordering::Relaxed) {
                    remove_ids.push(id.clone());
                }
            }
            for id in remove_ids.drain(..) {
                child_processes.remove(&id);
            }
            should_loop = !child_processes.is_empty();
        }
    });

    // read messages from child LSPs
    // TODO: merge server capabilities?
    tokio::spawn(async move {
        let mut stdout = io::stdout();
        let mut map = StreamMap::new();
        for (key, mut rx) in child_rxs.into_iter().enumerate() {
            let stream = Box::pin(async_stream::stream! {
                while let Some(value) = rx.recv().await {
                    yield value;
                }
            }) as Pin<Box<dyn Stream<Item = Value> + Send>>;
            map.insert(key, stream);
        }
        while let Some((_, value)) = map.next().await {
            let message = serde_json::to_string(&value).unwrap();
            debug!("received: {}", message);
            stdout
                .write_all(format!("Content-Length: {}\r\n\r\n", message.len()).as_bytes())
                .await
                .unwrap();
            stdout.write_all(message.as_bytes()).await.unwrap();
        }
    });

    // LSP server main loop
    // Read new command, send to all child LSP servers
    let mut stdin = BufReader::new(io::stdin());
    loop {
        let content_length = read_content_length(&mut stdin).await?;
        let mut body = vec![0u8; content_length];
        stdin.read_exact(&mut body).await.unwrap();
        let raw = String::from_utf8(body)?;
        debug!(request = %raw, "incoming lsp request");
        tx.send(raw.clone()).unwrap();
    }
}

#[derive(Debug, Parser)]
#[command(version)]
struct Cli {
    /// Configuration file path
    #[arg(short = 'c', long)]
    config: PathBuf,
    /// Select language servers by programming language name
    #[arg(short = 'l', long)]
    language: Option<String>,
}

#[tokio::main]
async fn main() -> MainResult<()> {
    let cli = Cli::parse();
    let config_content = fs::read_to_string(&cli.config)?;
    let mut lsp_config: LspConfig = toml_edit::easy::from_str(&config_content)?;

    if lsp_config.log_file.as_ref().is_some_and(|p| !p.exists()) {
        let path = lsp_config.log_file.as_ref().unwrap();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("failed to create parent for log file");
        }
    }

    if let Some(log_file_path) = lsp_config.log_file.as_ref() {
        let _ = std::fs::File::create(log_file_path).expect("failed to wipe log file");
    }

    if let Some(lang) = cli.language.as_deref() {
        lsp_config.languages.retain(|l| l.name == lang);
    }
    if lsp_config.languages.is_empty() {
        if let Some(lang) = cli.language.as_deref() {
            return Err(other_err!("No language server found for {}.", lang));
        }
        return Err(other_err!("No language server found."));
    }
    run(lsp_config).await?;
    Ok(())
}
