use clap::Parser;
use lsp_server::{Message, RequestId};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::HashMap,
    fs,
    path::PathBuf,
    pin::Pin,
    process::{Output, Stdio},
    sync::{
        atomic::AtomicBool,
        mpsc::{self, TryRecvError},
        Arc, LazyLock,
    },
};
use tokio::process::Child;
use tokio::{
    io::{self, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{ChildStdin, ChildStdout, Command},
    sync::broadcast,
    task::JoinHandle,
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

#[derive(Debug, Deserialize, Serialize)]
struct TaggedMessage {
    msg: lsp_server::Message,
    id: String,
}

struct ChildServer {
    id: String,
    process: Child,
    stdout: BufReader<ChildStdout>,
    stdin: ChildStdin,
    /// Receives from proxy
    /// Messages received here are passed to stdout of this process
    rx: mpsc::Receiver<TaggedMessage>,
    /// Sends to proxy from proxy
    tx: mpsc::Sender<TaggedMessage>,
}

struct ChildServerHandle {
    tx: mpsc::Sender<TaggedMessage>,
    handle: JoinHandle<MainResult<()>>,
}

struct ProxyServer {
    children: HashMap<String, ChildServerHandle>,
    rx: mpsc::Receiver<TaggedMessage>,
    lsp_connection: lsp_server::Connection,
}

/// Message Ids for requests and responses are prepended with `{child_id}{ID_DELIM}`
const ID_DELIM: &str = "---";
impl ProxyServer {
    async fn init(config: &LspConfig) -> MainResult<Self> {
        let (_tx, rx) = mpsc::channel();
        let children = create_and_spawn_childen(&_tx, config).await?;
        Ok(Self {
            rx,
            children,
            lsp_connection: create_lsp_connection()?,
        })
    }

    async fn main_loop(&mut self) {
        loop {
            match self.lsp_connection.receiver.try_recv() {
                Ok(msg) => {
                    tracing::warn!("received message from lsp: {msg:#?}");
                    if let Some(id) = match msg {
                        Message::Request(ref rq) => Some(rq.id.to_string()),
                        Message::Response(ref rs) => Some(rs.id.to_string()),
                        _ => None,
                    }
                    .and_then(|idstr| match idstr.split_once(ID_DELIM) {
                        Some((before, _after))
                            if self
                                .children
                                .keys()
                                .find(|k| k.as_str() == before)
                                .is_some() =>
                        {
                            Some(before.to_owned())
                        }
                        _ => None,
                    }) {
                        tracing::warn!("sending request or response to child with id: {id}");
                        self.children
                            .get(&id)
                            .unwrap()
                            .tx
                            .send(TaggedMessage { msg, id })
                            .expect("failed to send message");
                    } else {
                        tracing::warn!("sending message: {msg:#?} to all children");
                        for (id, child) in self.children.iter_mut() {
                            child
                                .tx
                                .send(TaggedMessage {
                                    msg: msg.clone(),
                                    id: id.to_owned(),
                                })
                                .expect("failed to send message");
                        }
                    }
                }
                Err(err) if err.is_empty() => {}
                Err(err) if err.is_disconnected() => {
                    tracing::error!("Lsp Disconnected");
                }
                Err(err) => {
                    tracing::error!("unexpected recv error: {err:#?}");
                }
            };

            match self.rx.try_recv() {
                Ok(TaggedMessage { mut msg, id }) => {
                    if let Some(msgid) = match msg {
                        Message::Request(ref mut rq) => Some(&mut rq.id),
                        Message::Response(ref mut rs) => Some(&mut rs.id),
                        _ => None,
                    } {
                        if !msgid.to_string().contains(&id) {
                            *msgid =
                                RequestId::from(format!("{id}{ID_DELIM}{}", msgid.to_string()));
                        }
                    }
                    self.lsp_connection
                        .sender
                        .send(msg)
                        .expect("failed to send");
                }
                Err(TryRecvError::Empty) => {}
                Err(TryRecvError::Disconnected) => {
                    tracing::warn!("Disconnected");
                    break;
                }
            }
        }
    }
}

impl ChildServer {
    async fn spawn(mut self) -> JoinHandle<MainResult<()>> {
        tokio::spawn(async move {
            match self.rx.try_recv() {
                Ok(msg) => {
                    tracing::warn!("recieved msg");
                    let bytes = serde_json::to_vec(&msg)?;
                    self.stdin
                        .write_all(format!("Content-Length: {}\r\n\r\n", bytes.len()).as_bytes())
                        .await?;
                    self.stdin.write_all(&bytes).await?;
                }
                Err(TryRecvError::Empty) => {}
                Err(TryRecvError::Disconnected) => {
                    tracing::warn!("Disconnected");
                    return Ok(());
                }
            }

            let msg = read_message(&mut self.stdout).await?;
            let message = TaggedMessage { msg, id: self.id };
            self.tx.send(message).expect("Failed to send message");
            Ok(())
        })
    }
}

fn create_lsp_connection() -> MainResult<lsp_server::Connection> {
    let (lsp_connection, _io_threads) = lsp_server::Connection::stdio();
    // idk if default is correct here
    let server_capabilities = lsp_types::ServerCapabilities::default();
    let server_capabilities = serde_json::to_value(server_capabilities)?;
    let params = lsp_connection
        .initialize(server_capabilities)
        .expect("failed to initialize");
    let _: lsp_types::InitializeParams = serde_json::from_value(params).unwrap();
    Ok(lsp_connection)
}

async fn create_and_spawn_childen(
    parent_tx: &mpsc::Sender<TaggedMessage>,
    config: &LspConfig,
) -> MainResult<HashMap<String, ChildServerHandle>> {
    let mut map = HashMap::new();
    for lang in &config.languages {
        // spawn LSP server command
        let mut cmd = Command::new(&lang.command);
        cmd.args(&lang.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped());
        match cmd.spawn() {
            Ok(mut process) => {
                tracing::warn!("spawned {}", lang.command.display());
                let stdin = process.stdin.take().unwrap();
                let stdout = BufReader::new(process.stdout.take().unwrap());

                let (tx, rx) = mpsc::channel();
                // this will return none if the child has stopped running
                let id = match process.id() {
                    Some(id) => id,
                    None => {
                        let error = other_err!("{} exited early!", lang.command.display());
                        tracing::error!("{error:#?}");
                        if config.abort {
                            return Err(error);
                        }
                        continue;
                    }
                };

                let name = format!("process_{}_{id}", lang.name);
                let child_server = ChildServer {
                    id: name.to_owned(),
                    tx: parent_tx.clone(),
                    rx,
                    stdin,
                    stdout,
                    process,
                };
                let child_handle = ChildServerHandle {
                    tx,
                    handle: child_server.spawn().await,
                };

                map.insert(name, child_handle)
            }
            Err(e) => {
                let error =
                    other_err!("Failed to create child process for language: {lang:#?}\n{e}");
                tracing::error!("{error:#?}");
                if config.abort {
                    return Err(error);
                }
                continue;
            }
        };
    }
    Ok(map)
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

async fn read_message<T>(reader: &mut BufReader<T>) -> MainResult<lsp_server::Message>
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

const CONFIG: LazyLock<LspConfig> = LazyLock::new(|| {
    let cli = Cli::parse();
    let config_content = fs::read_to_string(&cli.config).expect("error getting  config_content");
    let lsp_config: LspConfig =
        toml_edit::easy::from_str(&config_content).expect("error getting  lsp_config");
    lsp_config
});
#[tokio::main]
async fn main() -> MainResult<()> {
    let cli = Cli::parse();
    let c = CONFIG;
    let mut lsp_config = LazyLock::force(&c).clone();
    if lsp_config.log_file.as_ref().is_some_and(|p| !p.exists()) {
        let path = lsp_config.log_file.as_ref().unwrap();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("failed to create parent for log file");
        }
    }

    let mut _tracing_guard = None;
    if let Some(log_file_path) = lsp_config.log_file.as_ref() {
        let _ = std::fs::File::create(log_file_path).expect("failed to wipe log file");
        let directory = log_file_path.parent().unwrap();
        let file_name = log_file_path.file_name().unwrap();
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
        tracing::warn!("tracing initialized");
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

    let mut proxy_server = ProxyServer::init(&lsp_config).await?;

    proxy_server.main_loop().await;
    Ok(())
}
