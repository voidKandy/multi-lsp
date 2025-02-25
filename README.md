# multi-lsp

> **IMPORTANT!**: This is a fork of the archived [multi-lsp-proxy](https://github.com/messense/multi-lsp-proxy) project. 
A **barely working** LSP Proxy to multiple language servers, to use multiple LSPs per language in
editors that doesn't support multiple LSPs per language natively like Helix.

## Installation
All you need to do to install is build the binary and make it accessible in your `$PATH`
```shell
cargo build
mv ./target/debug/multi-lsp /path/in/$PATH
```

## Usage

```bash
Usage: multi-lsp [OPTIONS] --config <CONFIG>

Options:
  -c, --config <CONFIG>      Configuration file path
  -l, --language <LANGUAGE>  Select language servers by programming language name
  -h, --help                 Print help
  -V, --version              Print version
```

To use with Helix, set the `language-server` option in `languages.toml`,
below is an example for Python that enables both `pyright-langserver` and `ruff-lsp`:

```toml
# Helix languages.toml file
[[language]]
 name = "python"
 scope = "source.python"
 injection-regex = "python"
 file-types = ["py", "pyi"]
 shebangs = ["python"]
 roots = ["pyproject.toml", "setup.py", "Poetry.lock"]
 comment-token = "#"
 language-server = { command = "multi-lsp-proxy", args = ["--config", "/path/to/multi-lsp-config.toml"] }
 auto-format = false
 indent = { tab-width = 4, unit = "    " }
 config = {}
```

and configure multi-lsp in `multi-lsp-config.toml`

```toml
log-file = "/tmp/multi-lsp-proxy.log"
# Defaults to false
# If true the failure of ANY lsp of the multiples will cause the whole proxy server to crash
# If false, individual servers can crash/not startup without the whole proxy failing
abort = true

[[language]]
name = "python"
command = "pyright-langserver"
args = ["--stdio"]

[[language]]
name = "python"
command = "ruff-lsp"
```

## License

This work is released under the MIT license. A copy of the license is provided in the [LICENSE](./LICENSE) file.
