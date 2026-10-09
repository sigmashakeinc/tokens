# tokens

`tokens` shows how many tokens your AI coding sessions use and what they cost.
It reads the session logs of Claude Code, Codex, and Grok on your computer. It
keeps the usage history in a local SQLite file. It shows the data in a web
dashboard that updates while you work.

Everything stays on your computer. The service listens on `127.0.0.1` only. It
has no account, no login, and no cloud service.

## Platforms

| Platform | Support |
| --- | --- |
| Windows x64 | Yes |
| macOS arm64 (Apple Silicon) | Yes |
| Linux x64 | Yes |

No other platform has a build. On any other platform, the installer prints a
message and exits with an error.

## Install

Linux x64 and macOS arm64:

```sh
curl -fsSL https://raw.githubusercontent.com/sigmashakeinc/tokens/main/install.sh | sh
```

Windows (PowerShell):

```powershell
powershell -ExecutionPolicy ByPass -c "irm https://raw.githubusercontent.com/sigmashakeinc/tokens/main/install.ps1 | iex"
```

The installer does these steps:

1. It finds your operating system and CPU.
2. It downloads the matching archive from the latest GitHub release.
3. It checks the SHA-256 of the archive against `checksums.txt` from the same
   release. If the check fails, it installs nothing.
4. It puts `tokens` in `~/.local/bin` (Linux and macOS) or in
   `%LOCALAPPDATA%\Programs\tokens` (Windows). On Windows it also adds that
   directory to your user `PATH`.

To change the installer behavior, use these settings:

| Setting | Linux and macOS | Windows |
| --- | --- | --- |
| Release to install | `VERSION=1.2.3` or `--version 1.2.3` | `$env:VERSION = "1.2.3"` or `-Version 1.2.3` |
| Install directory | `INSTALL_DIR=/path` or `--install-dir /path` | `$env:INSTALL_DIR = "C:\path"` or `-InstallDir C:\path` |
| Install a local archive | `--from-file FILE` (also needs `checksums.txt` beside it, or `--checksums FILE`) | `-FromFile FILE` (same rule, `-Checksums FILE`) |

To use a setting with `curl | sh`, put the options after `sh -s --`. For
example: `curl -fsSL URL | sh -s -- --version 1.2.3`.

You can also download an archive by hand from the
[releases page](https://github.com/sigmashakeinc/tokens/releases). The archive
names are `tokens_<version>_<os>_<arch>.tar.gz` (`.zip` on Windows). The values
for `<os>` are `linux`, `darwin`, and `windows`. The values for `<arch>` are
The release has exactly three archives: `linux_x64`, `darwin_arm64`, and
`windows_x64`.

## Run

Start the service in the foreground. This command is the same on every
operating system:

```sh
tokens serve
```

Open <http://127.0.0.1:7913>. The first start reads all old session logs. This
can take some time if you have many logs. Later starts read only new data.

Stop the service with `Ctrl+C`.

To keep the service running after you log out, use the method of your system:

- Linux (optional): install the systemd user unit from a source checkout. It
  needs Bun. This unit also sets a memory and CPU limit for the service and its
  child processes.

  ```sh
  bun install
  bash scripts/install-systemd.sh --start
  ```

  Use `systemctl --user status sigmashake-tokens.service` to see its state. Use
  `journalctl --user -u sigmashake-tokens.service -f` to read its log.
- macOS: create a launchd agent that runs `tokens serve`.
- Windows: create a Task Scheduler task that runs `tokens serve` at logon.

## Commands

| Command | What it does |
| --- | --- |
| `tokens serve` | Runs the service (dashboard and API) in the foreground. |
| `tokens health` | Checks that the service is up. |
| `tokens usage [--from DATE] [--to DATE] [--models a,b] [--projects a,b]` | Prints aggregated usage as JSON. |
| `tokens rate` | Prints rolling burn-rate windows (1 minute to 1 hour). |
| `tokens day YYYY-MM-DD` | Prints the sessions of one day. |
| `tokens session ID` | Prints the events of one session. |
| `tokens stats` | Prints store statistics. |
| `tokens report [--days N]` | Prints a text summary. It scans the logs and reads the store directly. The service does not need to run. |
| `tokens govern [--hook] [--enforce]` | Policy gate for autonomous agent runs (see below). |
| `tokens mcp` | Runs the MCP server for AI agents (see below). |
| `tokens --version` | Prints the version. |

The commands that query a running service use `TOKENS_BASE_URL`
(default `http://127.0.0.1:7913`).

### MCP server

`tokens mcp` is a stdio MCP server. It gives an AI agent these tools:
`tokens_health`, `tokens_usage`, `tokens_rate`, `tokens_day`, `tokens_session`,
and `tokens_stats`. The service must be running. To register it with Claude
Code:

```sh
claude mcp add tokens -- tokens mcp
```

### Governor

`tokens govern` is a policy gate for autonomous agent runs. It reads the
rolling `/api/tokenomics` state. In `--hook` mode it accepts Claude Code hook
JSON from stdin. It returns exit code `2` only when `--enforce` is set and a
hard policy blocks the next tool call. Without `--enforce`, it only advises.

The default policy:

- It blocks verbose Bash commands (large diffs, raw log reads, broad searches)
  that are not shaped by a reducer such as `stk` or `headroom`.
- It blocks when the rolling window tokens exceed
  `TOKENOMICS_GOVERNOR_MAX_WINDOW_TOKENS` (default `250000000`).
- It blocks when the cache-read to input ratio exceeds
  `TOKENOMICS_GOVERNOR_MAX_CACHE_READ_RATIO` (default `8`).
- In enforce mode, it treats capped telemetry as unsafe unless `--allow-capped`
  or `TOKENOMICS_GOVERNOR_FAIL_ON_CAPPED=false` is set.
- It treats missing telemetry as advisory. Use `--fail-unknown` to fail closed.

## Where the data is

| Data | Default location | Override |
| --- | --- | --- |
| SQLite store | `<home>/.sigmashake/tokens.sqlite` | `TOKENS_DB_PATH`, or `SIGMASHAKE_HOME` for the directory |
| Claude Code logs | `<home>/.claude/projects` | `CLAUDE_PROJECTS_DIR`, or `CLAUDE_CONFIG_DIR` |
| Codex logs | `<home>/.codex/sessions` | `CODEX_SESSIONS_DIR`, or `CODEX_HOME` |
| Grok logs | `<home>/.grok/logs`, `<home>/.grok/sessions` | `GROK_LOGS_DIR`, `GROK_SESSIONS_DIR` |

`<home>` is the home directory of the current user (`%USERPROFILE%` on
Windows). The store keeps events after the source logs are rotated or deleted.
Do not delete the store file if you want to keep the history.

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `TOKENS_PORT` | `7913` | HTTP port |
| `TOKENS_HOST` | `127.0.0.1` | Bind address. Keep it on loopback. The service has no login. |
| `TOKENS_POLL_MS` | `5000` | Ingest interval in milliseconds |
| `TOKENS_BASE_URL` | `http://127.0.0.1:7913` | Service URL for `tokens health`, `usage`, and the other query commands |
| `TOKENS_WORKER_LAUNCHER` | automatic | `direct` always starts child processes directly. `systemd` forces systemd transient units (Linux only). |

## HTTP API

| Path | Description |
| --- | --- |
| `GET /` | Dashboard |
| `GET /healthz` | Liveness |
| `GET /api/usage` | Aggregated usage. Parameters: `from`, `to`, `models`, `projects` |
| `GET /api/rate` | Rolling-window burn rate |
| `GET /api/day?date=YYYY-MM-DD` | Sessions of one day |
| `GET /api/session?id=ID` | Events of one session |
| `GET /api/stats` | Store statistics |
| `GET /api/live` | Server-Sent Events feed of new usage |

## How it works

1. The scanner walks the log directories. It reads only the bytes that were
   added since the last scan. It keeps a cursor for each file.
2. The store is a SQLite file in WAL mode. It keeps every usage event.
3. A background loop runs every 5 seconds. It runs the scan in a child
   process, so a slow scan does not block the HTTP server.
4. The dashboard (`src/public/`) draws the charts. It subscribes to the live
   feed, so the numbers update when a new assistant turn ends.

Do not open the SQLite file while the service runs. Use the API or the CLI.

## Notes

- Cost is an estimate. It uses the public list prices of the providers. The
  price table is in `src/pricing.ts`. A model that is not in the table uses the
  Sonnet price tier, so a new model can show a wrong cost until the table is
  updated.
- The dashboard loads Chart.js from `cdn.jsdelivr.net`. The page needs internet
  access to draw charts. The service itself does not.

## Develop

You need [Bun](https://bun.sh) 1.4 or later.

```sh
bun install
bun run dev           # hot-reload server
bun run typecheck
bun test test/
bun run build         # compile dist/tokens for this computer
bun scripts/smoke.ts dist/tokens
```

`bun test` runs the full suite. A few tests are for Linux and systemd only.
They skip on other systems.

## Releases

SigmaShake builds a binary for each of the three platforms with
`bun build --compile`. It makes the archives and `checksums.txt`, and creates
the GitHub release.

## Contributing

SigmaShake maintains this repository. The repository does not accept outside issues or pull requests. Issues and outside pull requests are disabled.

## License

MIT. See [LICENSE](LICENSE).
