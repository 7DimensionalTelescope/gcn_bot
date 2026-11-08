# GCN Alert Bot

Real-time monitoring of the [GCN](https://gcn.nasa.gov) (Gamma-ray Coordinates Network) Kafka stream. The bot parses transient alerts (GRBs, neutrinos, …), posts them to Slack with visibility and tile-coverage analysis, and coordinates follow-up with Target-of-Opportunity (ToO) requests for the **7DT** telescope array and **RASA36** (both at CTIO, Chile). LOAO (Arizona) visibility is plotted for reference.

## Features

- **Real-time monitoring** of the GCN Kafka stream with heartbeat tracking, reconnect handling, and a per-message Kafka timestamp / lag log.
- **Multi-facility parsing**: Fermi, Swift, Einstein Probe, SVOM, IceCube, HAWC, AMON, CALET — plus GCN circulars (first-detection and follow-up).
- **Slack posts** (Block Kit) with one thread per event: later notices and circulars for the same event are added to the original thread, even after the event has left the recent-events file.
- **Visibility analysis** (altitude and moon separation) for 7DT/RASA36 (shared CTIO site) and LOAO, rendered as plots in the thread.
- **Tile coverage** of the localization for 7DT and RASA36, with an optional GW Portal reference-image check (7DT only).
- **Automatic ToO emails** for GRBs, per telescope and per tile case, with a deferred send for targets that are about to rise (see below).
- **Manual ToO** from a Slack button (modal pre-filled with the automatic config).
- **Databases**: notices, circulars, and a rolling list of recent targets (`grb_targets.ascii`) with duplicate detection.

## How it works

```
GCN Kafka stream
  └─ GCNConsumer (gcn/consumer.py)
       ├─ gcn.circulars      → GCNCircularHandler (gcn/circular_handler.py)
       └─ all other topics   → GCNNoticeHandler   (gcn/notice_handler.py)
            │
            ├─ SlackManager + MessageFormatter      (slack/)         → Slack thread
            ├─ VisibilityManager                    (visibility/plotter.py)  → plots
            ├─ TileManager × 2 (7DT, RASA36)        (visibility/tiles.py)
            └─ GCNToOEmailer × 2 (7DT, RASA36)      (too/emailer.py) → ToO email
                 └─ DeferredToOScheduler            (too/scheduler.py)
```

| Path | Responsibility |
|---|---|
| `main.py` | `GCNBot` orchestrator and CLI entry point |
| `config.py` | `BotConfig` — loads `settings.toml` |
| `settings_template.toml` | Template for the (git-ignored) `settings.toml` |
| `gcn/` | Kafka consumer, notice parsing, circular parsing, event databases |
| `slack/` | Slack client, message formatting, manual-ToO modal, per-event status message |
| `too/` | ToO criteria, SMTP sending, deferred-send scheduler |
| `visibility/` | Visibility plots and tile coverage |
| `api/` | GW Portal REST client (optional) |
| `supy/` | Visibility / tile library (git submodule) |

## Installation

Requires Python 3.11+ (the config loader uses `tomllib`).

```bash
git clone --recurse-submodules git@github.com:7DimensionalTelescope/gcn_bot.git
cd gcn_bot
# if the clone was made without --recurse-submodules:
git submodule update --init

conda env create -f environment.yml
conda activate gcn-bot

cp settings_template.toml settings.toml   # then fill in your credentials
```

`settings.toml` holds credentials (Slack tokens, GCN client ID/secret, email app password) and is git-ignored — never commit it.

## Configuration

All runtime settings live in `settings.toml`; nothing tunable is hard-coded. The template documents every key. The important groups:

| Group | Keys |
|---|---|
| Credentials | `GCN_ID`, `GCN_SECRET`, `SLACK_TOKEN`, `SLACK_APP_TOKEN`, `SLACK_CHANNEL`, `EMAIL_FROM`, `EMAIL_PASSWORD` |
| Topics | `DISPLAY_TOPICS` — comment/uncomment to add or remove sources |
| Visibility | `MIN_ALTITUDE` (30°), `MIN_MOON_SEP` (30°) |
| Output files | `OUTPUT_NOTICE_CSV`, `OUTPUT_CIRCULAR_CSV`, `OUTPUT_ASCII`, `ASCII_MAX_EVENTS`, `OUTPUT_THREAD_INDEX`, `OUTPUT_TOO_SLOTS` |
| Manual ToO | `TURN_ON_TOO_EMAIL_SLACK`, `TOO_CONFIG` (7DT form defaults), `EMAIL_TO`, `EMAIL_TO_RASA36` |
| Automatic ToO | the four enable flags, `TURN_ON_TOO_EMAIL_AUTO_ON_UPDATE`, `AUTO_TOO_MAX_TILES`, `TOO_AUTO_GRB_ONLY`, `TOO_OBSERVABLE_SOON_HOURS`, `TOO_DEFERRED_CHECK_INTERVAL_SEC`, `TOO_CONFIG_{7DT,RASA36}_AUTO_{SINGLE,MULTI}` |
| Email transport | `SMTP_KEEPALIVE_SEC` (keep a logged-in SMTP connection warm; `0` = new connection per send) |
| GW Portal | `TURN_ON_DB_TILE_CHECK` (needs `GWPORTAL_BASE_URL` / `GWPORTAL_API_KEY`, or the like-named environment variables) |

## Usage

Run from the repository root:

```bash
# Production
python main.py

# Development: also process *_TEST topics and post to Slack (--send is required with --include-test)
python main.py --include-test --send

# Alternative settings file / logging
python main.py --config path/to/settings.toml --log-level DEBUG --log-file bot.log
```

| Option | Meaning |
|---|---|
| `--include-test` | Also process topics containing `_TEST` |
| `--send` | Deliver to Slack when `--include-test` is used. Normal runs always post; an `--include-test` run without `--send` posts nothing and sends no automatic ToO |
| `--config PATH` | Use another `settings.toml` |
| `--log-level` | `DEBUG` / `INFO` / `WARNING` / `ERROR` (default `INFO`) |
| `--log-file PATH` | Also write logs to a file (or set `LOG_FILE` in settings) |

## ToO requests

### Automatic ToO

A ToO email is sent automatically when **all** of the following hold:

1. **Enabled**: the flag for that telescope and tile case is on — `TURN_ON_TOO_EMAIL_AUTO_7DT_SINGLE` / `_7DT_MULTI` / `_RASA36_SINGLE` / `_RASA36_MULTI`. Single = 1 tile, multi = 2..`AUTO_TOO_MAX_TILES`. Each combination is toggled independently.
2. **GRB source**: with `TOO_AUTO_GRB_ONLY = true`, only GRB facilities qualify (Swift, Fermi, CALET, Einstein Probe, SVOM-*); neutrino and other multi-messenger alerts are excluded.
3. **Localization**: it covers between 1 and `AUTO_TOO_MAX_TILES` (default 5) tiles of that telescope. A missing error radius or a zero-tile result counts as unconfirmed and is suppressed.
4. **Visibility**: the target is observable now, or rises within `TOO_OBSERVABLE_SOON_HOURS` (default 2 h).

Observation parameters come from `TOO_CONFIG_{7DT,RASA36}_AUTO_{SINGLE,MULTI}`, chosen by the tile count when the email is sent. Defaults: 7DT Spec mode, 100 s × 3; RASA36 rapid ToO, 60 s × 55, priority 1.

- **Observable now** → the email is sent immediately (in the background, parallel to the Slack post and plots).
- **Observable soon** → the send is *deferred* until the target is up, then re-checked against the latest information (retraction, refined coordinates, tile count, visibility) before sending. Pending fire times are stored in `grb_targets.ascii` and re-armed after a restart.
- **Duplicates**: a per-telescope marker in `grb_targets.ascii` prevents a second send for the same event, including after a manual request.
- **Updates**: with `TURN_ON_TOO_EMAIL_AUTO_ON_UPDATE = true`, a refined position that newly qualifies can also trigger a ToO (e.g. a wide Fermi box shrinking to a single tile).

Each event thread gets one editable **🤖 Auto-ToO** message showing sent / failed / scheduled / outcome lines per telescope.

### Manual ToO

With `TURN_ON_TOO_EMAIL_SLACK = true`, each new-event message carries **Submit 7DT ToO Request** and **Submit RASA36 ToO Request** buttons for authorized users. The RASA36 form is pre-filled with the automatic RASA36 config for that event, so a manual request starts from the same values the automatic one would use.

### Email delivery

ToO emails are sent over SMTP from `EMAIL_FROM` to `EMAIL_TO` (7DT) and `EMAIL_TO_RASA36` (RASA36). Bot mail can land in the recipients' spam folder — add a mail filter on the sender address. The log line for each send includes the SMTP host, recipients, Message-ID and timing (`build`, `smtp`, `reused|new connection`) to help trace a "sent but not received" report.

## Output files

Written next to `main.py` by default (paths are configurable):

| File | Content |
|---|---|
| `gcn_notices.csv` | Parsed notices, deduplicated by (facility, trigger) |
| `gcn_circular.csv` | Parsed GCN circulars |
| `grb_targets.ascii` | Rolling list of the most recent events (`ASCII_MAX_EVENTS`, default 10), used for observation planning; also holds the ToO-sent and deferred markers |
| `thread_index.json` | Runtime state: event → Slack `thread_ts`, so late notices stay in their thread (git-ignored) |
| `too_slots.json` | Runtime state: Auto-ToO status message per thread (git-ignored) |
| `gcn_bot.log` | Log file |

## Monitored facilities

- **Fermi**: GBM and LAT
- **Swift**: BAT, XRT, UVOT
- **Einstein Probe**: WXT
- **SVOM**: GRM, ECLAIRs, MXT
- **IceCube**, **HAWC**, **AMON**: neutrino / multi-messenger alerts
- **CALET**: gamma-ray burst monitor

Which topics are actually subscribed is controlled by `DISPLAY_TOPICS` in `settings.toml`.

## Logging

All modules log through the standard `logging` module with one format (`time [LEVEL] module — message`). A startup banner and a "GCNBot ready" summary (channel, topics, telescopes, send mode, auto-ToO) are printed at launch. For every non-heartbeat Kafka message the consumer logs the broker timestamp and the Kafka→bot lag, which helps separate source/GCN delay from bot delay.

## License

MIT — see [LICENSE](LICENSE).

## Acknowledgments

- NASA GCN team for the Kafka alert stream
- The astronomical community for the collaborative alert system
