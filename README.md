[![Build and Release](https://github.com/MADS-NET/mongodb_plugin/actions/workflows/release.yml/badge.svg)](https://github.com/MADS-NET/mongodb_plugin/actions/workflows/release.yml) ![Dynamic JSON Badge](https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fraw.githubusercontent.com%2FMADS-NET%2F.github%2Fmain%2Fprofile%2Fpackages.json&query=%24.packages.%5B'mongodb.plugin'%5D.type&label=mads%20package&logo=C%2B%2B)


# mongodb plugin for MADS

MongoDB plugins for [MADS](https://github.com/MADS-NET/MADS), formerly part of
MADS itself:

- `mongodb.plugin`, a **filter** that logs every message it receives to
  MongoDB, one collection per topic, and periodically publishes its state on
  `logger_status`. It replaces the `mads-logger` agent and writes the same
  documents: `mads logger` is now a deprecated shortcut for
  `mads filter mongodb.plugin -n logger -b`.
- `replay.plugin`, a **source** that replays collections written by
  `mongodb.plugin`, in timestamp order and with the original timing.

The MongoDB C and C++ drivers are fetched and linked statically into the
plugins: nothing else needs to be installed.

*Required MADS version: 3.0.0.*


## Supported platforms

Currently, the supported platforms are:

* **Linux** 
* **MacOS**
* **Windows**


## Package install

With MADS v3.0.0 or later, install both plugins with:

```bash
mads package --install mongodb.plugin
```

Release ZIPs for Linux (x86_64, arm64), macOS (universal) and Windows
(x86_64) are built by CI for each `v*.*.*` tag.


## Install from source

Linux and MacOS:

```bash
cmake -Bbuild -DCMAKE_INSTALL_PREFIX="$(mads -p)"
cmake --build build -j4
sudo cmake --install build
```

Windows:

```powershell
cmake -Bbuild -DCMAKE_INSTALL_PREFIX="$(mads -p)"
cmake --build build --config Release
cmake --install build --config Release
```


## INI settings

The plugin supports the following settings in the INI file:

### mongodb.plugin

Run it with `mads filter mongodb.plugin -n logger -b` to keep using an
existing `[logger]` section:

```ini
[logger]
mongo_uri = "mongodb://localhost:27017"   # default
mongo_db = "mads"                         # default
initially_paused = false                  # default
status_period_ms = 1000                   # default; 0 disables the status
dont_block = true                         # or -b: needed for the status when idle
sub_topic = [""]                          # log every topic
```

Each message becomes a document `{timestamp, message}` (plus `data`, the raw
bytes, for blob messages), where `timestamp` is the reception time and
`message` the payload; `$date` objects in the payload are stored as BSON
dates. A message on the `metadata` topic with a boolean `pause` field pauses
or resumes logging. Every `status_period_ms` the plugin publishes
`{"logger_paused": <bool>}` on the `logger_status` topic; in blocking mode
(no `dont_block`) that only happens when messages arrive. At shutdown the plugin stores its own shutdown event in
`agent_event` and indexes `message.timestamp`, `message.hostname` and
`message.timecode` in every collection it wrote.

### replay.plugin

```ini
[replay]
db_uri = "mongodb://localhost:27017"
db_name = "mads_test"                    # mandatory
collections = ["topic_a", "topic_b"]     # mandatory
start_time = "2026-03-10T20:27:00Z"      # optional, ISO 8601 UTC or epoch ms
end_time = "2026-03-10T20:27:10Z"        # optional, default: now
max_loop_duration_ms = 0                 # cap on the wait between records
repeat = false
unwrap_original = false                  # true: publish only `message`
view_name = ""                           # non-empty: keep a view with this name
reuse_view = false                       # true: reuse the existing view_name
```

Unless stated otherwise, settings are optional.




## Executable demo

`./build/mongodb` checks the JSON-to-BSON conversion against the driver's own
parser. With `MONGO_TEST_URI` set, it also writes test documents to the
`mads_plugin_test` database and checks them, and `./build/replay` then replays
them (on macOS the executables are `build/mongodb.plugin` and
`build/replay.plugin`):

```bash
docker run -d --rm -p 27017:27017 mongo
MONGO_TEST_URI=mongodb://localhost:27017 ./build/mongodb
MONGO_TEST_URI=mongodb://localhost:27017 ./build/replay
```

---

# LICENSE

Apache 2.0
(2026) Paolo Bosetti