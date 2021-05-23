# slp

`slp` is an inspector/converter for Slippi replay files.

## Installation

Build it with `cargo build --release`.

## Usage

Run `slp --help` for a complete list of options.

### Examples

Print the post-frame action state for each port (player) on the last frame of the game:

```bash
$ slp -nq frames[-1].ports[].leader.post.state game.slp
["14:WAIT","1:DEAD_LEFT"]
```

Convert a replay to JSON:

```bash
$ slp -s game.slp | jq # `-s` to skip frame data; `jq` for pretty-printing
{
  "metadata": {
    "date": "2018-06-22T07:52:59Z",
    "duration": 5209,
    "platform": "dolphin",
    "players": [
      {
        "port": "P1",
        "characters": {
          "18": 5209
        }
      },
      {
        "port": "P2",
        "characters": {
          "1": 5209
        }
      }
    ]
  },
  "start": {...},
  "end": {...}
}
```

Convert a replay to Peppi format:

```bash
$ slp -f peppi -o foo game.slp
```

This creates a directory `foo` with the following files:
```
start.json
end.json
metadata.json
frames.parquet
items.parquet
```

Frame and item data are stored in columnar [Parquet](https://parquet.apache.org/) format. It will be larger than the original .slp file uncompressed, but smaller compressed.
