# slp

`slp` is an inspector/converter for Slippi replay files.

## Installation

Download the [latest release](https://github.com/hohav/peppi-slp/releases/latest), or get the source and build it with `cargo build --release`.

## Usage

Run `slp --help` for a complete list of options.

### Examples

Print the post-frame action state for each port (player) on the last frame of the game:

```bash
# `-n` to print states with human-readable names
$ slp -n game.slp | jq .frames[-1].ports[].leader.post.state
```

```json
"14:WAIT"
"1:DEAD_LEFT"
```

Convert a replay to JSON, skipping frame data:

```bash
# `-s` to skip frame data
$ slp -s game.slp | jq
```

```json
{
  "start": { ... },
  "end": {
    "method": 2,
    "lras_initiator": null
  },
  "metadata": {
    "startAt": "2020-08-01T19:41:19Z",
    "lastFrame": 11238,
    "players": {
      "1": {
        "names": {
          "netplay": "abbott",
          "code": "AAAA#123"
        },
        "characters": {
          "17": 11469
        }
      },
      "0": {
        "names": {
          "netplay": "costello",
          "code": "BBBB#456"
        },
        "characters": {
          "18": 11469
        }
      }
    },
    "playedOn": "dolphin"
  }
}
```

Convert a replay to Peppi format:

```bash
$ slp -f peppi -o game.slpp game.slp
```

## Peppi format

The Peppi format (.slpp) is a GNU tar archive containing the following files:

- peppi.json
- metadata.json
- start.json
- start.raw
- end.json
- end.raw
- frames.arrow

The last of these, `frames.arrow`, is an [Arrow IPC](https://arrow.apache.org/docs/format/Columnar.html#ipc-file-format) file containing all of the game's frame data. This is a columnar format, which makes .slpp files about twice as compressible as .slp files.
