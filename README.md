# slp

`slp` is an inspector & converter for Slippi replay files.

## Installation

Download the [latest release](https://github.com/hohav/peppi-slp/releases/latest), or get the source and build it with `cargo build --release` (requires Rust, which you can install with [rustup](https://rustup.rs/)).

## Usage

Run `slp --help` for a complete list of options.

### Examples

Print the post-frame action state for each port (player) on the last frame of the game:

```bash
$ slp game.slp | jq '.frames[-1].ports[].leader.post.state'
14
1
```

Convert a replay to JSON, skipping frame data:

```bash
# `-s` skips the frame data
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

## Peppi format

Convert a replay to [Peppi format](https://github.com/hohav/peppi#peppi-format):

```bash
$ slp -f peppi -o game.slpp game.slp
```

⚠️ **The Peppi format has been upgraded from v1 to v2**. The latest version of this tool (slp 0.5) can currently only handle v2 `.slpp` files. But you can upgrade a v1 file to v2 by converting it to `.slp` using the prior release of this tool, then back to `.slpp` using the latest release. I will work on adding backwards compatibility if there's demand, so please let me know if this is a problem for you!
