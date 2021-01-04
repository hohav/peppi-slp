# slp

`slp` is an inspector/converter for Slippi replay files.

## Installation

Build it with `cargo build --release` (requires nightly Rust).

## Usage

Print the post-frame action state for each port (player) on the last frame of the game:

```bash
$ slp -nq frames[-1].ports[].leader.post.state game.slp
["14:WAIT","1:DEAD_LEFT"]
```

Run `slp --help` for more info.
