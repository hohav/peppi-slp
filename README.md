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

Convert a replay to JSON (frame data is elided by default; use `-f` to override this):

```bash
$ slp -j game.slp
{"start":{"slippi":{"version":[1,0,0]},"bitfield":[50,1,76],"is_teams":false,"item_spawn_frequency":-1,"self_destruct_score":-1,"stage":8,"timer":480,"item_spawn_bitfield":[255,255,255,255,255],"damage_ratio":1.0,"players":[{"port":"P1","character":9,"type":0,"stocks":4,"costume":3,"team":null,"handicap":9,"bitfield":192,"cpu_level":null,"offense_ratio":0.0,"defense_ratio":1.0,"model_scale":1.0,"ucf":{"dash_back":null,"shield_drop":null}},{"port":"P2","character":2,"type":1,"stocks":4,"costume":0,"team":null,"handicap":9,"bitfield":64,"cpu_level":1,"offense_ratio":0.0,"defense_ratio":1.0,"model_scale":1.0,"ucf":{"dash_back":null,"shield_drop":null}}],"random_seed":3803194226},"end":{"method":3},"metadata":{"date":"2018-06-22T07:52:59Z","duration":5209,"platform":"dolphin","players":[{"port":"P1","characters":{"18":5209}},{"port":"P2","characters":{"1":5209}}]}}
```

Convert a replay to [HDF5](https://www.hdfgroup.org/solutions/hdf5/):

```bash
$ slp -o hdf5 game.slp > game.hdf5
```

Run `slp --help` for more info.
