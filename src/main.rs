use std::{fs, io, path};
use std::error::Error;
use std::io::Write;

use clap::{App, Arg};

use peppi::game::{Game, SlippiVersion};

mod parquet;
mod transform;

const MAX_SUPPORTED_VERSION: SlippiVersion = SlippiVersion(3, 8, 0);

#[derive(Copy, Clone)]
enum Format {
	Json, Peppi, Rust
}

struct Opts {
	infile: String,
	outfile: String,
	format: Format,
	skip_frames: bool,
	enum_names: bool,
}

fn write_peppi<P: AsRef<path::Path>>(game: &Game, dir: P, skip_frames: bool) -> Result<(), Box<dyn Error>> {
	if game.start.slippi.version > MAX_SUPPORTED_VERSION {
		eprintln!("WARNING: unsupported Slippi version ({} > {}). Unknown fields will be omitted from output!",
			game.start.slippi.version, MAX_SUPPORTED_VERSION);
	}

	let dir = dir.as_ref();
	fs::create_dir_all(dir)?;
	fs::write(dir.join("metadata.json"), serde_json::to_string(&game.metadata_raw)?)?;
	fs::write(dir.join("start.json"), serde_json::to_string(&game.start)?)?;
	fs::write(dir.join("end.json"), serde_json::to_string(&game.end)?)?;
	if !skip_frames {
		let frames = transform::transpose_rows(&game.frames);
		if let Some(item) = &frames.item {
			parquet::write_items(item, dir.join("items.parquet"))?;
		}
		parquet::write_frames(&frames, dir.join("frames.parquet"))?;
	}

	Ok(())
}

fn write_json<W: Write>(game: &Game, mut out: W) -> Result<(), Box<dyn Error>> {
	writeln!(out, "{}", serde_json::to_string(game)?)?;
	Ok(())
}

fn write_rust<W: Write>(game: &Game, mut out: W) -> io::Result<()> {
	writeln!(out, "{:#?}", game)
}

fn write<W: Write>(game: &Game, out: W, format: Format) -> Result<(), Box<dyn Error>> {
	use Format::*;
	match format {
		Json => write_json(game, out)?,
		Rust => write_rust(game, out)?,
		_ => unimplemented!(),
	}
	Ok(())
}

fn inspect<R: io::Read>(mut buf: R, opts: &Opts) -> Result<(), Box<dyn Error>> {
	let game = peppi::game(&mut buf, Some(peppi::parse::Opts { skip_frames: opts.skip_frames }))?;
	use Format::*;
	match (opts.format, opts.outfile.as_str()) {
		(Peppi, "-") => Err("cannot output Peppi to STDOUT")?,
		(Peppi, o) => write_peppi(&game, o, opts.skip_frames),
		(format, "-") => write(&game, io::stdout(), format),
		(format, s) => write(&game, fs::File::create(s)?, format),
	}
}

fn parse_opts() -> Result<Opts, String> {
	let matches = App::new("slp")
		.version("0.1")
		.author("melkor <hohav@fastmail.com>")
		.about("Inspector for Slippi SSBM replay files")
		.arg(Arg::with_name("outfile")
			 .help("Output path")
			 .short("o")
			 .default_value("-"))
		.arg(Arg::with_name("format")
			 .help("Output format")
			 .short("f")
			 .possible_values(&["json", "peppi", "rust"])
			 .default_value("json"))
		.arg(Arg::with_name("names")
			.help("Append names for known constants")
			.short("n")
			.long("names"))
		.arg(Arg::with_name("skip-frames")
			.help("Don't output frame data")
			.short("s")
			.long("skip-frames"))
		.arg(Arg::with_name("game.slp")
			.help("Replay file to parse (`-` for STDIN)")
			.index(1))
		.get_matches();

	let infile = matches.value_of("game.slp").unwrap_or("-");
	let outfile = matches.value_of("outfile").unwrap();

	let format = {
		use Format::*;
		match matches.value_of("format").unwrap() {
			"json" => Json,
			"peppi" => Peppi,
			"rust" => Rust,
			_ => unimplemented!(),
		}
	};

	Ok(Opts {
		infile: infile.to_string(),
		outfile: outfile.to_string(),
		format: format,
		skip_frames: matches.is_present("skip-frames"),
		enum_names: matches.is_present("names"),
	})
}

pub fn main() -> Result<(), Box<dyn Error>> {
	pretty_env_logger::init();

	let opts = parse_opts()?;
	unsafe {
		peppi::SERIALIZATION_CONFIG = peppi::SerializationConfig {
			enum_names: opts.enum_names,
		}
	};

	match opts.infile.as_str() {
		"-" => inspect(io::stdin(), &opts),
		path => inspect(io::BufReader::new(fs::File::open(path)?), &opts),
	}
}
