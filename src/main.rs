//TODO: split this file up
#![allow(clippy::redundant_field_names)]

use std::{
	error::Error,
	fs::File,
	io::{self, BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
	path::PathBuf,
};

use clap::{Arg, ArgAction, Command};
use log::{debug, error, info, log, Level, LevelFilter};
use xxhash_rust::xxh3::Xxh3;

use arrow2::io::{
	ipc::write::Compression,
	json::write as json_write,
};

use peppi::{
	frame::PortOccupancy,
	game::{immutable::Game, ICE_CLIMBERS},
	io::{
		peppi as io_peppi,
		slippi,
	},
};

#[derive(Clone, Copy, Debug, PartialEq)]
enum Format {
	Json, Peppi, Slippi, Null
}

impl TryFrom<&str> for Format {
	type Error = String;
	fn try_from(s: &str) -> Result<Self, Self::Error> {
		match s {
			"json" => Ok(Format::Json),
			"peppi" => Ok(Format::Peppi),
			"slippi" => Ok(Format::Slippi),
			"null" => Ok(Format::Null),
			s => Err(format!("invalid format: {}", s)),
		}
	}
}

fn parse_compression(s: &str) -> Result<Compression, String> {
	use Compression::*;
	match s {
		"lz4" => Ok(LZ4),
		"zstd" => Ok(ZSTD),
		s => Err(format!("invalid compression: {}", s)),
	}
}

struct Opts {
	debug_dir: Option<PathBuf>,
	infile: Option<PathBuf>,
	input_format: Option<Format>,
	log_level: LevelFilter,
	no_verify: bool,
	outfile: Option<PathBuf>,
	output_format: Format,
	compression: Option<Compression>,
	short: bool,
}

fn port_occupancy(game: &Game) -> Vec<PortOccupancy> {
	game.start.players.iter().map(|p|
		PortOccupancy {
			port: p.port,
			follower: p.character == ICE_CLIMBERS,
		}
	).collect()
}

fn write_json<W: Write>(game: Game, mut w: W) -> Result<(), Box<dyn Error>> {
	let ports = port_occupancy(&game);
	let frames = game.frames.into_struct_array(game.start.slippi.version, &ports).boxed();
	let mut serializer = json_write::Serializer::new(vec![Ok(frames)].into_iter(), vec![]);

	writeln!(w, "{{\n")?;
	writeln!(w, "\"hash\": {},", serde_json::to_string(&game.hash)?)?;
	writeln!(w, "\"start\": {},", serde_json::to_string(&game.start)?)?;
	if let Some(end) = &game.end {
		writeln!(w, "\"end\": {},", serde_json::to_string(end)?)?;
	}
	if let Some(meta) = &game.metadata {
		writeln!(w, "\"metadata\": {},", serde_json::to_string(meta)?)?;
	}
	writeln!(w, "\"frames\": ")?;
	json_write::write(&mut w, &mut serializer)?;
	writeln!(w, "\n}}")?;
	w.flush()?;
	Ok(())
}

fn write_slippi<W: Write>(game: Game, w: &mut W) -> Result<(), Box<dyn Error>> {
	slippi::write(w, &game)?;
	w.flush()?;
	Ok(())
}

fn write<W: Write>(game: Game, w: &mut W, opts: &Opts) -> Result<(), Box<dyn Error>> {
	use Format::*;
	match opts.output_format {
		Peppi => io_peppi::write(w, game, Some(&io_peppi::ser::Opts {
			compression: opts.compression,
			..Default::default()
		}))?,
		Slippi => write_slippi(game, w)?,
		Json => write_json(game, w)?,
		Null => {},
	}
	Ok(())
}

fn convert(game: Game, opts: &Opts) -> Result<(), Box<dyn Error>> {
	let now = std::time::Instant::now();
	match &opts.outfile {
		None => write(game, &mut io::stdout(), opts)?,
		Some(path) => write(game, &mut BufWriter::new(File::create(path)?), opts)?,
	}
	if opts.output_format != Format::Null {
		info!("Wrote replay in {} μs", now.elapsed().as_micros());
	}
	Ok(())
}

fn hash(f: &mut File) -> Result<String, Box<dyn Error>> {
	let mut hasher = Box::new(Xxh3::new());
	let mut buf = Vec::<u8>::new();
	f.read_to_end(&mut buf)?;
	hasher.update(&buf);
	Ok(peppi::io::format_hash(&hasher))
}

fn verify_peppi(hash_in: String, opts: &Opts) -> Result<(), Box<dyn Error>> {
	let now = std::time::Instant::now();
	let outfile = opts.outfile.as_ref().unwrap();

	let game = read_peppi(
		&mut BufReader::new(
			File::open(&outfile)
				.map_err(|e| format!("couldn't open `{}`: {}", outfile.display(), e))?),
		opts)?;

	let mut buf = BufWriter::new(tempfile::tempfile()?);
	write_slippi(game, &mut buf)?;
	let mut tmpfile = buf.into_inner()?;
	tmpfile.rewind()?;

	let hash_out = hash(&mut tmpfile)?;

	debug!("original hash: {}", hash_in);
	debug!("round-trip hash: {}", hash_out);
	if hash_in == hash_out {
		info!("Verified output in {} μs", now.elapsed().as_micros());
		Ok(())
	} else {
		Err(format!("round-trip verification error (hash: {})", hash_in).into())
	}
}

fn no_verify_reason(opts: &Opts) -> Option<(String, Level)> {
	use Level::*;
	if opts.output_format != Format::Peppi {
		Some(("non-Peppi output".to_string(), Info))
	} else if opts.no_verify {
		Some(("`--no-verify`".to_string(), Info))
	} else if opts.outfile.is_none() {
		Some(("writing to STDOUT".to_string(), Warn))
	} else if opts.short {
		Some(("`--short`".to_string(), Warn))
	} else {
		None
	}
}

fn detect_format<R: Read>(r: &mut BufReader<R>, opts: &Opts) -> Result<Format, Box<dyn Error>> {
	let buf = r.fill_buf()?;
	let format = match opts.input_format {
		Some(format) => format,
		_ => {
			let format = if buf.starts_with(&slippi::FILE_SIGNATURE) {
				Format::Slippi
			} else if buf.starts_with(&io_peppi::FILE_SIGNATURE) {
				Format::Peppi
			} else {
				return Err("unknown file format".into());
			};
			info!("Detected format: {:?}", format);
			format
		}
	};
	Ok(format)
}

fn read_slippi<R: Read + Seek>(mut r: R, opts: &Opts) -> Result<Game, Box<dyn Error>> {
	let game = slippi::read(&mut r,
		Some(&slippi::de::Opts {
			skip_frames: opts.short,
			compute_hash: no_verify_reason(opts).is_none(),
			debug: opts.debug_dir.as_ref().map(|dir|
				slippi::de::Debug {
					dir: dir.clone(),
				},
			),
			..Default::default()
		}),
	)?;
	Ok(game)
}

fn read_peppi<R: Read>(mut r: R, opts: &Opts) -> Result<Game, Box<dyn Error>> {
	Ok(io_peppi::read(&mut r, Some(&io_peppi::de::Opts {
		skip_frames: opts.short,
		..Default::default()
	}))?)
}

fn read_game_<R: Read + Seek>(r: R, opts: &Opts) -> Result<(Game, Format), Box<dyn Error>> {
	let now = std::time::Instant::now();
	let mut buf = BufReader::new(r);
	let format = detect_format(&mut buf, opts)?;
	let game = match format {
		Format::Peppi => read_peppi(buf, opts)?,
		Format::Slippi => read_slippi(buf, opts)?,
		f => return Err(format!("reading from {:?} is not supported", f).into()),
	};
	info!("Parsed replay in {} μs", now.elapsed().as_micros());
	Ok((game, format))
}

struct SkippingReader<R: Read> {
	reader: R,
}

impl<R: Read> SkippingReader<R> {
	fn new(reader: R) -> Self {
		Self { reader }
	}
}

impl<R: Read> Read for SkippingReader<R> {
	fn read(&mut self, size: &mut [u8]) -> Result<usize, io::Error> {
		self.reader.read(size)
	}
}

impl<R: Read> Seek for SkippingReader<R> {
	fn seek(&mut self, pos: SeekFrom) -> Result<u64, io::Error> {
		match pos {
			SeekFrom::Current(offset) if offset >= 0 => {
				io::copy(&mut self.reader.by_ref().take(offset as u64), &mut io::sink())?;
				Ok(0) // we don't have a real position, so just return 0
			},
			_ => unimplemented!(),
		}
	}
}

fn read_game(opts: &Opts) -> Result<(Game, Format), Box<dyn Error>> {
	match &opts.infile {
		None => read_game_(SkippingReader::new(io::stdin()), opts),
		Some(path) => read_game_(
			File::open(path).map_err(|e| format!("couldn't open `{}`: {}", path.display(), e))?,
			opts,
		),
	}
}

fn log_level(verbosity: u8) -> LevelFilter {
	use LevelFilter::*;
	match verbosity {
		0 => Warn,
		1 => Info,
		2 => Debug,
		_ => Trace,
	}
}

fn parse_opts() -> Opts {
	let matches = Command::new("slp")
		.version(env!("CARGO_PKG_VERSION"))
		.author("melkor <hohav@fastmail.com>")
		.about("Inspector for Slippi SSBM replay files")
		.arg(Arg::new("game.slp")
			.help("Replay file to parse (`-` for STDIN)")
			.index(1))
		.arg(Arg::new("input-format")
			.help("Input format")
			.long("input-format")
			.num_args(1)
			.value_parser(clap::builder::PossibleValuesParser::new(["peppi", "slippi"])))
		.arg(Arg::new("outfile")
			.help("Output path")
			.short('o')
			.long("outfile")
			.num_args(1))
		.arg(Arg::new("format")
			.help("Output format")
			.short('f')
			.long("format")
			.num_args(1)
			.value_parser(clap::builder::PossibleValuesParser::new(["json", "null", "peppi", "rust", "slippi"]))
			.default_value("json"))
		.arg(Arg::new("compression")
			.help("Compression method")
			.short('c')
			.long("compression")
			.num_args(1)
			.value_parser(clap::builder::PossibleValuesParser::new(["lz4", "zstd"])))
		.arg(Arg::new("short")
			.help("Don't output frame data")
			.short('s')
			.long("short")
			.action(ArgAction::SetTrue))
		.arg(Arg::new("no-verify")
			.help("Don't verify Peppi output")
			.long("no-verify")
			.action(ArgAction::SetTrue))
		.arg(Arg::new("debug-dir")
			.help("Debug output dir")
			.long("debug-dir")
			.num_args(1))
		.arg(Arg::new("verbose")
			.help("Be more verbose")
			.short('v')
			.long("verbose")
			.action(ArgAction::Count))
		.get_matches();

	Opts {
		debug_dir: matches.get_one::<String>("debug-dir").map(PathBuf::from),
		infile: matches.get_one::<String>("game.slp").map(|s| s.into()),
		input_format: matches.get_one::<String>("input-format").map(|f| (&f[..]).try_into().unwrap()),
		log_level: log_level(*matches.get_one("verbose").unwrap()),
		no_verify: matches.get_flag("no-verify"),
		outfile: matches.get_one::<String>("outfile").map(|s| s.into()),
		output_format: (&matches.get_one::<String>("format").unwrap()[..]).try_into().unwrap(),
		compression: matches.get_one::<String>("compression").map(|c|
			parse_compression(c).unwrap()
		),
		short: matches.get_flag("short"),
	}
}

pub fn _main() -> Result<(), Box<dyn Error>> {
	let mut opts = parse_opts();

	env_logger::builder()
		.filter_level(opts.log_level)
		.format_timestamp(None)
		.format_target(false)
		.init();

	// don't check for "-", to allow the user to force reading from STDIN
	// in case of TTY detection false-positives
	if opts.infile.is_none() && atty::is(atty::Stream::Stdin) {
		return Err("refusing to read from a TTY (`slp -h` for usage)".into());
	}

	if opts.outfile.is_none() && atty::is(atty::Stream::Stdout) &&
			(opts.output_format == Format::Peppi || opts.output_format == Format::Slippi) {
		return Err("refusing to write binary data to a TTY (`slp -h` for usage)".into());
	}

	let (game, actual_format) = read_game(&opts)?;
	match opts.input_format {
		Some(nominal_format) => assert_eq!(nominal_format, actual_format),
		_ => opts.input_format = Some(actual_format),
	}

	let hash = game.hash.clone();
	convert(game, &opts)?;

	if let Some((ref reason, log_level)) = no_verify_reason(&opts) {
		log!(log_level, "Skipping round-trip verification ({})", reason);
	} else {
		verify_peppi(hash.ok_or("missing hash")?, &opts)?;
	}

	Ok(())
}

pub fn main() {
	if let Err(e) = _main() {
		error!("{}", e);
		std::process::exit(2);
	}
}
