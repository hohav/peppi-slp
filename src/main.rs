//TODO: split this file up
#![allow(clippy::redundant_field_names)]

use std::{
	error::Error,
	fs::File,
	io::{self, BufReader, BufWriter, Read, Seek, Write},
	path::{Path, PathBuf},
};

use clap::{Arg, ArgAction, Command};
use log::{debug, error, info, log, Level, LevelFilter};
use peekread::{BufPeekReader, PeekRead};
use serde::{Deserialize, Serialize};
use xxhash_rust::xxh3::Xxh3;

use arrow2::{
	array::Array,
	datatypes::{Field, Metadata, Schema},
	io::ipc::{
		read::{read_stream_metadata, StreamReader, StreamState},
		write::stream_async::WriteOptions,
	},
};

use peppi::{
	serde::{arrow, collect::Rollback},
	model::{
		game::{self, MAX_SUPPORTED_VERSION, Frames, Game},
		metadata,
	},
};

/// Peppi files are TAR archives, and are guaranteed to start with `peppi.json`
pub const PEPPI_FILE_SIGNATURE: [u8; 10] =
	[0x70, 0x65, 0x70, 0x70, 0x69, 0x2e, 0x6a, 0x73, 0x6f, 0x6e];

type JsMap = serde_json::Map<String, serde_json::Value>;

#[derive(Clone, Copy, Debug, PartialEq)]
enum Format {
	Json, Peppi, Rust, Slippi, Null
}

impl TryFrom<&str> for Format {
	type Error = String;
	fn try_from(s: &str) -> Result<Self, Self::Error> {
		match s {
			"json" => Ok(Format::Json),
			"peppi" => Ok(Format::Peppi),
			"rust" => Ok(Format::Rust),
			"slippi" => Ok(Format::Slippi),
			"null" => Ok(Format::Null),
			s => Err(format!("invalid Format: {}", s)),
		}
	}
}

struct Opts {
	debug_dir: Option<PathBuf>,
	enum_names: bool,
	infile: Option<PathBuf>,
	input_format: Option<Format>,
	log_level: LevelFilter,
	no_verify: bool,
	outfile: Option<PathBuf>,
	output_format: Format,
	rollback: Rollback,
	short: bool,
}

fn tar_append<W: Write, P: AsRef<Path>>(builder: &mut tar::Builder<W>, buf: &[u8], path: P) -> Result<(), Box<dyn Error>> {
	let mut header = tar::Header::new_gnu();
	header.set_size(buf.len().try_into()?);
	header.set_path(path)?;
	header.set_mode(0o644);
	header.set_cksum();
	builder.append(&header, buf)?;
	Ok(())
}

#[derive(Debug, Deserialize, Serialize)]
struct Version(u8, u8, u8);

#[derive(Debug, Deserialize, Serialize)]
struct Peppi {
	version: Version,
	#[serde(skip_serializing_if = "Option::is_none")]
	slp_hash: Option<String>,
}

fn write_peppi<W: Write>(game: &Game, w: W, slp_hash: Option<String>) -> Result<(), Box<dyn Error>> {
	if game.start.slippi.version > MAX_SUPPORTED_VERSION {
		return Err(
			format!("Unsupported Slippi version ({} > {})",
				game.start.slippi.version, MAX_SUPPORTED_VERSION).into());
	}

	let peppi = Peppi {
		version: Version(0, 3, 0),
		slp_hash: slp_hash,
	};

	let mut tar = tar::Builder::new(w);
	tar_append(&mut tar, &serde_json::to_vec(&peppi)?, "peppi.json")?;
	tar_append(&mut tar, &serde_json::to_vec(&game.metadata_raw)?, "metadata.json")?;
	tar_append(&mut tar, &serde_json::to_vec(&game.start)?, "start.json")?;
	tar_append(&mut tar, &serde_json::to_vec(&game.end)?, "end.json")?;
	tar_append(&mut tar, &game.start.raw_bytes, "start.raw")?;
	tar_append(&mut tar, &game.end.raw_bytes, "end.raw")?;

	if let Some(gecko_codes) = &game.gecko_codes {
		let mut buf = gecko_codes.actual_size.to_le_bytes().to_vec();
		buf.write_all(&gecko_codes.bytes)?;
		tar_append(&mut tar, &buf, "gecko_codes.raw")?;
	}

	if game.frames.frame_count() > 0 {
		let opts = Some(arrow::Opts { avro_compatible: true });
		let write_opts = WriteOptions {
			compression: None, //Some(arrow2::io::ipc::write::Compression::LZ4),
		};
		let batch = arrow::frames_to_arrow(game, opts);
		let schema = Schema::from(
			vec![Field {
				name: "frame".to_string(),
				data_type: batch.data_type().clone(),
				is_nullable: false,
				metadata: Metadata::default(),
			}]
		);

		let chunk = arrow2::chunk::Chunk::new(vec![
			Box::new(batch) as Box<dyn arrow2::array::Array>]);
		let mut buf = Vec::new();
		let mut writer = arrow2::io::ipc::write::FileWriter::try_new(&mut buf, &schema, None, write_opts)?;
		writer.write(&chunk, None)?;
		writer.finish()?;
		tar_append(&mut tar, &buf, "frames.arrow")?;
	}

	tar.into_inner()?.flush()?;
	Ok(())
}

fn write_json<W: Write>(game: &Game, mut w: W) -> Result<(), Box<dyn Error>> {
	writeln!(w, "{}", serde_json::to_string(game)?)?;
	w.flush()?;
	Ok(())
}

fn write_rust<W: Write>(game: &Game, mut w: W) -> io::Result<()> {
	writeln!(w, "{:#?}", game)?;
	w.flush()?;
	Ok(())
}

fn write_slippi<W: Write>(game: &Game, mut w: W) -> Result<(), Box<dyn Error>> {
	peppi::serde::ser::serialize(&mut w, game)?;
	w.flush()?;
	Ok(())
}

fn write<W: Write>(game: &Game, w: &mut W, format: Format, slp_hash: Option<String>) -> Result<(), Box<dyn Error>> {
	use Format::*;
	match format {
		Null => {},
		Peppi => write_peppi(game, w, slp_hash)?,
		Slippi => write_slippi(game, w)?,
		Json => write_json(game, w)?,
		Rust => write_rust(game, w)?,
	}
	Ok(())
}

fn convert(game: &Game, opts: &Opts, slp_hash: Option<String>) -> Result<(), Box<dyn Error>> {
	let now = std::time::Instant::now();
	match &opts.outfile {
		None => write(game, &mut io::stdout(), opts.output_format, slp_hash)?,
		Some(path) => write(game, &mut BufWriter::new(File::create(path)?), opts.output_format, slp_hash)?,
	}
	if opts.output_format != Format::Null {
		info!("Wrote replay in {} μs", now.elapsed().as_micros());
	}
	Ok(())
}

/// Hashing

fn format_hash(hasher: &Xxh3) -> String {
	format!("xxh3:{:08x}", &hasher.digest())
}

pub struct HashReader<R> {
	reader: R,
	hasher: Option<Box<Xxh3>>,
}

impl<R> HashReader<R> {
	fn new(reader: R, enabled: bool) -> Self {
		HashReader {
			reader,
			hasher: match enabled {
				true => Some(Box::new(Xxh3::new())),
				_ => None,
			},
		}
	}

	fn into_digest(self) -> Option<String> {
		self.hasher.map(|h| format_hash(&h))
	}
}

impl<R: Read> Read for HashReader<R> {
	fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
		let n = self.reader.read(buf)?;
		if let Some(h) = &mut self.hasher {
			h.update(&buf[..n])
		}
		Ok(n)
	}
}

fn hash(f: &mut File) -> Result<String, Box<dyn Error>> {
	let mut hasher = Box::new(Xxh3::new());
	let mut buf = Vec::<u8>::new();
	f.read_to_end(&mut buf)?;
	hasher.update(&buf);
	Ok(format_hash(&hasher))
}

fn verify_peppi(slp_hash: String, opts: &Opts) -> Result<(), Box<dyn Error>> {
	let now = std::time::Instant::now();
	let outfile = opts.outfile.as_ref().unwrap();

	let (game, _) = read_peppi(
		BufReader::new(
			File::open(&outfile)
				.map_err(|e| format!("couldn't open `{}`: {}", outfile.display(), e))?),
		opts)?;

	let mut buf = BufWriter::new(tempfile::tempfile()?);
	write_slippi(&game, &mut buf)?;
	let mut tmpfile = buf.into_inner()?;
	tmpfile.rewind()?;

	let new_hash = hash(&mut tmpfile)?;

	debug!("original hash: {}", slp_hash);
	debug!("round-trip hash: {}", new_hash);
	if slp_hash == new_hash {
		info!("Verified output in {} μs", now.elapsed().as_micros());
		Ok(())
	} else {
		Err("round-trip verification error".into())
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
	} else if opts.rollback != Rollback::All {
		Some((format!("`--rollback={}`", <&str>::from(opts.rollback)), Warn))
	} else {
		None
	}
}

fn detect_format<R: Read>(buf: &mut BufPeekReader<R>, opts: &Opts) -> Result<Format, Box<dyn Error>> {
	let format = match opts.input_format {
		Some(format) => format,
		_ => {
			let format = if buf.starts_with(&peppi::SLIPPI_FILE_SIGNATURE)? {
				Format::Slippi
			} else if buf.starts_with(&PEPPI_FILE_SIGNATURE)? {
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

fn read_slippi<R: Read>(buf: R, opts: &Opts) -> Result<(Game, Option<String>), Box<dyn Error>> {
	let should_verify = match no_verify_reason(opts) {
		Some((ref reason, log_level)) => {
			log!(log_level, "Skipping round-trip verification ({})", reason);
			false
		}
		_ => true,
	};
	let mut buf = HashReader::new(buf, should_verify);

	let game = peppi::game(&mut buf,
		Some(&peppi::serde::de::Opts {
			skip_frames: opts.short,
			debug_dir: opts.debug_dir.clone(),
		}),
		Some(&peppi::serde::collect::Opts {
			rollback: opts.rollback,
		}),
	)?;
	Ok((game, buf.into_digest()))
}

fn read_arrow_frames<R: Read>(mut r: R) -> Result<Frames, Box<dyn Error>> {
	r.read_exact(&mut [0; 8])?; // skip the magic number `ARROW1\0\0`
	let metadata = read_stream_metadata(&mut r)?;
	let reader = StreamReader::new(r, metadata, None);
	let mut frames: Option<Frames> = None;
	for result in reader {
		match result? {
			StreamState::Some(batch) => if frames.is_none() {
				frames = Some(arrow::frames_from_arrow(batch.arrays()[0].as_ref()));
			} else {
				return Err("multiple batches".into());
			}
			StreamState::Waiting => std::thread::sleep(std::time::Duration::from_millis(1000)),
		}
	}
	match frames {
		Some(f) => Ok(f),
		_ => Err("no batches".into()),
	}
}

fn read_peppi_start<R: Read>(mut r: R) -> Result<game::Start, Box<dyn Error>> {
	let mut buf = Vec::new();
	r.read_to_end(&mut buf)?;
	Ok(game::Start::from_bytes(buf.as_slice())?)
}

fn read_peppi_end<R: Read>(mut r: R) -> Result<game::End, Box<dyn Error>> {
	let mut buf = Vec::new();
	r.read_to_end(&mut buf)?;
	Ok(game::End::from_bytes(buf.as_slice())?)
}

fn read_peppi_metadata<R: Read>(r: R) -> Result<JsMap, Box<dyn Error>> {
	let json_object: serde_json::Value = serde_json::from_reader(r)?;
	match json_object {
		serde_json::Value::Object(map) => Ok(map),
		obj => Err(format!("expected map, got: {:?}", obj).into()),
	}
}

fn read_peppi_gecko_codes<R: Read>(mut r: R) -> Result<game::GeckoCodes, Box<dyn Error>> {
	let mut actual_size = [0; 2];
	r.read_exact(&mut actual_size)?;
	let mut bytes = Vec::new();
	r.read_to_end(&mut bytes)?;
	Ok(game::GeckoCodes {
		actual_size: u16::from_le_bytes(actual_size),
		bytes: bytes,
	})
}

fn read_peppi<R: Read>(r: R, opts: &Opts) -> Result<(Game, Option<String>), Box<dyn Error>> {
	let mut start: Option<game::Start> = None;
	let mut end: Option<game::End> = None;
	let mut metadata_raw: Option<JsMap> = None;
	let mut gecko_codes: Option<game::GeckoCodes> = None;
	let mut frames: Option<Frames> = None;
	let mut peppi: Option<Peppi> = None;
	for entry in tar::Archive::new(r).entries()? {
		let file = entry?;
		let path = file.path()?;
		debug!("processing file: {}", path.display());
		match path.file_name().and_then(|n| n.to_str()) {
			Some("peppi.json") =>
				peppi = serde_json::from_reader(file)?,
			Some("start.raw") =>
				start = Some(read_peppi_start(file)?),
			Some("end.raw") =>
				end = Some(read_peppi_end(file)?),
			Some("metadata.json") =>
				metadata_raw = Some(read_peppi_metadata(file)?),
			Some("gecko_codes.raw") =>
				gecko_codes = Some(read_peppi_gecko_codes(file)?),
			Some("frames.arrow") => {
				frames = Some(match opts.short {
					true => Frames::P2(Vec::new()),
					_ => read_arrow_frames(file)?,
				});
				if opts.short {
					break;
				}
			},
			_ => debug!("=> skipping"),
		};
	};

	let metadata_raw = metadata_raw.ok_or("missing metadata")?;
	let metadata = metadata::Metadata::parse(&metadata_raw)?;

	let game = Game {
		metadata_raw: metadata_raw,
		metadata: metadata,
		start: start.ok_or("missing start")?,
		end: end.ok_or("missing end")?,
		gecko_codes: gecko_codes,
		frames: frames.ok_or("missing frames")?,
	};
	Ok((game, peppi.and_then(|p| p.slp_hash)))
}

fn read_game_<R: Read>(buf: R, format: Format, opts: &Opts) -> Result<(Game, Format, Option<String>), Box<dyn Error>> {
	match format {
		Format::Peppi => {
			let (game, hash) = read_peppi(buf, opts)?;
			Ok((game, format, hash))
		},
		Format::Slippi => {
			let (game, hash)= read_slippi(buf, opts)?;
			Ok((game, format, hash))
		},
		f => Err(format!("reading from {:?} is not supported", f).into()),
	}
}

fn read_game(opts: &Opts) -> Result<(Game, Format, Option<String>), Box<dyn Error>> {
	let now = std::time::Instant::now();
	let result = match &opts.infile {
		None => {
			let mut buf = BufPeekReader::new(io::stdin());
			let format = detect_format(&mut buf, opts)?;
			read_game_(buf, format, opts)?
		},
		Some(path) => {
			let mut buf = BufPeekReader::new(
				File::open(path)
					.map_err(|e| format!("couldn't open `{}`: {}", path.display(), e))?);
			buf.set_min_read_size(8192);
			let format = detect_format(&mut buf, opts)?;
			read_game_(buf, format, opts)?
		},
	};
	info!("Parsed replay in {} μs", now.elapsed().as_micros());
	Ok(result)
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
		.version(clap::crate_version!())
		.author("melkor <hohav@fastmail.com>")
		.about("Inspector for Slippi SSBM replay files")
		.arg(Arg::new("game.slp")
			.help("Replay file to parse (`-` for STDIN)")
			.index(1))
		.arg(Arg::new("input-format")
			.help("Input format")
			.long("input-format")
			.number_of_values(1)
			.value_parser(clap::builder::PossibleValuesParser::new(["peppi", "slippi"])))
		.arg(Arg::new("outfile")
			.help("Output path")
			.short('o')
			.long("outfile")
			.number_of_values(1))
		.arg(Arg::new("format")
			.help("Output format")
			.short('f')
			.long("format")
			.number_of_values(1)
			.value_parser(clap::builder::PossibleValuesParser::new(["json", "null", "peppi", "rust", "slippi"]))
			.default_value("json"))
		.arg(Arg::new("named-constants")
			.help("Show names for known constants (action states, etc)")
			.short('n')
			.long("named-constants")
			.takes_value(false))
		.arg(Arg::new("short")
			.help("Don't output frame data")
			.short('s')
			.long("short")
			.takes_value(false))
		.arg(Arg::new("no-verify")
			.help("Don't verify Peppi output")
			.long("no-verify")
			.takes_value(false))
		.arg(Arg::new("rollback")
			.help("Rollback frames to keep")
			.short('r')
			.long("rollback")
			.number_of_values(1)
			.value_parser(clap::builder::PossibleValuesParser::new(["all", "first", "last"]))
			.default_value("all"))
		.arg(Arg::new("debug-dir")
			.help("Debug output dir")
			.long("debug-dir")
			.number_of_values(1))
		.arg(Arg::new("verbose")
			.help("Be more verbose")
			.short('v')
			.long("verbose")
			.action(ArgAction::Count))
		.get_matches();

	Opts {
		debug_dir: matches.get_one::<String>("debug-dir").map(PathBuf::from),
		enum_names: matches.is_present("named-constants"),
		infile: matches.get_one::<String>("game.slp").map(|s| s.into()),
		input_format: matches.get_one::<String>("input-format").map(|f| (&f[..]).try_into().unwrap()),
		log_level: log_level(*matches.get_one("verbose").unwrap()),
		no_verify: matches.is_present("no-verify"),
		outfile: matches.get_one::<String>("outfile").map(|s| s.into()),
		output_format: (&matches.get_one::<String>("format").unwrap()[..]).try_into().unwrap(),
		rollback: (&matches.get_one::<String>("rollback").unwrap()[..]).try_into().unwrap(),
		short: matches.is_present("short"),
	}
}

pub fn _main() -> Result<(), Box<dyn Error>> {
	let mut opts = parse_opts();

	env_logger::builder()
		.filter_level(opts.log_level)
		.format_timestamp(None)
		.format_target(false)
		.init();

	unsafe {
		peppi::SERIALIZATION_CONFIG = peppi::SerializationConfig {
			enum_names: opts.enum_names,
		}
	};

	// don't check for "-", to allow the user to force reading from STDIN
	// in case of TTY detection false-positives
	if opts.infile.is_none() && atty::is(atty::Stream::Stdin) {
		return Err("refusing to read from a TTY (`slp -h` for usage)".into());
	}

	if opts.outfile.is_none() && atty::is(atty::Stream::Stdout) &&
			(opts.output_format == Format::Peppi || opts.output_format == Format::Slippi) {
		return Err("refusing to write binary data to a TTY (`slp -h` for usage)".into());
	}

	let (game, actual_format, hash) = read_game(&opts)?;
	match opts.input_format {
		Some(nominal_format) => assert_eq!(nominal_format, actual_format),
		_ => opts.input_format = Some(actual_format),
	}

	convert(&game, &opts, hash.clone())?;
	if let Some(hash) = hash {
		verify_peppi(hash, &opts)?;
	}

	Ok(())
}

pub fn main() {
	if let Err(e) = _main() {
		error!("{}", e);
		std::process::exit(2);
	}
}
