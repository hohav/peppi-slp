use std::{fs, io};
use std::error::Error;

use clap::{App, Arg};
use jmespatch::ToJmespath;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

enum Format {
	Rust, Json
}

struct Opts {
	path: String,
	format: Format,
	frames: bool,
	enum_names: bool,
	query: Option<String>,
}

fn inspect<R: io::Read>(buf: &mut R, format: Format, query: Option<String>) -> Result<(), Box<dyn Error>> {
	let game = peppi::game(buf)?;
	if let Some(query) = &query {
		let query = jmespatch::compile(query)?;
		let jmes = game.to_jmespath()?;
		let result = query.search(jmes)?;
		println!("{}", serde_json::to_string(&result)?);
	} else {
		use Format::*;
		match format {
			Json => println!("{}", serde_json::to_string(&game)?),
			Rust => println!("{:#?}", game),
		};
	}
	Ok(())
}

fn parse_opts() -> Result<Opts, String> {
	let matches = App::new("slp")
		.version("0.1")
		.author("melkor <hohav@fastmail.com>")
		.about("Inspector for Slippi SSBM replay files")
		.arg(Arg::with_name("format")
			 .help("Output format")
			 .short("o")
			 .possible_values(&["json", "rust"])
			 .default_value("rust"))
		.arg(Arg::with_name("json")
			.help("Output as JSON (same as `-o json`)")
			.short("j")
			.long("json"))
		.arg(Arg::with_name("frames")
			.help("Output frame data")
			.short("f")
			.long("frames"))
		.arg(Arg::with_name("QUERY")
			.help("Print a subset of parsed data (JMESPath syntax; implies `-j`)")
			.short("q")
			.long("query")
			.takes_value(true))
		.arg(Arg::with_name("names")
			.help("Append names for known constants")
			.short("n")
			.long("names"))
		.arg(Arg::with_name("FILE")
			.help("Replay file to parse (`-` for STDIN)")
			.index(1))
		.get_matches();

	let path = matches.value_of("FILE").unwrap_or("-");
	let format = if matches.is_present("json") {
		Format::Json
	} else {
		use Format::*;
		match matches.value_of("format").unwrap() {
			"json" => Json,
			"rust" => Rust,
			o => Err(format!("unsupported output format: {}", o))?,
		}
	};

	Ok(Opts {
		path: path.to_string(),
		format: format,
		frames: matches.is_present("frames") || matches.is_present("QUERY"),
		enum_names: matches.is_present("names"),
		query: matches.value_of("QUERY").map(|q| q.to_string()),
	})
}

pub fn main() -> Result<(), Box<dyn Error>> {
	pretty_env_logger::init();

	let opts = parse_opts()?;
	unsafe {
		peppi::CONFIG = peppi::Config {
			frames: opts.frames,
			enum_names: opts.enum_names,
		}
	};

	if opts.path == "-" {
		inspect(&mut io::stdin(), opts.format, opts.query)
	} else {
		let mut buf = io::BufReader::new(fs::File::open(opts.path)?);
		inspect(&mut buf, opts.format, opts.query)
	}
}
