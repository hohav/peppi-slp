use std::{
    error::Error,
    fs::{self, File},
    io::{self, Read, Write},
    path,
    sync::Arc,
};

use ::arrow::{
    array::{Array, StructArray},
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use clap::{App, Arg};
use log::{error, warn};
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, Encoding},
    file::properties::{WriterProperties, WriterVersion},
};

use peppi::{
    model::game::{Game, MAX_SUPPORTED_VERSION},
    serde::arrow,
};

#[derive(Clone, Copy)]
enum Format {
    Json,
    Peppi,
    Rust,
    Slippi,
}

struct Opts {
    infile: String,
    outfile: String,
    format: Format,
    short: bool,
    rollbacks: bool,
    enum_names: bool,
}

/// Work around bugs in ArrowWriter's support for Lists by removing items
/// (to be written separately).
fn remove_items(frames: StructArray) -> Result<RecordBatch, Box<dyn Error>> {
    match frames.data().data_type() {
        DataType::Struct(fields) => {
            let mut filtered_fields = vec![];
            let mut filtered_columns = vec![];
            for (idx, f) in fields.iter().enumerate() {
                if f.name() != "items" {
                    filtered_fields.push(f.clone());
                    filtered_columns.push(frames.column(idx).clone());
                }
            }
            Ok(RecordBatch::try_new(
                Arc::new(Schema::new(filtered_fields)),
                filtered_columns,
            )?)
        }
        _ => unreachable!(),
    }
}

fn write_peppi<P: AsRef<path::Path>>(
    game: &Game,
    dir: P,
    short: bool,
) -> Result<(), Box<dyn Error>> {
    warn!("Peppi format is experimental!");

    if game.start.slippi.version > MAX_SUPPORTED_VERSION {
        warn!(
            "unsupported Slippi version ({} > {}). Unknown fields will be omitted from output!",
            game.start.slippi.version, MAX_SUPPORTED_VERSION
        );
    }

    let dir = dir.as_ref();
    fs::create_dir_all(dir)?;
    fs::write(
        dir.join("metadata.json"),
        serde_json::to_string(&game.metadata_raw)?,
    )?;
    fs::write(dir.join("start.json"), serde_json::to_string(&game.start)?)?;
    fs::write(dir.join("end.json"), serde_json::to_string(&game.end)?)?;

    if !short {
        let opts = Some(arrow::Opts {
            avro_compatible: true,
        });
        let props = WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .set_dictionary_enabled(false)
            .set_encoding(Encoding::PLAIN)
            .set_compression(Compression::UNCOMPRESSED)
            .build();

        // write items separately (workaround for buggy/missing ListArray support in Parquet)
        if let Some(items) = arrow::items_to_arrow(game, opts) {
            let batch = RecordBatch::from(&items);
            let buf = File::create(dir.join("items.parquet"))?;
            let mut writer = ArrowWriter::try_new(buf, batch.schema(), Some(props.clone()))?;
            writer.write(&batch)?;
            writer.close()?;
        }

        // write the frame data
        let batch = remove_items(arrow::frames_to_arrow(game, opts))?;
        let buf = File::create(dir.join("frames.parquet"))?;
        let mut writer = ArrowWriter::try_new(buf, batch.schema(), Some(props))?;
        writer.write(&batch)?;
        writer.close()?;
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

fn write_slippi<P: AsRef<path::Path>>(game: &Game, path: P) -> Result<(), Box<dyn Error>> {
    peppi::serde::ser::serialize(&mut File::create(path)?, game)?;
    Ok(())
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

fn inspect<R: Read>(mut buf: R, opts: &Opts) -> Result<(), Box<dyn Error>> {
    let game = peppi::game(
        &mut buf,
        Some(peppi::serde::de::Opts {
            skip_frames: opts.short,
        }),
        Some(peppi::serde::collect::Opts {
            rollbacks: opts.rollbacks,
        }),
    )?;
    use Format::*;
    match (opts.format, opts.outfile.as_str()) {
        (Peppi, "-") => Err("cannot output Peppi to STDOUT")?,
        (Peppi, o) => write_peppi(&game, o, opts.short),
        (Slippi, "-") => Err("cannot output Slippi to STDOUT")?,
        (Slippi, o) => write_slippi(&game, o),
        (format, "-") => write(&game, io::stdout(), format),
        (format, s) => write(&game, File::create(s)?, format),
    }
}

fn parse_opts() -> Opts {
    let matches = App::new("slp")
        .version("0.2.1")
        .author("melkor <hohav@fastmail.com>")
        .about("Inspector for Slippi SSBM replay files")
        .arg(
            Arg::with_name("outfile")
                .help("Output path")
                .short("o")
                .default_value("-"),
        )
        .arg(
            Arg::with_name("format")
                .help("Output format")
                .short("f")
                .possible_values(&["json", "slippi", "rust", "peppi"])
                .default_value("json"),
        )
        .arg(
            Arg::with_name("names")
                .help("Append names for known constants")
                .short("n")
                .long("names"),
        )
        .arg(
            Arg::with_name("short")
                .help("Don't output frame data")
                .short("s")
                .long("short"),
        )
        .arg(
            Arg::with_name("rollbacks")
                .help("Include rollback frames")
                .short("r")
                .long("rollbacks"),
        )
        .arg(
            Arg::with_name("game.slp")
                .help("Replay file to parse (`-` for STDIN)")
                .index(1),
        )
        .get_matches();

    let infile = matches.value_of("game.slp").unwrap_or("");
    let outfile = matches.value_of("outfile").unwrap();

    let format = {
        use Format::*;
        match matches.value_of("format").unwrap() {
            "json" => Json,
            "peppi" => Peppi,
            "rust" => Rust,
            "slippi" => Slippi,
            _ => unimplemented!(),
        }
    };

    Opts {
        infile: infile.to_string(),
        outfile: outfile.to_string(),
        format,
        short: matches.is_present("short"),
        rollbacks: matches.is_present("rollbacks"),
        enum_names: matches.is_present("names"),
    }
}

pub fn _main() -> Result<(), Box<dyn Error>> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn"))
        .format_timestamp(None)
        .format_target(false)
        .init();

    let opts = parse_opts();
    unsafe {
        peppi::SERIALIZATION_CONFIG = peppi::SerializationConfig {
            enum_names: opts.enum_names,
        }
    };

    if opts.infile.is_empty() && atty::is(atty::Stream::Stdin) {
        return Err("refusing to read from a TTY (`slp -h` for usage)".into());
    }

    match opts.infile.as_str() {
        "-" | "" => inspect(io::stdin(), &opts),
        path => {
            let file = File::open(path).map_err(|e| format!("couldn't open `{}`: {}", path, e))?;
            inspect(io::BufReader::new(file), &opts)
        }
    }
}

pub fn main() {
    if let Err(e) = _main() {
        error!("{}", e);
        std::process::exit(2);
    }
}
