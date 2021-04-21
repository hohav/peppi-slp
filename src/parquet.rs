use std::{
	error::Error,
	fs::File,
	path::Path,
	sync::Arc,
};

use parquet::{
	basic::{
		Compression,
		Encoding,
	},
	column::writer::{get_typed_column_writer_mut},
	data_type::{BoolType, FloatType, Int32Type, Int64Type},
	file::{
		properties::WriterProperties,
		writer::{FileWriter, RowGroupWriter, SerializedFileWriter},
	},
	schema::{
		parser::parse_message_type,
	},
};

use peppi::{
	game::FIRST_FRAME_INDEX,
	frame::{PreCol, PostCol, ItemCol},
	primitives::Direction,
};

use super::transform;

const SCHEMA_FRAME_PRE: &str = "
required group position {
	required float x;
	required float y;
}
required boolean direction;
required group joystick {
	required float x;
	required float y;
}
required group cstick {
	required float x;
	required float y;
}
required group triggers {
	required group physical {
		required float l;
		required float r;
	}
	required float logical;
}
required int32 random_seed (UINT_32);
required group buttons {
	required int32 physical (UINT_16);
	required int32 logical (UINT_32);
}
required int32 state (UINT_16);
";

const SCHEMA_FRAME_PRE_V1_2: &str = "
required int32 raw_analog_x (UINT_8);
";

const SCHEMA_FRAME_PRE_V1_4: &str = "
required float damage;
";

const SCHEMA_FRAME_POST: &str = "
required group position {
	required float x;
	required float y;
}
required boolean direction;
required float damage;
required float shield;
required int32 state (UINT_16);
required int32 character (UINT_8);
required int32 last_attack_landed (UINT_8);
required int32 combo_count (UINT_8);
required int32 last_hit_by (UINT_8);
required int32 stocks (UINT_8);
";

const SCHEMA_FRAME_POST_V0_2: &str = "
required float state_age;
";

const SCHEMA_FRAME_POST_V2_0: &str = "
required int64 flags (UINT_64);
required float misc_as;
required boolean airborne;
required int32 ground (UINT_16);
required int32 jumps (UINT_8);
required int32 l_cancel (UINT_8);
";

const SCHEMA_FRAME_POST_V2_1: &str = "
required int32 hurtbox_state (UINT_8);
";

const SCHEMA_FRAME_POST_V3_5: &str = "
required group velocities {
	required group autogenous {
		required float x;
		required float y;
	}
	required group knockback {
		required float x;
		required float y;
	}
}
";

const SCHEMA_FRAME_POST_V3_8: &str = "
required float hitlag;
";

const SCHEMA_ITEM: &str = "
required int32 index;
required int32 id (UINT_32);
required int32 type (UINT_16);
required int32 state (UINT_8);
required boolean direction;
required group position {
	required float x;
	required float y;
}
required group velocity {
	required float x;
	required float y;
}
required int32 damage (UINT_16);
required float timer;
";

const SCHEMA_ITEM_V3_2: &str = "
required int32 misc (UINT_32);
";

const SCHEMA_ITEM_V3_6: &str = "
required int32 owner (UINT_8);
";

fn write_bool(rgw: &mut Box<dyn RowGroupWriter>, data: &[bool]) -> Result<(), Box<dyn Error>> {
	let mut c = rgw.next_column()?.ok_or("no column")?;
	let w = get_typed_column_writer_mut::<BoolType>(&mut c);
	w.write_batch(data, None, None)?;
	rgw.close_column(c)?;
	Ok(())
}

fn write_i32(rgw: &mut Box<dyn RowGroupWriter>, data: &[i32]) -> Result<(), Box<dyn Error>> {
	let mut c = rgw.next_column()?.ok_or("no column")?;
	let w = get_typed_column_writer_mut::<Int32Type>(&mut c);
	w.write_batch(data, None, None)?;
	rgw.close_column(c)?;
	Ok(())
}

fn write_i64(rgw: &mut Box<dyn RowGroupWriter>, data: &[i64]) -> Result<(), Box<dyn Error>> {
	let mut c = rgw.next_column()?.ok_or("no column")?;
	let w = get_typed_column_writer_mut::<Int64Type>(&mut c);
	w.write_batch(data, None, None)?;
	rgw.close_column(c)?;
	Ok(())
}

fn write_f32(rgw: &mut Box<dyn RowGroupWriter>, data: &[f32]) -> Result<(), Box<dyn Error>> {
	let mut c = rgw.next_column()?.ok_or("no column")?;
	let w = get_typed_column_writer_mut::<FloatType>(&mut c);
	w.write_batch(data, None, None)?;
	rgw.close_column(c)?;
	Ok(())
}

fn write_pre(rgw: &mut Box<dyn RowGroupWriter>, pre: &PreCol, p: usize) -> Result<(), Box<dyn Error>> {
	write_f32(rgw, &pre.position[p].iter().map(|p| p.x).collect::<Vec<_>>())?;
	write_f32(rgw, &pre.position[p].iter().map(|p| p.y).collect::<Vec<_>>())?;
	write_bool(rgw, &pre.direction[p].iter().map(|d| *d == Direction::Right).collect::<Vec<_>>())?;
	write_f32(rgw, &pre.joystick[p].iter().map(|p| p.x).collect::<Vec<_>>())?;
	write_f32(rgw, &pre.joystick[p].iter().map(|p| p.y).collect::<Vec<_>>())?;
	write_f32(rgw, &pre.cstick[p].iter().map(|p| p.x).collect::<Vec<_>>())?;
	write_f32(rgw, &pre.cstick[p].iter().map(|p| p.y).collect::<Vec<_>>())?;
	write_f32(rgw, &pre.triggers[p].iter().map(|t| t.logical).collect::<Vec<_>>())?;
	write_f32(rgw, &pre.triggers[p].iter().map(|t| t.physical.l).collect::<Vec<_>>())?;
	write_f32(rgw, &pre.triggers[p].iter().map(|t| t.physical.r).collect::<Vec<_>>())?;
	write_i32(rgw, &pre.random_seed[p].iter().map(|r| *r as i32).collect::<Vec<_>>())?;
	write_i32(rgw, &pre.buttons[p].iter().map(|b| b.logical.0 as i32).collect::<Vec<_>>())?;
	write_i32(rgw, &pre.buttons[p].iter().map(|b| b.physical.0 as i32).collect::<Vec<_>>())?;
	write_i32(rgw, &pre.state[p].iter().map(|s| u16::from(*s) as i32).collect::<Vec<_>>())?;

	// v1.2
	if let Some(raw_analog_x) = &pre.raw_analog_x {
		write_i32(rgw, &raw_analog_x[p].iter().map(|r| *r as i32).collect::<Vec<_>>())?;
		// v1.4
		if let Some(damage) = &pre.damage {
			write_f32(rgw, &damage[p])?;
		}
	}

	Ok(())
}

fn write_post(rgw: &mut Box<dyn RowGroupWriter>, post: &PostCol, p: usize) -> Result<(), Box<dyn Error>> {
	write_f32(rgw, &post.position[p].iter().map(|p| p.x).collect::<Vec<_>>())?;
	write_f32(rgw, &post.position[p].iter().map(|p| p.y).collect::<Vec<_>>())?;
	write_bool(rgw, &post.direction[p].iter().map(|d| *d == Direction::Right).collect::<Vec<_>>())?;
	write_f32(rgw, &post.damage[p])?;
	write_f32(rgw, &post.shield[p])?;
	write_i32(rgw, &post.state[p].iter().map(|s| u16::from(*s) as i32).collect::<Vec<_>>())?;
	write_i32(rgw, &post.character[p].iter().map(|c| c.0 as i32).collect::<Vec<_>>())?;
	write_i32(rgw, &post.last_attack_landed[p].iter().map(|l| l.map(|l| l.0 as i32).unwrap_or(0)).collect::<Vec<_>>())?;
	write_i32(rgw, &post.combo_count[p].iter().map(|c| *c as i32).collect::<Vec<_>>())?;
	write_i32(rgw, &post.last_hit_by[p].iter().map(|l| l.map(|l| l as i32).unwrap_or(u8::MAX as i32)).collect::<Vec<_>>())?;
	write_i32(rgw, &post.stocks[p].iter().map(|s| *s as i32).collect::<Vec<_>>())?;

	// v0.2
	if let Some(state_age) = &post.state_age {
		write_f32(rgw, &state_age[p])?;
		// v2.0
		if let Some(flags) = &post.flags {
			write_i64(rgw, &flags[p].iter().map(|f| f.0 as i64).collect::<Vec<_>>())?;
			write_f32(rgw, &post.misc_as.as_ref().unwrap()[p])?;
			write_bool(rgw, &post.airborne.as_ref().unwrap()[p])?;
			write_i32(rgw, &post.ground.as_ref().unwrap()[p].iter().map(|g| *g as i32).collect::<Vec<_>>())?;
			write_i32(rgw, &post.jumps.as_ref().unwrap()[p].iter().map(|j| *j as i32).collect::<Vec<_>>())?;
			write_i32(rgw, &post.l_cancel.as_ref().unwrap()[p].iter().map(|l| match l {
				None => 0,
				Some(true) => 1,
				Some(false) => 2,
			}).collect::<Vec<_>>())?;
			// v2.1
			if let Some(hurtbox_state) = &post.hurtbox_state {
				write_i32(rgw, &hurtbox_state[p].iter().map(|h| h.0 as i32).collect::<Vec<_>>())?;
				// v3.5
				if let Some(velocities) = &post.velocities {
					write_f32(rgw, &velocities[p].iter().map(|v| v.autogenous.x).collect::<Vec<_>>())?;
					write_f32(rgw, &velocities[p].iter().map(|v| v.autogenous.y).collect::<Vec<_>>())?;
					write_f32(rgw, &velocities[p].iter().map(|v| v.knockback.x).collect::<Vec<_>>())?;
					write_f32(rgw, &velocities[p].iter().map(|v| v.knockback.y).collect::<Vec<_>>())?;
					// v3.8
					if let Some(hitlag) = &post.hitlag {
						write_f32(rgw, &hitlag[p])?;
					}
				}
			}
		}
	}

	Ok(())
}

fn write_item(rgw: &mut Box<dyn RowGroupWriter>, item: &ItemCol) -> Result<(), Box<dyn Error>> {
	write_i32(rgw, &item.index)?;
	write_i32(rgw, &item.id.iter().map(|i| *i as i32).collect::<Vec<_>>())?;
	write_i32(rgw, &item.r#type.iter().map(|t| t.0 as i32).collect::<Vec<_>>())?;
	write_i32(rgw, &item.state.iter().map(|s| *s as i32).collect::<Vec<_>>())?;
	write_bool(rgw, &item.direction.iter().map(|d| *d == Direction::Right).collect::<Vec<_>>())?;
	write_f32(rgw, &item.position.iter().map(|p| p.x).collect::<Vec<_>>())?;
	write_f32(rgw, &item.position.iter().map(|p| p.y).collect::<Vec<_>>())?;
	write_f32(rgw, &item.velocity.iter().map(|v| v.x).collect::<Vec<_>>())?;
	write_f32(rgw, &item.velocity.iter().map(|v| v.y).collect::<Vec<_>>())?;
	write_i32(rgw, &item.damage.iter().map(|d| *d as i32).collect::<Vec<_>>())?;
	write_f32(rgw, &item.timer)?;

	// v3.2
	if let Some(misc) = &item.misc {
		write_i32(rgw, &misc.iter().map(|m| u32::from_le_bytes(*m) as i32).collect::<Vec<_>>())?;
		// v3.6
		if let Some(owner) = &item.owner {
			write_i32(rgw, &owner.iter()
				.map(|o| o.map(|o| o as i32).unwrap_or(u8::MAX as i32))
				.collect::<Vec<_>>())?;
		}
	}

	Ok(())
}

fn schema_frame_pre(frames: &transform::Frames) -> String {
	let mut schema = String::from(SCHEMA_FRAME_PRE);
	let pre = &frames.leader.pre;
	if pre.raw_analog_x.is_some() {
		schema += SCHEMA_FRAME_PRE_V1_2;
		if pre.damage.is_some() {
			schema += SCHEMA_FRAME_PRE_V1_4;
		}
	}
	schema
}

fn schema_frame_post(frames: &transform::Frames) -> String {
	let mut schema = String::from(SCHEMA_FRAME_POST);
	let post = &frames.leader.post;
	if post.state_age.is_some() {
		schema += SCHEMA_FRAME_POST_V0_2;
		if post.flags.is_some() {
			schema += SCHEMA_FRAME_POST_V2_0;
			if post.hurtbox_state.is_some() {
				schema += SCHEMA_FRAME_POST_V2_1;
				if post.velocities.is_some() {
					schema += SCHEMA_FRAME_POST_V3_5;
					if post.hitlag.is_some() {
						schema += SCHEMA_FRAME_POST_V3_8;
					}
				}
			}
		}
	}
	schema
}

fn schema_frames(frames: &transform::Frames) -> String {
	format!("
message frame_data {{
	required int32 index;
	required int32 port (UINT_8);
	required boolean is_follower;
	required group pre {{ {} }}
	required group post {{ {} }}
}}",
		schema_frame_pre(frames), schema_frame_post(frames))
}

fn schema_item(item: &ItemCol) -> String {
	let mut schema = String::from(SCHEMA_ITEM);
	if item.misc.is_some() {
		schema += SCHEMA_ITEM_V3_2;
		if item.owner.is_some() {
			schema += SCHEMA_ITEM_V3_6;
		}
	}
	schema
}

fn schema_items(item: &ItemCol) -> String {
	format!("
message item_data {{
	{}
}}
",
		schema_item(item))
}

pub fn write_frames<P: AsRef<Path>>(frames: &transform::Frames, path: P) -> Result<(), Box<dyn Error>> {
	let schema = Arc::new(parse_message_type(&schema_frames(frames))?);
	let props = Arc::new(WriterProperties::builder()
		.set_writer_version(parquet::file::properties::WriterVersion::PARQUET_2_0)
		.set_dictionary_enabled(false)
		.set_encoding(Encoding::PLAIN)
		.set_compression(Compression::UNCOMPRESSED)
		.build());
	let file = File::create(path)?;
	let mut writer = SerializedFileWriter::new(file, schema, props)?;

	let num_ports = frames.leader.pre.state.len();
	let num_frames = frames.leader.pre.state[0].len();

	let frame_indexes: Vec<_> = (0 .. num_frames)
		.map(|idx| idx as i32 + FIRST_FRAME_INDEX).collect();

	for port in 0 .. num_ports {
		let mut rgw = writer.next_row_group()?;
		write_i32(&mut rgw, &frame_indexes)?;
		write_i32(&mut rgw, &vec![port as i32; num_frames])?;
		write_bool(&mut rgw, &vec![false; num_frames])?;
		write_pre(&mut rgw, &frames.leader.pre, port)?;
		write_post(&mut rgw, &frames.leader.post, port)?;
		writer.close_row_group(rgw)?;
	}

	for port in 0 .. num_ports {
		use peppi::character::Internal;
		match frames.leader.post.character[port][0] {
			Internal::POPO | Internal::NANA => {
				let mut rgw = writer.next_row_group()?;
				write_i32(&mut rgw, &frame_indexes)?;
				write_i32(&mut rgw, &vec![port as i32; num_frames])?;
				write_bool(&mut rgw, &vec![true; num_frames])?;
				write_pre(&mut rgw, &frames.follower.pre, port)?;
				write_post(&mut rgw, &frames.follower.post, port)?;
				writer.close_row_group(rgw)?;
			},
			_ => {},
		}
	}

	writer.close()?;
	Ok(())
}

pub fn write_items<P: AsRef<Path>>(item: &ItemCol, path: P) -> Result<(), Box<dyn Error>> {
	let schema = Arc::new(parse_message_type(&schema_items(item))?);
	let props = Arc::new(WriterProperties::builder()
		.set_writer_version(parquet::file::properties::WriterVersion::PARQUET_2_0)
		.set_dictionary_enabled(false)
		.set_encoding(Encoding::PLAIN)
		.set_compression(Compression::UNCOMPRESSED)
		.build());
	let file = File::create(path)?;
	let mut writer = SerializedFileWriter::new(file, schema, props)?;

	let mut rgw = writer.next_row_group()?;
	write_item(&mut rgw, item)?;
	writer.close_row_group(rgw)?;

	writer.close()?;

	Ok(())
}
