use std::{fs, path, sync::Arc};
use std::error::Error;

use parquet::{
	column::writer::{get_typed_column_writer_mut},
	data_type::{BoolType, FloatType, Int32Type, Int64Type},
	file::{
		properties::WriterProperties,
		writer::{FileWriter, RowGroupWriter, SerializedFileWriter},
	},
	schema::parser::parse_message_type,
};

use super::transform;
use super::transform::MAX_ITEMS;

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

fn write_bool(rgw: &mut Box<dyn RowGroupWriter>, data: &[bool], dls: &[i16], rls: &[i16]) -> Result<(), Box<dyn Error>> {
	let mut c = rgw.next_column()?.ok_or("no column")?;
	let w = get_typed_column_writer_mut::<BoolType>(&mut c);
	w.write_batch(data, Some(&dls), Some(&rls))?;
	rgw.close_column(c)?;
	Ok(())
}

fn write_i32(rgw: &mut Box<dyn RowGroupWriter>, data: &[i32], dls: &[i16], rls: &[i16]) -> Result<(), Box<dyn Error>> {
	let mut c = rgw.next_column()?.ok_or("no column")?;
	let w = get_typed_column_writer_mut::<Int32Type>(&mut c);
	w.write_batch(data, Some(&dls), Some(&rls))?;
	rgw.close_column(c)?;
	Ok(())
}

fn write_i64(rgw: &mut Box<dyn RowGroupWriter>, data: &[i64], dls: &[i16], rls: &[i16]) -> Result<(), Box<dyn Error>> {
	let mut c = rgw.next_column()?.ok_or("no column")?;
	let w = get_typed_column_writer_mut::<Int64Type>(&mut c);
	w.write_batch(data, Some(&dls), Some(&rls))?;
	rgw.close_column(c)?;
	Ok(())
}

fn write_f32(rgw: &mut Box<dyn RowGroupWriter>, data: &[f32], dls: &[i16], rls: &[i16]) -> Result<(), Box<dyn Error>> {
	let mut c = rgw.next_column()?.ok_or("no column")?;
	let w = get_typed_column_writer_mut::<FloatType>(&mut c);
	w.write_batch(data, Some(&dls), Some(&rls))?;
	rgw.close_column(c)?;
	Ok(())
}

fn write_pre(rgw: &mut Box<dyn RowGroupWriter>, pre: &transform::Pre, p: usize) -> Result<(), Box<dyn Error>> {
	let dls: Vec<Vec<_>> =
		(0i16 .. 4)
		.map(|n| pre.position.x[0].iter().map(|_| n).collect())
		.collect();
	let rls = {
		let mut x: Vec<_> = pre.position.x[0].iter().map(|_| 1i16).collect();
		x[0] = 0;
		x
	};

	write_f32(rgw, &pre.position.x[p], &dls[1], &rls)?;
	write_f32(rgw, &pre.position.y[p], &dls[1], &rls)?;
	write_bool(rgw, &pre.direction[p], &dls[1], &rls)?;
	write_f32(rgw, &pre.joystick.x[p], &dls[1], &rls)?;
	write_f32(rgw, &pre.joystick.y[p], &dls[1], &rls)?;
	write_f32(rgw, &pre.cstick.x[p], &dls[1], &rls)?;
	write_f32(rgw, &pre.cstick.y[p], &dls[1], &rls)?;
	write_f32(rgw, &pre.triggers.logical[p], &dls[1], &rls)?;
	write_f32(rgw, &pre.triggers.physical.l[p], &dls[1], &rls)?;
	write_f32(rgw, &pre.triggers.physical.r[p], &dls[1], &rls)?;
	write_i32(rgw, &pre.random_seed[p], &dls[1], &rls)?;
	write_i32(rgw, &pre.buttons.physical[p], &dls[1], &rls)?;
	write_i32(rgw, &pre.buttons.logical[p], &dls[1], &rls)?;
	write_i32(rgw, &pre.state[p], &dls[1], &rls)?;

	if let Some(v1_2) = &pre.v1_2 {
		write_i32(rgw, &v1_2.raw_analog_x[p], &dls[2], &rls)?;
		if let Some(v1_4) = &v1_2.v1_4 {
			write_f32(rgw, &v1_4.damage[p], &dls[3], &rls)?;
		}
	}

	Ok(())
}

fn write_post(rgw: &mut Box<dyn RowGroupWriter>, post: &transform::Post, p: usize) -> Result<(), Box<dyn Error>> {
	let dls: Vec<Vec<_>> =
		(0i16 .. 7)
		.map(|n| post.position.x[0].iter().map(|_| n).collect())
		.collect();
	let rls = {
		let mut rls: Vec<_> = post.position.x[0].iter().map(|_| 1i16).collect();
		rls[0] = 0;
		rls
	};

	write_f32(rgw, &post.position.x[p], &dls[1], &rls)?;
	write_f32(rgw, &post.position.y[p], &dls[1], &rls)?;
	write_bool(rgw, &post.direction[p], &dls[1], &rls)?;
	write_f32(rgw, &post.damage[p], &dls[1], &rls)?;
	write_f32(rgw, &post.shield[p], &dls[1], &rls)?;
	write_i32(rgw, &post.state[p], &dls[1], &rls)?;
	write_i32(rgw, &post.character[p], &dls[1], &rls)?;
	write_i32(rgw, &post.last_attack_landed[p], &dls[1], &rls)?;
	write_i32(rgw, &post.combo_count[p], &dls[1], &rls)?;
	write_i32(rgw, &post.last_hit_by[p], &dls[1], &rls)?;
	write_i32(rgw, &post.stocks[p], &dls[1], &rls)?;

	if let Some(v0_2) = &post.v0_2 {
		write_f32(rgw, &v0_2.state_age[p], &dls[2], &rls)?;
		if let Some(v2_0) = &v0_2.v2_0 {
			write_i64(rgw, &v2_0.flags[p], &dls[3], &rls)?;
			write_f32(rgw, &v2_0.misc_as[p], &dls[3], &rls)?;
			write_bool(rgw, &v2_0.airborne[p], &dls[3], &rls)?;
			write_i32(rgw, &v2_0.ground[p], &dls[3], &rls)?;
			write_i32(rgw, &v2_0.jumps[p], &dls[3], &rls)?;
			write_i32(rgw, &v2_0.l_cancel[p], &dls[3], &rls)?;
			if let Some(v2_1) = &v2_0.v2_1 {
				write_i32(rgw, &v2_1.hurtbox_state[p], &dls[4], &rls)?;
				if let Some(v3_5) = &v2_1.v3_5 {
					write_f32(rgw, &v3_5.velocities.autogenous.x[p], &dls[5], &rls)?;
					write_f32(rgw, &v3_5.velocities.autogenous.y[p], &dls[5], &rls)?;
					write_f32(rgw, &v3_5.velocities.knockback.x[p], &dls[5], &rls)?;
					write_f32(rgw, &v3_5.velocities.knockback.y[p], &dls[5], &rls)?;
					if let Some(v3_8) = &v3_5.v3_8 {
						write_f32(rgw, &v3_8.hitlag[p], &dls[6], &rls)?;
					}
				}
			}
		}
	}

	Ok(())
}

fn write_item(rgw: &mut Box<dyn RowGroupWriter>, item: &transform::Item) -> Result<(), Box<dyn Error>> {
	let dls = {
		let mut dls: Vec<_> = (0 ..= MAX_ITEMS).map(|n| vec![1i16; n]).collect();
		dls[0] = vec![0i16; 1];
		dls
	};

	let rls = {
		let mut rls: Vec<_> = (0 ..= MAX_ITEMS).map(|n| vec![1i16; n]).collect();
		for r in rls.iter_mut() {
			if r.len() > 0 {
				r[0] = 0;
			}
		}
		rls[0] = vec![0i16; 1];
		rls
	};

	let len = item.id.len();
	let lens: Vec<_> = item.id.iter().map(|row| row.len()).collect();

	let mut c = rgw.next_column()?.ok_or("no column: item.id")?;
	let w = get_typed_column_writer_mut::<Int32Type>(&mut c);
	for n in 0 .. len {
		w.write_batch(&item.id[n],
			Some(&dls[lens[n]]), Some(&rls[lens[n]]))?;
	}
	rgw.close_column(c)?;

	let mut c = rgw.next_column()?.ok_or("no column: item.type")?;
	let w = get_typed_column_writer_mut::<Int32Type>(&mut c);
	for n in 0 .. len {
		w.write_batch(&item.r#type[n],
			Some(&dls[lens[n]]), Some(&rls[lens[n]]))?;
	}
	rgw.close_column(c)?;

	let mut c = rgw.next_column()?.ok_or("no column: item.state")?;
	let w = get_typed_column_writer_mut::<Int32Type>(&mut c);
	for n in 0 .. len {
		w.write_batch(&item.state[n],
			Some(&dls[lens[n]]), Some(&rls[lens[n]]))?;
	}
	rgw.close_column(c)?;

	let mut c = rgw.next_column()?.ok_or("no column: item.direction")?;
	let w = get_typed_column_writer_mut::<BoolType>(&mut c);
	for n in 0 .. len {
		w.write_batch(&item.direction[n],
			Some(&dls[lens[n]]), Some(&rls[lens[n]]))?;
	}
	rgw.close_column(c)?;

	let mut c = rgw.next_column()?.ok_or("no column: item.position.x")?;
	let w = get_typed_column_writer_mut::<FloatType>(&mut c);
	for n in 0 .. len {
		w.write_batch(&item.position.x[n],
			Some(&dls[lens[n]]), Some(&rls[lens[n]]))?;
	}
	rgw.close_column(c)?;

	let mut c = rgw.next_column()?.ok_or("no column: item.position.y")?;
	let w = get_typed_column_writer_mut::<FloatType>(&mut c);
	for n in 0 .. len {
		w.write_batch(&item.position.y[n],
			Some(&dls[lens[n]]), Some(&rls[lens[n]]))?;
	}
	rgw.close_column(c)?;

	let mut c = rgw.next_column()?.ok_or("no column: item.velocity.x")?;
	let w = get_typed_column_writer_mut::<FloatType>(&mut c);
	for n in 0 .. len {
		w.write_batch(&item.velocity.x[n],
			Some(&dls[lens[n]]), Some(&rls[lens[n]]))?;
	}
	rgw.close_column(c)?;

	let mut c = rgw.next_column()?.ok_or("no column: item.velocity.y")?;
	let w = get_typed_column_writer_mut::<FloatType>(&mut c);
	for n in 0 .. len {
		w.write_batch(&item.velocity.y[n],
			Some(&dls[lens[n]]), Some(&rls[lens[n]]))?;
	}
	rgw.close_column(c)?;

	let mut c = rgw.next_column()?.ok_or("no column: item.damage")?;
	let w = get_typed_column_writer_mut::<Int32Type>(&mut c);
	for n in 0 .. len {
		w.write_batch(&item.damage[n],
			Some(&dls[lens[n]]), Some(&rls[lens[n]]))?;
	}
	rgw.close_column(c)?;

	let mut c = rgw.next_column()?.ok_or("no column: item.timer")?;
	let w = get_typed_column_writer_mut::<FloatType>(&mut c);
	for n in 0 .. len {
		w.write_batch(&item.timer[n],
			Some(&dls[lens[n]]), Some(&rls[lens[n]]))?;
	}
	rgw.close_column(c)?;

	if let Some(v3_2) = &item.v3_2 {
		let dls = {
			let mut dls: Vec<_> = (0 ..= MAX_ITEMS).map(|n| vec![2i16; n]).collect();
			dls[0] = vec![0i16; 1];
			dls
		};

		let mut c = rgw.next_column()?.ok_or("no column: item.v3_2.misc")?;
		let w = get_typed_column_writer_mut::<Int32Type>(&mut c);
		for n in 0 .. len {
			w.write_batch(&v3_2.misc[n],
				Some(&dls[lens[n]]), Some(&rls[lens[n]]))?;
		}
		rgw.close_column(c)?;

		if let Some(v3_6) = &v3_2.v3_6 {
			let dls = {
				let mut dls: Vec<_> = (0 ..= MAX_ITEMS).map(|n| vec![3i16; n]).collect();
				dls[0] = vec![0i16; 1];
				dls
			};

			let mut c = rgw.next_column()?.ok_or("no column: item.v3_2.v3_6.owner")?;
			let w = get_typed_column_writer_mut::<Int32Type>(&mut c);
			for n in 0 .. len {
				w.write_batch(&v3_6.owner[n],
					Some(&dls[lens[n]]), Some(&rls[lens[n]]))?;
			}
			rgw.close_column(c)?;
		}
	}

	Ok(())
}

fn schema_frame_pre(frames: &transform::Frames) -> String {
	let mut schema = String::from(SCHEMA_FRAME_PRE);
	if let Some(v1_2) = &frames.leader.pre.v1_2 {
		schema += "optional group v1_2 {";
		schema += SCHEMA_FRAME_PRE_V1_2;
		if let Some(_v1_4) = &v1_2.v1_4 {
			schema += "optional group v1_2 {";
			schema += SCHEMA_FRAME_PRE_V1_4;
			schema += "}";
		}
		schema += "}";
	}
	schema
}

fn schema_frame_post(frames: &transform::Frames) -> String {
	let mut schema = String::from(SCHEMA_FRAME_POST);
	if let Some(v0_2) = &frames.leader.post.v0_2 {
		schema += "optional group v0_2 {";
		schema += SCHEMA_FRAME_POST_V0_2;
		if let Some(v2_0) = &v0_2.v2_0 {
			schema += "optional group v2_0 {";
			schema += SCHEMA_FRAME_POST_V2_0;
			if let Some(v2_1) = &v2_0.v2_1 {
				schema += "optional group v2_1 {";
				schema += SCHEMA_FRAME_POST_V2_1;
				if let Some(v3_5) = &v2_1.v3_5 {
					schema += "optional group v3_5 {";
					schema += SCHEMA_FRAME_POST_V3_5;
					if let Some(_v3_8) = &v3_5.v3_8 {
						schema += "optional group v3_8 {";
						schema += SCHEMA_FRAME_POST_V3_8;
						schema += "}";
					}
					schema += "}";
				}
				schema += "}";
			}
			schema += "}";
		}
		schema += "}";
	}
	schema
}

fn schema_frames(frames: &transform::Frames) -> String {
	format!("
message frame_data {{
	required int32 port (UINT_8);
	required boolean is_follower;
	repeated group frames {{
		required group pre {{ {} }}
		required group post {{ {} }}
	}}
}}",
		schema_frame_pre(frames), schema_frame_post(frames))
}

fn schema_item(item: &transform::Item) -> String {
	let mut schema = String::from(SCHEMA_ITEM);
	if let Some(v3_2) = &item.v3_2 {
		schema += "optional group v3_2 {";
		schema += SCHEMA_ITEM_V3_2;
		if let Some(_v3_6) = &v3_2.v3_6 {
			schema += "optional group v3_6 {";
			schema += SCHEMA_ITEM_V3_6;
			schema += "}";
		}
		schema += "}";
	}
	schema
}

fn schema_items(item: &transform::Item) -> String {
	format!("
message item_data {{
	repeated group items {{ {} }}
}}
",
		schema_item(item))
}

pub fn write_frames<P: AsRef<path::Path>>(frames: &transform::Frames, path: P) -> Result<(), Box<dyn Error>> {
	let schema = Arc::new(parse_message_type(&schema_frames(frames))?);
	let props = Arc::new(WriterProperties::builder()
		.set_writer_version(parquet::file::properties::WriterVersion::PARQUET_2_0)
		.set_dictionary_enabled(false)
		.set_encoding(parquet::basic::Encoding::PLAIN)
		.set_compression(parquet::basic::Compression::UNCOMPRESSED)
		.build());
	let file = fs::File::create(path)?;
	let mut writer = SerializedFileWriter::new(file, schema, props)?;

	for port in 0 .. frames.leader.pre.position.x.len() {
		let mut rgw = writer.next_row_group()?;
		write_i32(&mut rgw, &[port as i32], &[0], &[0])?; // port
		write_bool(&mut rgw, &[false], &[0], &[0])?; // is_follower
		write_pre(&mut rgw, &frames.leader.pre, port)?;
		write_post(&mut rgw, &frames.leader.post, port)?;
		writer.close_row_group(rgw)?;
	}

	for port in 0 .. frames.leader.pre.position.x.len() {
		use peppi::character::Internal;
		match frames.leader.post.character[port][0] {
			x if x == Internal::POPO.0 as i32 || x == Internal::NANA.0 as i32 => {
				let mut rgw = writer.next_row_group()?;
				write_i32(&mut rgw, &[port as i32], &[0], &[0])?; // port
				write_bool(&mut rgw, &[true], &[0], &[0])?; // is_follower
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

pub fn write_items<P: AsRef<path::Path>>(item: &transform::Item, path: P) -> Result<(), Box<dyn Error>> {
	let schema = Arc::new(parse_message_type(&schema_items(item))?);
	let props = Arc::new(WriterProperties::builder()
		.set_writer_version(parquet::file::properties::WriterVersion::PARQUET_2_0)
		.set_dictionary_enabled(false)
		.set_encoding(parquet::basic::Encoding::PLAIN)
		.set_compression(parquet::basic::Compression::UNCOMPRESSED)
		.build());
	let file = fs::File::create(path)?;
	let mut writer = SerializedFileWriter::new(file, schema, props)?;

	let mut rgw = writer.next_row_group()?;
	write_item(&mut rgw, item)?;
	writer.close_row_group(rgw)?;
	writer.close()?;
	Ok(())
}
