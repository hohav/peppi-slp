use std::path;

use ndarray::{Array1, Array2};
use hdf5::{File, Group, Result};

use peppi::{frame, game, triggers};
use peppi::primitives::{Position, Velocity};

const MAX_ITEMS: usize = 16;

struct Start {
	random_seed: Array1<u32>,
}

struct EndV3_7 {
	latest_finalized_frame: Array1<i32>,
}

struct End {
	v3_7: Option<EndV3_7>,
}

struct PreV1_4 {
	damage: Array2<f32>,
}

struct PreV1_2 {
	raw_analog_x: Array2<u8>,
	v1_4: Option<PreV1_4>,
}

struct Pre {
	position: Array2<Position>,
	direction: Array2<i8>,
	joystick: Array2<Position>,
	cstick: Array2<Position>,
	triggers_logical: Array2<f32>,
	triggers_physical: Array2<triggers::Physical>,
	random_seed: Array2<u32>,
	buttons_logical: Array2<u32>,
	buttons_physical: Array2<u16>,
	state: Array2<u16>,
	v1_2: Option<PreV1_2>,
}

struct PostV3_5 {
	velocities_autogenous: Array2<Velocity>,
	velocities_knockback: Array2<Velocity>,
}

struct PostV2_1 {
	hurtbox_state: Array2<u8>,
	v3_5: Option<PostV3_5>,
}

struct PostV2_0 {
	flags: Array2<u64>,
	misc_as: Array2<f32>,
	airborne: Array2<bool>,
	ground: Array2<u16>,
	jumps: Array2<u8>,
	l_cancel: Array2<u8>,
	v2_1: Option<PostV2_1>,
}

struct PostV0_2 {
	state_age: Array2<f32>,
	v2_0: Option<PostV2_0>,
}

struct Post {
	position: Array2<Position>,
	direction: Array2<i8>,
	damage: Array2<f32>,
	shield: Array2<f32>,
	state: Array2<u16>,
	character: Array2<u8>,
	last_attack_landed: Array2<u8>,
	combo_count: Array2<u8>,
	last_hit_by: Array2<u8>,
	stocks: Array2<u8>,
	v0_2: Option<PostV0_2>,
}

struct ItemV3_6 {
	owner: Array2<u8>,
}

struct ItemV3_2 {
	misc: Array2<u32>,
	v3_6: Option<ItemV3_6>,
}

struct Item {
	id: Array2<u32>,
	r#type: Array2<u16>,
	state: Array2<u8>,
	direction: Array2<i8>,
	position: Array2<Position>,
	velocity: Array2<Velocity>,
	damage: Array2<u16>,
	timer: Array2<f32>,
	v3_2: Option<ItemV3_2>,
}

struct Port {
	pre: Pre,
	post: Post,
}

struct Frames {
	leader: Port,
	follower: Port,
	start: Option<Start>,
	end: Option<End>,
	item: Option<Item>,
}

fn start(dim: usize, _start: frame::Start) -> Start {
	Start {
		random_seed: Array1::zeros(dim),
	}
}

fn end_v3_7(dim: usize, _end: frame::EndV3_7) -> EndV3_7 {
	EndV3_7 {
		latest_finalized_frame: Array1::zeros(dim),
	}
}

fn end(dim: usize, end: frame::End) -> End {
	End {
		v3_7: end.v3_7.map(|v3_7| end_v3_7(dim, v3_7)),
	}
}

fn pre_v1_4(dim: (usize, usize), _pre: frame::PreV1_4) -> PreV1_4 {
	PreV1_4 {
		damage: Array2::zeros(dim),
	}
}

fn pre_v1_2(dim: (usize, usize), pre: frame::PreV1_2) -> PreV1_2 {
	PreV1_2 {
		raw_analog_x: Array2::zeros(dim),
		v1_4: pre.v1_4.map(|v1_4| pre_v1_4(dim, v1_4)),
	}
}

fn pre<const N: usize>(frames: &Vec<frame::Frame<N>>) -> Pre {
	let dim = (N, frames.len());
	let f = frames[0].ports[0].leader.pre;
	Pre {
		position: Array2::zeros(dim),
		direction: Array2::zeros(dim),
		joystick: Array2::zeros(dim),
		cstick: Array2::zeros(dim),
		triggers_logical: Array2::zeros(dim),
		triggers_physical: Array2::zeros(dim),
		random_seed: Array2::zeros(dim),
		buttons_logical: Array2::zeros(dim),
		buttons_physical: Array2::zeros(dim),
		state: Array2::zeros(dim),
		v1_2: f.v1_2.map(|v1_2| pre_v1_2(dim, v1_2)),
	}
}

fn post_v3_5(dim: (usize, usize), _post: frame::PostV3_5) -> PostV3_5 {
	PostV3_5 {
		velocities_autogenous: Array2::zeros(dim),
		velocities_knockback: Array2::zeros(dim),
	}
}

fn post_v2_1(dim: (usize, usize), post: frame::PostV2_1) -> PostV2_1 {
	PostV2_1 {
		hurtbox_state: Array2::zeros(dim),
		v3_5: post.v3_5.map(|v3_5| post_v3_5(dim, v3_5)),
	}
}

fn post_v2_0(dim: (usize, usize), post: frame::PostV2_0) -> PostV2_0 {
	PostV2_0 {
		flags: Array2::zeros(dim),
		misc_as: Array2::zeros(dim),
		airborne: Array2::from_elem(dim, false),
		ground: Array2::zeros(dim),
		jumps: Array2::zeros(dim),
		l_cancel: Array2::zeros(dim),
		v2_1: post.v2_1.map(|v2_1| post_v2_1(dim, v2_1)),
	}
}

fn post_v0_2(dim: (usize, usize), post: frame::PostV0_2) -> PostV0_2 {
	PostV0_2 {
		state_age: Array2::zeros(dim),
		v2_0: post.v2_0.map(|v2_0| post_v2_0(dim, v2_0)),
	}
}

fn post<const N: usize>(frames: &Vec<frame::Frame<N>>) -> Post {
	let dim = (N, frames.len());
	let f = frames[0].ports[0].leader.post;
	Post {
		position: Array2::zeros(dim),
		direction: Array2::zeros(dim),
		damage: Array2::zeros(dim),
		shield: Array2::zeros(dim),
		state: Array2::zeros(dim),
		character: Array2::zeros(dim),
		last_attack_landed: Array2::zeros(dim),
		combo_count: Array2::zeros(dim),
		last_hit_by: Array2::zeros(dim),
		stocks: Array2::zeros(dim),
		v0_2: f.v0_2.map(|v0_2| post_v0_2(dim, v0_2)),
	}
}

fn item_v3_6(dim: (usize, usize), _v3_6: frame::ItemV3_6) -> ItemV3_6 {
	ItemV3_6 {
		owner: Array2::zeros(dim),
	}
}

fn item_v3_2(dim: (usize, usize), v3_2: frame::ItemV3_2) -> ItemV3_2 {
	ItemV3_2 {
		misc: Array2::zeros(dim),
		v3_6: v3_2.v3_6.map(|v3_6| item_v3_6(dim, v3_6)),
	}
}

fn item(dim: (usize, usize), item: &frame::Item) -> Item {
	Item {
		id: Array2::from_elem(dim, u32::MAX),
		r#type: Array2::zeros(dim),
		state: Array2::zeros(dim),
		direction: Array2::zeros(dim),
		position: Array2::zeros(dim),
		velocity: Array2::zeros(dim),
		damage: Array2::zeros(dim),
		timer: Array2::zeros(dim),
		v3_2: item.v3_2.map(|v3_2| item_v3_2(dim, v3_2)),
	}
}

fn transform_pre_v1_4(src: &frame::PreV1_4, dst: &mut PreV1_4, i: (usize, usize)) {
	dst.damage[i] = src.damage;
}

fn transform_pre_v1_2(src: &frame::PreV1_2, dst: &mut PreV1_2, i: (usize, usize)) {
	dst.raw_analog_x[i] = src.raw_analog_x;
	if let Some(ref mut dst) = dst.v1_4 {
		transform_pre_v1_4(&src.v1_4.unwrap(), dst, i);
	}
}

fn transform_pre(src: &frame::Pre, dst: &mut Pre, i: (usize, usize)) {
	dst.position[i] = src.position;
	dst.direction[i] = src.direction.0;
	dst.joystick[i] = src.joystick;
	dst.cstick[i] = src.cstick;
	dst.triggers_logical[i] = src.triggers.logical;
	dst.triggers_physical[i] = src.triggers.physical;
	dst.random_seed[i] = src.random_seed;
	dst.buttons_logical[i] = src.buttons.logical.0;
	dst.buttons_physical[i] = src.buttons.physical.0;
	dst.state[i] = src.state.into();
	if let Some(ref mut dst) = dst.v1_2 {
		transform_pre_v1_2(&src.v1_2.unwrap(), dst, i);
	}
}

fn transform_post_v3_5(src: &frame::PostV3_5, dst: &mut PostV3_5, i: (usize, usize)) {
	dst.velocities_autogenous[i] = src.velocities.autogenous;
	dst.velocities_knockback[i] = src.velocities.knockback;
}

fn transform_post_v2_1(src: &frame::PostV2_1, dst: &mut PostV2_1, i: (usize, usize)) {
	dst.hurtbox_state[i] = src.hurtbox_state.0;
	if let Some(ref mut dst) = dst.v3_5 {
		transform_post_v3_5(&src.v3_5.unwrap(), dst, i);
	}
}

fn transform_post_v2_0(src: &frame::PostV2_0, dst: &mut PostV2_0, i: (usize, usize)) {
	dst.flags[i] = src.flags.0;
	dst.misc_as[i] = src.misc_as;
	dst.airborne[i] = src.airborne;
	dst.ground[i] = src.ground;
	dst.jumps[i] = src.jumps;
	dst.l_cancel[i] = match src.l_cancel {
		Some(true) => 1,
		Some(false) => 2,
		_ => 0,
	};
	if let Some(ref mut dst) = dst.v2_1 {
		transform_post_v2_1(&src.v2_1.unwrap(), dst, i);
	}
}

fn transform_post_v0_2(src: &frame::PostV0_2, dst: &mut PostV0_2, i: (usize, usize)) {
	dst.state_age[i] = src.state_age;
	if let Some(ref mut dst) = dst.v2_0 {
		transform_post_v2_0(&src.v2_0.unwrap(), dst, i);
	}
}

fn transform_post(src: &frame::Post, dst: &mut Post, i: (usize, usize)) {
	dst.position[i] = src.position;
	dst.direction[i] = src.direction.0;
	dst.damage[i] = src.damage;
	dst.shield[i] = src.shield;
	dst.state[i] = src.state.into();
	dst.character[i] = src.character.0;
	dst.last_attack_landed[i] = match src.last_attack_landed {
		Some(a) => a.0,
		_ => 0,
	};
	dst.combo_count[i] = src.combo_count;
	dst.last_hit_by[i] = match src.last_hit_by {
		Some(l) => l as u8,
		_ => u8::MAX,
	};
	dst.stocks[i] = src.stocks;
	if let Some(ref mut dst) = dst.v0_2 {
		transform_post_v0_2(&src.v0_2.unwrap(), dst, i);
	}
}

fn transform_port(src: &frame::Data, dst: &mut Port, i: (usize, usize)) {
	transform_pre(&src.pre, &mut dst.pre, i);
	transform_post(&src.post, &mut dst.post, i);
}

fn transform_item_v3_6(src: &frame::ItemV3_6, dst: &mut ItemV3_6, i: (usize, usize)) {
	dst.owner[i] = match src.owner {
		Some(o) => o as u8,
		_ => u8::MAX,
	}
}

fn transform_item_v3_2(src: &frame::ItemV3_2, dst: &mut ItemV3_2, i: (usize, usize)) {
	dst.misc[i] = u32::from_le_bytes(src.misc);
	if let Some(ref mut dst) = dst.v3_6 {
		transform_item_v3_6(&src.v3_6.unwrap(), dst, i);
	}
}

fn transform_item(src: &frame::Item, dst: &mut Item, i: (usize, usize)) {
	dst.id[i] = src.id;
	dst.r#type[i] = src.r#type.0;
	dst.state[i] = src.state;
	dst.direction[i] = src.direction.0;
	dst.position[i] = src.position;
	dst.velocity[i] = src.velocity;
	dst.damage[i] = src.damage;
	dst.timer[i] = src.timer;
	if let Some(ref mut dst) = dst.v3_2 {
		transform_item_v3_2(&src.v3_2.unwrap(), dst, i);
	}
}

fn transform_start(src: &frame::Start, dst: &mut Start, i: usize) {
	dst.random_seed[i] = src.random_seed;
}

fn transform_end_v3_7(src: &frame::EndV3_7, dst: &mut EndV3_7, i: usize) {
	dst.latest_finalized_frame[i] = src.latest_finalized_frame;
}

fn transform_end(src: &frame::End, dst: &mut End, i: usize) {
	if let Some(ref mut dst) = dst.v3_7 {
		transform_end_v3_7(&src.v3_7.unwrap(), dst, i);
	}
}

fn transform_frames<const N: usize>(src: &Vec<frame::Frame<N>>) -> Frames {
	let len = src.len();
	let mut dst = Frames {
		start: src[0].start.map(|s| start(len, s)),
		end: src[0].end.map(|e| end(len, e)),
		leader: Port {
			pre: pre(src),
			post: post(src),
		},
		follower: Port {
			pre: pre(src),
			post: post(src),
		},
		item: src.iter()
			.flat_map(|f| f.items.as_ref().and_then(|i| i.first()))
			.next().map(|i| item((len, MAX_ITEMS), i)),
	};

	for (f_idx, f) in src.iter().enumerate() {
		if let Some(ref mut start) = dst.start {
			transform_start(&f.start.unwrap(), start, f_idx);
		}

		if let Some(ref mut end) = dst.end {
			transform_end(&f.end.unwrap(), end, f_idx);
		}

		for (p_idx, p) in f.ports.iter().enumerate() {
			transform_port(&p.leader, &mut dst.leader, (p_idx, f_idx));
			if let Some(follower) = &p.follower {
				transform_port(&follower, &mut dst.follower, (p_idx, f_idx));
			}
		}

		if let Some(ref mut dst_item) = dst.item {
			let items = f.items.as_ref().unwrap();
			for (i_idx, src_item) in items.iter().enumerate() {
				transform_item(&src_item, dst_item, (f_idx, i_idx));
			}
		}
	}

	dst
}

fn transform(game: &game::Game) -> Frames {
	match &game.frames {
		game::Frames::P1(f) => transform_frames(f),
		game::Frames::P2(f) => transform_frames(f),
		game::Frames::P3(f) => transform_frames(f),
		game::Frames::P4(f) => transform_frames(f),
	}
}

fn write_arr1<T: hdf5::H5Type>(group: &Group, arr: &Array1<T>, name: &'static str) -> Result<()> {
	group.new_dataset::<T>().create(name, arr.dim())?.write(arr)
}

fn write_arr2<T: hdf5::H5Type>(group: &Group, arr: &Array2<T>, name: &'static str) -> Result<()> {
	group.new_dataset::<T>().create(name, arr.dim())?.write(arr)
}

fn write_pre_v1_4(pre: &PreV1_4, g: &Group) -> Result<()> {
	write_arr2(&g, &pre.damage, "damage")?;
	Ok(())
}

fn write_pre_v1_2(pre: &PreV1_2, g: &Group) -> Result<()> {
	write_arr2(&g, &pre.raw_analog_x, "raw_analog_x")?;
	if let Some(pre) = &pre.v1_4 {
		write_pre_v1_4(pre, g)?;
	}
	Ok(())
}

fn write_pre(pre: &Pre, group: &Group) -> Result<()> {
	let g = group.create_group("pre")?;
	write_arr2(&g, &pre.position, "position")?;
	write_arr2(&g, &pre.direction, "direction")?;
	write_arr2(&g, &pre.joystick, "joystick")?;
	write_arr2(&g, &pre.cstick, "cstick")?;
	write_arr2(&g, &pre.triggers_logical, "triggers_logical")?;
	write_arr2(&g, &pre.triggers_physical, "triggers_physical")?;
	write_arr2(&g, &pre.random_seed, "random_seed")?;
	write_arr2(&g, &pre.buttons_logical, "buttons_logical")?;
	write_arr2(&g, &pre.buttons_physical, "buttons_physical")?;
	write_arr2(&g, &pre.state, "state")?;
	if let Some(pre) = &pre.v1_2 {
		write_pre_v1_2(pre, &g)?;
	}
	Ok(())
}

fn write_post_v3_5(post: &PostV3_5, g: &Group) -> Result<()> {
	write_arr2(&g, &post.velocities_autogenous, "velocities_autogenous")?;
	write_arr2(&g, &post.velocities_knockback, "velocities_knockback")?;
	Ok(())
}

fn write_post_v2_1(post: &PostV2_1, g: &Group) -> Result<()> {
	write_arr2(&g, &post.hurtbox_state, "hurtbox_state")?;
	if let Some(post) = &post.v3_5 {
		write_post_v3_5(post, g)?;
	}
	Ok(())
}

fn write_post_v2_0(post: &PostV2_0, g: &Group) -> Result<()> {
	write_arr2(&g, &post.flags, "flags")?;
	write_arr2(&g, &post.misc_as, "misc_as")?;
	write_arr2(&g, &post.airborne, "airborne")?;
	write_arr2(&g, &post.ground, "ground")?;
	write_arr2(&g, &post.jumps, "jumps")?;
	write_arr2(&g, &post.l_cancel, "l_cancel")?;
	if let Some(post) = &post.v2_1 {
		write_post_v2_1(post, g)?;
	}
	Ok(())
}

fn write_post_v0_2(post: &PostV0_2, g: &Group) -> Result<()> {
	write_arr2(&g, &post.state_age, "state_age")?;
	if let Some(post) = &post.v2_0 {
		write_post_v2_0(post, g)?;
	}
	Ok(())
}

fn write_post(post: &Post, group: &Group) -> Result<()> {
	let g = group.create_group("post")?;
	write_arr2(&g, &post.position, "position")?;
	write_arr2(&g, &post.direction, "direction")?;
	write_arr2(&g, &post.damage, "damage")?;
	write_arr2(&g, &post.shield, "shield")?;
	write_arr2(&g, &post.state, "state")?;
	write_arr2(&g, &post.character, "character")?;
	write_arr2(&g, &post.last_attack_landed, "last_attack_landed")?;
	write_arr2(&g, &post.combo_count, "combo_count")?;
	write_arr2(&g, &post.last_hit_by, "last_hit_by")?;
	write_arr2(&g, &post.stocks, "stocks")?;
	if let Some(post) = &post.v0_2 {
		write_post_v0_2(post, &g)?;
	}
	Ok(())
}

fn write_port(port: &Port, group: &Group) -> Result<()> {
	write_pre(&port.pre, &group)?;
	write_post(&port.post, &group)?;
	Ok(())
}

fn write_start(start: &Start, file: &File) -> Result<()> {
	let g_start = file.create_group("start")?;
	write_arr1(&g_start, &start.random_seed, "random_seed")?;
	Ok(())
}

fn write_end_v3_7(end: &EndV3_7, g: &Group) -> Result<()> {
	write_arr1(&g, &end.latest_finalized_frame, "latest_finalized_frame")?;
	Ok(())
}

fn write_end(end: &End, file: &File) -> Result<()> {
	let g_end = file.create_group("end")?;
	if let Some(end) = &end.v3_7 {
		write_end_v3_7(end, &g_end)?;
	}
	Ok(())
}

fn write_item_v3_6(item: &ItemV3_6, g: &Group) -> Result<()> {
	write_arr2(&g, &item.owner, "owner")?;
	Ok(())
}

fn write_item_v3_2(item: &ItemV3_2, g: &Group) -> Result<()> {
	write_arr2(&g, &item.misc, "misc")?;
	if let Some(item) = &item.v3_6 {
		write_item_v3_6(item, g)?;
	}
	Ok(())
}

fn write_item(item: &Item, file: &File) -> Result<()> {
	let g_item = file.create_group("item")?;
	write_arr2(&g_item, &item.id, "id")?;
	write_arr2(&g_item, &item.r#type, "type")?;
	write_arr2(&g_item, &item.state, "state")?;
	write_arr2(&g_item, &item.direction, "direction")?;
	write_arr2(&g_item, &item.position, "position")?;
	write_arr2(&g_item, &item.velocity, "velocity")?;
	write_arr2(&g_item, &item.damage, "damage")?;
	write_arr2(&g_item, &item.timer, "timer")?;
	if let Some(item) = &item.v3_2 {
		write_item_v3_2(item, &g_item)?;
	}
	Ok(())
}

pub fn write<P: AsRef<path::Path>>(game: &game::Game, path: P) -> Result<()> {
	let file = File::create(path)?;
	let frames = transform(game);

	if let Some(start) = &frames.start {
		write_start(start, &file)?;
	}

	if let Some(end) = &frames.end {
		write_end(end, &file)?;
	}

	write_port(&frames.leader, &file.create_group("leader")?)?;
	write_port(&frames.follower, &file.create_group("follower")?)?;

	if let Some(item) = &frames.item {
		write_item(item, &file)?;
	}

	Ok(())
}
