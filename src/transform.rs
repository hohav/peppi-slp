use ndarray::{Array1, Array2};

use peppi::{frame, game};

pub const MAX_ITEMS: usize = 16;

pub struct Position {
	pub x: Array2<f32>,
	pub y: Array2<f32>,
}

pub struct Velocity {
	pub x: Array2<f32>,
	pub y: Array2<f32>,
}

pub struct Velocities {
	pub autogenous: Velocity,
	pub knockback: Velocity,
}

pub struct TriggersPhysical {
	pub l: Array2<f32>,
	pub r: Array2<f32>,
}

pub struct Triggers {
	pub logical: Array2<f32>,
	pub physical: TriggersPhysical,
}

pub struct Buttons {
	pub logical: Array2<i32>,
	pub physical: Array2<i32>,
}

pub struct Start {
	pub random_seed: Array1<i32>,
}

pub struct EndV3_7 {
	pub latest_finalized_frame: Array1<i32>,
}

pub struct End {
	pub v3_7: Option<EndV3_7>,
}

pub struct PreV1_4 {
	pub damage: Array2<f32>,
}

pub struct PreV1_2 {
	pub raw_analog_x: Array2<i32>,
	pub v1_4: Option<PreV1_4>,
}

pub struct Pre {
	pub position: Position,
	pub direction: Array2<bool>,
	pub joystick: Position,
	pub cstick: Position,
	pub triggers: Triggers,
	pub random_seed: Array2<i32>,
	pub buttons: Buttons,
	pub state: Array2<i32>,
	pub v1_2: Option<PreV1_2>,
}

pub struct PostV3_5 {
	pub velocities: Velocities,
}

pub struct PostV2_1 {
	pub hurtbox_state: Array2<i32>,
	pub v3_5: Option<PostV3_5>,
}

pub struct PostV2_0 {
	pub flags: Array2<i64>,
	pub misc_as: Array2<f32>,
	pub airborne: Array2<bool>,
	pub ground: Array2<i32>,
	pub jumps: Array2<i32>,
	pub l_cancel: Array2<i32>,
	pub v2_1: Option<PostV2_1>,
}

pub struct PostV0_2 {
	pub state_age: Array2<f32>,
	pub v2_0: Option<PostV2_0>,
}

pub struct Post {
	pub position: Position,
	pub direction: Array2<bool>,
	pub damage: Array2<f32>,
	pub shield: Array2<f32>,
	pub state: Array2<i32>,
	pub character: Array2<i32>,
	pub last_attack_landed: Array2<i32>,
	pub combo_count: Array2<i32>,
	pub last_hit_by: Array2<i32>,
	pub stocks: Array2<i32>,
	pub v0_2: Option<PostV0_2>,
}

pub struct ItemV3_6 {
	pub owner: Array2<i32>,
}

pub struct ItemV3_2 {
	pub misc: Array2<i32>,
	pub v3_6: Option<ItemV3_6>,
}

pub struct Item {
	pub id: Array2<i32>,
	pub r#type: Array2<i32>,
	pub state: Array2<i32>,
	pub direction: Array2<bool>,
	pub position: Position,
	pub velocity: Velocity,
	pub damage: Array2<i32>,
	pub timer: Array2<f32>,
	pub v3_2: Option<ItemV3_2>,
}

pub struct Port {
	pub pre: Pre,
	pub post: Post,
}

pub struct Frames {
	pub leader: Port,
	pub follower: Port,
	pub start: Option<Start>,
	pub end: Option<End>,
	pub item: Option<Item>,
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
		position: Position {
			x: Array2::zeros(dim),
			y: Array2::zeros(dim),
		},
		direction: Array2::from_elem(dim, false),
		joystick: Position {
			x: Array2::zeros(dim),
			y: Array2::zeros(dim),
		},
		cstick: Position {
			x: Array2::zeros(dim),
			y: Array2::zeros(dim),
		},
		triggers: Triggers {
			logical: Array2::zeros(dim),
			physical: TriggersPhysical {
				l: Array2::zeros(dim),
				r: Array2::zeros(dim),
			},
		},
		random_seed: Array2::zeros(dim),
		buttons: Buttons {
			logical: Array2::zeros(dim),
			physical: Array2::zeros(dim),
		},
		state: Array2::zeros(dim),
		v1_2: f.v1_2.map(|v1_2| pre_v1_2(dim, v1_2)),
	}
}

fn post_v3_5(dim: (usize, usize), _post: frame::PostV3_5) -> PostV3_5 {
	PostV3_5 {
		velocities: Velocities {
			autogenous: Velocity {
				x: Array2::zeros(dim),
				y: Array2::zeros(dim),
			},
			knockback: Velocity {
				x: Array2::zeros(dim),
				y: Array2::zeros(dim),
			},
		},
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
		position: Position {
			x: Array2::zeros(dim),
			y: Array2::zeros(dim),
		},
		direction: Array2::from_elem(dim, false),
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
		id: Array2::from_elem(dim, -1),
		r#type: Array2::zeros(dim),
		state: Array2::zeros(dim),
		direction: Array2::from_elem(dim, false),
		position: Position {
			x: Array2::zeros(dim),
			y: Array2::zeros(dim),
		},
		velocity: Velocity {
			x: Array2::zeros(dim),
			y: Array2::zeros(dim),
		},
		damage: Array2::zeros(dim),
		timer: Array2::zeros(dim),
		v3_2: item.v3_2.map(|v3_2| item_v3_2(dim, v3_2)),
	}
}

fn transform_pre_v1_4(src: &frame::PreV1_4, dst: &mut PreV1_4, i: (usize, usize)) {
	dst.damage[i] = src.damage;
}

fn transform_pre_v1_2(src: &frame::PreV1_2, dst: &mut PreV1_2, i: (usize, usize)) {
	dst.raw_analog_x[i] = src.raw_analog_x as i32;
	if let Some(ref mut dst) = dst.v1_4 {
		transform_pre_v1_4(&src.v1_4.unwrap(), dst, i);
	}
}

fn transform_pre(src: &frame::Pre, dst: &mut Pre, i: (usize, usize)) {
	dst.position.x[i] = src.position.x;
	dst.position.y[i] = src.position.y;
	dst.direction[i] = src.direction.0 == 1;
	dst.joystick.x[i] = src.joystick.x;
	dst.joystick.y[i] = src.joystick.y;
	dst.cstick.x[i] = src.cstick.x;
	dst.cstick.y[i] = src.cstick.y;
	dst.triggers.logical[i] = src.triggers.logical;
	dst.triggers.physical.l[i] = src.triggers.physical.l;
	dst.triggers.physical.r[i] = src.triggers.physical.r;
	dst.random_seed[i] = src.random_seed as i32;
	dst.buttons.logical[i] = src.buttons.logical.0 as i32;
	dst.buttons.physical[i] = src.buttons.physical.0 as i32;
	dst.state[i] = {
		let s: u16 = src.state.into();
		s as i32
	};
	if let Some(ref mut dst) = dst.v1_2 {
		transform_pre_v1_2(&src.v1_2.unwrap(), dst, i);
	}
}

fn transform_post_v3_5(src: &frame::PostV3_5, dst: &mut PostV3_5, i: (usize, usize)) {
	dst.velocities.autogenous.x[i] = src.velocities.autogenous.x;
	dst.velocities.autogenous.y[i] = src.velocities.autogenous.y;
	dst.velocities.knockback.x[i] = src.velocities.knockback.x;
	dst.velocities.knockback.y[i] = src.velocities.knockback.y;
}

fn transform_post_v2_1(src: &frame::PostV2_1, dst: &mut PostV2_1, i: (usize, usize)) {
	dst.hurtbox_state[i] = src.hurtbox_state.0 as i32;
	if let Some(ref mut dst) = dst.v3_5 {
		transform_post_v3_5(&src.v3_5.unwrap(), dst, i);
	}
}

fn transform_post_v2_0(src: &frame::PostV2_0, dst: &mut PostV2_0, i: (usize, usize)) {
	dst.flags[i] = src.flags.0 as i64;
	dst.misc_as[i] = src.misc_as;
	dst.airborne[i] = src.airborne;
	dst.ground[i] = src.ground as i32;
	dst.jumps[i] = src.jumps as i32;
	dst.l_cancel[i] = match src.l_cancel {
		None => 0,
		Some(true) => 1,
		Some(false) => 2,
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
	dst.position.x[i] = src.position.x;
	dst.position.y[i] = src.position.y;
	dst.direction[i] = src.direction.0 == 1;
	dst.damage[i] = src.damage;
	dst.shield[i] = src.shield;
	dst.state[i] = {
		let s: u16 = src.state.into();
		s as i32
	};
	dst.character[i] = src.character.0 as i32;
	dst.last_attack_landed[i] = match src.last_attack_landed {
		Some(a) => a.0 as i32,
		_ => 0,
	};
	dst.combo_count[i] = src.combo_count as i32;
	dst.last_hit_by[i] = match src.last_hit_by {
		Some(l) => l as i32,
		_ => u8::MAX as i32,
	};
	dst.stocks[i] = src.stocks as i32;
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
		Some(o) => o as i32,
		_ => u8::MAX as i32,
	}
}

fn transform_item_v3_2(src: &frame::ItemV3_2, dst: &mut ItemV3_2, i: (usize, usize)) {
	dst.misc[i] = u32::from_le_bytes(src.misc) as i32;
	if let Some(ref mut dst) = dst.v3_6 {
		transform_item_v3_6(&src.v3_6.unwrap(), dst, i);
	}
}

fn transform_item(src: &frame::Item, dst: &mut Item, i: (usize, usize)) {
	dst.id[i] = src.id as i32;
	dst.r#type[i] = src.r#type.0 as i32;
	dst.state[i] = src.state as i32;
	dst.direction[i] = src.direction.0 == 1;
	dst.position.x[i] = src.position.x;
	dst.velocity.y[i] = src.velocity.y;
	dst.damage[i] = src.damage as i32;
	dst.timer[i] = src.timer;
	if let Some(ref mut dst) = dst.v3_2 {
		transform_item_v3_2(&src.v3_2.unwrap(), dst, i);
	}
}

fn transform_start(src: &frame::Start, dst: &mut Start, i: usize) {
	dst.random_seed[i] = src.random_seed as i32;
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

pub fn transform(game: &game::Game) -> Frames {
	match &game.frames {
		game::Frames::P1(f) => transform_frames(f),
		game::Frames::P2(f) => transform_frames(f),
		game::Frames::P3(f) => transform_frames(f),
		game::Frames::P4(f) => transform_frames(f),
	}
}
