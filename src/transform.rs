use peppi::{frame, game, primitives::Direction};

pub const MAX_ITEMS: usize = 16;

pub struct Position {
	pub x: Vec<Vec<f32>>,
	pub y: Vec<Vec<f32>>,
}

pub struct Velocity {
	pub x: Vec<Vec<f32>>,
	pub y: Vec<Vec<f32>>,
}

pub struct Velocities {
	pub autogenous: Velocity,
	pub knockback: Velocity,
}

pub struct TriggersPhysical {
	pub l: Vec<Vec<f32>>,
	pub r: Vec<Vec<f32>>,
}

pub struct Triggers {
	pub logical: Vec<Vec<f32>>,
	pub physical: TriggersPhysical,
}

pub struct Buttons {
	pub logical: Vec<Vec<i32>>,
	pub physical: Vec<Vec<i32>>,
}

pub struct Start {
	pub random_seed: Vec<i32>,
}

pub struct EndV3_7 {
	pub latest_finalized_frame: Vec<i32>,
}

pub struct End {
	pub v3_7: Option<EndV3_7>,
}

pub struct PreV1_4 {
	pub damage: Vec<Vec<f32>>,
}

pub struct PreV1_2 {
	pub raw_analog_x: Vec<Vec<i32>>,
	pub v1_4: Option<PreV1_4>,
}

pub struct Pre {
	pub position: Position,
	pub direction: Vec<Vec<bool>>,
	pub joystick: Position,
	pub cstick: Position,
	pub triggers: Triggers,
	pub random_seed: Vec<Vec<i32>>,
	pub buttons: Buttons,
	pub state: Vec<Vec<i32>>,
	pub v1_2: Option<PreV1_2>,
}

pub struct PostV3_8 {
	pub hitlag: Vec<Vec<f32>>,
}

pub struct PostV3_5 {
	pub velocities: Velocities,
	pub v3_8: Option<PostV3_8>,
}

pub struct PostV2_1 {
	pub hurtbox_state: Vec<Vec<i32>>,
	pub v3_5: Option<PostV3_5>,
}

pub struct PostV2_0 {
	pub flags: Vec<Vec<i64>>,
	pub misc_as: Vec<Vec<f32>>,
	pub airborne: Vec<Vec<bool>>,
	pub ground: Vec<Vec<i32>>,
	pub jumps: Vec<Vec<i32>>,
	pub l_cancel: Vec<Vec<i32>>,
	pub v2_1: Option<PostV2_1>,
}

pub struct PostV0_2 {
	pub state_age: Vec<Vec<f32>>,
	pub v2_0: Option<PostV2_0>,
}

pub struct Post {
	pub position: Position,
	pub direction: Vec<Vec<bool>>,
	pub damage: Vec<Vec<f32>>,
	pub shield: Vec<Vec<f32>>,
	pub state: Vec<Vec<i32>>,
	pub character: Vec<Vec<i32>>,
	pub last_attack_landed: Vec<Vec<i32>>,
	pub combo_count: Vec<Vec<i32>>,
	pub last_hit_by: Vec<Vec<i32>>,
	pub stocks: Vec<Vec<i32>>,
	pub v0_2: Option<PostV0_2>,
}

pub struct ItemV3_6 {
	pub owner: Vec<Vec<i32>>,
}

pub struct ItemV3_2 {
	pub misc: Vec<Vec<i32>>,
	pub v3_6: Option<ItemV3_6>,
}

pub struct Item {
	pub id: Vec<Vec<i32>>,
	pub r#type: Vec<Vec<i32>>,
	pub state: Vec<Vec<i32>>,
	pub direction: Vec<Vec<bool>>,
	pub position: Position,
	pub velocity: Velocity,
	pub damage: Vec<Vec<i32>>,
	pub timer: Vec<Vec<f32>>,
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

fn vec2<X>(dim: (usize, usize)) -> Vec<Vec<X>> {
	let mut v = Vec::with_capacity(dim.0);
	for _ in 0 .. dim.0 {
		v.push(Vec::with_capacity(dim.1));
	}
	v
}

fn start(_dim: usize, _start: frame::Start) -> Start {
	Start {
		random_seed: Vec::new(),
	}
}

fn end_v3_7(_dim: usize, _end: frame::EndV3_7) -> EndV3_7 {
	EndV3_7 {
		latest_finalized_frame: Vec::new(),
	}
}

fn end(dim: usize, end: frame::End) -> End {
	End {
		v3_7: end.v3_7.map(|v3_7| end_v3_7(dim, v3_7)),
	}
}

fn pre_v1_4(dim: (usize, usize), _pre: frame::PreV1_4) -> PreV1_4 {
	PreV1_4 {
		damage: vec2(dim),
	}
}

fn pre_v1_2(dim: (usize, usize), pre: frame::PreV1_2) -> PreV1_2 {
	PreV1_2 {
		raw_analog_x: vec2(dim),
		v1_4: pre.v1_4.map(|v1_4| pre_v1_4(dim, v1_4)),
	}
}

fn pre<const N: usize>(frames: &Vec<frame::Frame<N>>) -> Pre {
	let dim = (N, frames.len());
	let f = frames[0].ports[0].leader.pre;
	Pre {
		position: Position {
			x: vec2(dim),
			y: vec2(dim),
		},
		direction: vec2(dim),
		joystick: Position {
			x: vec2(dim),
			y: vec2(dim),
		},
		cstick: Position {
			x: vec2(dim),
			y: vec2(dim),
		},
		triggers: Triggers {
			logical: vec2(dim),
			physical: TriggersPhysical {
				l: vec2(dim),
				r: vec2(dim),
			},
		},
		random_seed: vec2(dim),
		buttons: Buttons {
			logical: vec2(dim),
			physical: vec2(dim),
		},
		state: vec2(dim),
		v1_2: f.v1_2.map(|v1_2| pre_v1_2(dim, v1_2)),
	}
}

fn post_v3_8(dim: (usize, usize), _post: frame::PostV3_8) -> PostV3_8 {
	PostV3_8 {
		hitlag: vec2(dim),
	}
}

fn post_v3_5(dim: (usize, usize), post: frame::PostV3_5) -> PostV3_5 {
	PostV3_5 {
		velocities: Velocities {
			autogenous: Velocity {
				x: vec2(dim),
				y: vec2(dim),
			},
			knockback: Velocity {
				x: vec2(dim),
				y: vec2(dim),
			},
		},
		v3_8: post.v3_8.map(|v3_8| post_v3_8(dim, v3_8)),
	}
}

fn post_v2_1(dim: (usize, usize), post: frame::PostV2_1) -> PostV2_1 {
	PostV2_1 {
		hurtbox_state: vec2(dim),
		v3_5: post.v3_5.map(|v3_5| post_v3_5(dim, v3_5)),
	}
}

fn post_v2_0(dim: (usize, usize), post: frame::PostV2_0) -> PostV2_0 {
	PostV2_0 {
		flags: vec2(dim),
		misc_as: vec2(dim),
		airborne: vec2(dim),
		ground: vec2(dim),
		jumps: vec2(dim),
		l_cancel: vec2(dim),
		v2_1: post.v2_1.map(|v2_1| post_v2_1(dim, v2_1)),
	}
}

fn post_v0_2(dim: (usize, usize), post: frame::PostV0_2) -> PostV0_2 {
	PostV0_2 {
		state_age: vec2(dim),
		v2_0: post.v2_0.map(|v2_0| post_v2_0(dim, v2_0)),
	}
}

fn post<const N: usize>(frames: &Vec<frame::Frame<N>>) -> Post {
	let dim = (N, frames.len());
	let f = frames[0].ports[0].leader.post;
	Post {
		position: Position {
			x: vec2(dim),
			y: vec2(dim),
		},
		direction: vec2(dim),
		damage: vec2(dim),
		shield: vec2(dim),
		state: vec2(dim),
		character: vec2(dim),
		last_attack_landed: vec2(dim),
		combo_count: vec2(dim),
		last_hit_by: vec2(dim),
		stocks: vec2(dim),
		v0_2: f.v0_2.map(|v0_2| post_v0_2(dim, v0_2)),
	}
}

fn item_v3_6(dim: (usize, usize), _v3_6: frame::ItemV3_6) -> ItemV3_6 {
	ItemV3_6 {
		owner: vec2(dim),
	}
}

fn item_v3_2(dim: (usize, usize), v3_2: frame::ItemV3_2) -> ItemV3_2 {
	ItemV3_2 {
		misc: vec2(dim),
		v3_6: v3_2.v3_6.map(|v3_6| item_v3_6(dim, v3_6)),
	}
}

fn item(dim: (usize, usize), item: &frame::Item) -> Item {
	Item {
		id: vec2(dim),
		r#type: vec2(dim),
		state: vec2(dim),
		direction: vec2(dim),
		position: Position {
			x: vec2(dim),
			y: vec2(dim),
		},
		velocity: Velocity {
			x: vec2(dim),
			y: vec2(dim),
		},
		damage: vec2(dim),
		timer: vec2(dim),
		v3_2: item.v3_2.map(|v3_2| item_v3_2(dim, v3_2)),
	}
}

fn transform_pre_v1_4(src: &frame::PreV1_4, dst: &mut PreV1_4, i: usize) {
	dst.damage[i].push(src.damage);
}

fn transform_pre_v1_2(src: &frame::PreV1_2, dst: &mut PreV1_2, i: usize) {
	dst.raw_analog_x[i].push(src.raw_analog_x as i32);
	if let Some(ref mut dst) = dst.v1_4 {
		transform_pre_v1_4(&src.v1_4.unwrap(), dst, i);
	}
}

fn transform_pre(src: &frame::Pre, dst: &mut Pre, i: usize) {
	dst.position.x[i].push(src.position.x);
	dst.position.y[i].push(src.position.y);
	dst.direction[i].push(src.direction == Direction::Right);
	dst.joystick.x[i].push(src.joystick.x);
	dst.joystick.y[i].push(src.joystick.y);
	dst.cstick.x[i].push(src.cstick.x);
	dst.cstick.y[i].push(src.cstick.y);
	dst.triggers.logical[i].push(src.triggers.logical);
	dst.triggers.physical.l[i].push(src.triggers.physical.l);
	dst.triggers.physical.r[i].push(src.triggers.physical.r);
	dst.random_seed[i].push(src.random_seed as i32);
	dst.buttons.logical[i].push(src.buttons.logical.0 as i32);
	dst.buttons.physical[i].push(src.buttons.physical.0 as i32);
	dst.state[i].push({
		let s: u16 = src.state.into();
		s as i32
	});
	if let Some(ref mut dst) = dst.v1_2 {
		transform_pre_v1_2(&src.v1_2.unwrap(), dst, i);
	}
}

fn transform_post_v3_8(src: &frame::PostV3_8, dst: &mut PostV3_8, i: usize) {
	dst.hitlag[i].push(src.hitlag);
}

fn transform_post_v3_5(src: &frame::PostV3_5, dst: &mut PostV3_5, i: usize) {
	dst.velocities.autogenous.x[i].push(src.velocities.autogenous.x);
	dst.velocities.autogenous.y[i].push(src.velocities.autogenous.y);
	dst.velocities.knockback.x[i].push(src.velocities.knockback.x);
	dst.velocities.knockback.y[i].push(src.velocities.knockback.y);
	if let Some(ref mut dst) = dst.v3_8 {
		transform_post_v3_8(&src.v3_8.unwrap(), dst, i);
	}
}

fn transform_post_v2_1(src: &frame::PostV2_1, dst: &mut PostV2_1, i: usize) {
	dst.hurtbox_state[i].push(src.hurtbox_state.0 as i32);
	if let Some(ref mut dst) = dst.v3_5 {
		transform_post_v3_5(&src.v3_5.unwrap(), dst, i);
	}
}

fn transform_post_v2_0(src: &frame::PostV2_0, dst: &mut PostV2_0, i: usize) {
	dst.flags[i].push(src.flags.0 as i64);
	dst.misc_as[i].push(src.misc_as);
	dst.airborne[i].push(src.airborne);
	dst.ground[i].push(src.ground as i32);
	dst.jumps[i].push(src.jumps as i32);
	dst.l_cancel[i].push(match src.l_cancel {
		None => 0,
		Some(true) => 1,
		Some(false) => 2,
	});
	if let Some(ref mut dst) = dst.v2_1 {
		transform_post_v2_1(&src.v2_1.unwrap(), dst, i);
	}
}

fn transform_post_v0_2(src: &frame::PostV0_2, dst: &mut PostV0_2, i: usize) {
	dst.state_age[i].push(src.state_age);
	if let Some(ref mut dst) = dst.v2_0 {
		transform_post_v2_0(&src.v2_0.unwrap(), dst, i);
	}
}

fn transform_post(src: &frame::Post, dst: &mut Post, i: usize) {
	dst.position.x[i].push(src.position.x);
	dst.position.y[i].push(src.position.y);
	dst.direction[i].push(src.direction == Direction::Right);
	dst.damage[i].push(src.damage);
	dst.shield[i].push(src.shield);
	dst.state[i].push({
		let s: u16 = src.state.into();
		s as i32
	});
	dst.character[i].push(src.character.0 as i32);
	dst.last_attack_landed[i].push(match src.last_attack_landed {
		Some(a) => a.0 as i32,
		_ => 0,
	});
	dst.combo_count[i].push(src.combo_count as i32);
	dst.last_hit_by[i].push(match src.last_hit_by {
		Some(l) => l as i32,
		_ => u8::MAX as i32,
	});
	dst.stocks[i].push(src.stocks as i32);
	if let Some(ref mut dst) = dst.v0_2 {
		transform_post_v0_2(&src.v0_2.unwrap(), dst, i);
	}
}

fn transform_port(src: &frame::Data, dst: &mut Port, i: usize) {
	transform_pre(&src.pre, &mut dst.pre, i);
	transform_post(&src.post, &mut dst.post, i);
}

fn transform_item_v3_6(src: &frame::ItemV3_6, dst: &mut ItemV3_6, i: usize) {
	dst.owner[i].push(match src.owner {
		Some(o) => o as i32,
		_ => u8::MAX as i32,
	});
}

fn transform_item_v3_2(src: &frame::ItemV3_2, dst: &mut ItemV3_2, i: usize) {
	dst.misc[i].push(u32::from_le_bytes(src.misc) as i32);
	if let Some(ref mut dst) = dst.v3_6 {
		transform_item_v3_6(&src.v3_6.unwrap(), dst, i);
	}
}

fn transform_item(src: &frame::Item, dst: &mut Item, i: usize) {
	dst.id[i].push(src.id as i32);
	dst.r#type[i].push(src.r#type.0 as i32);
	dst.state[i].push(src.state as i32);
	dst.direction[i].push(src.direction == Direction::Right);
	dst.position.x[i].push(src.position.x);
	dst.position.y[i].push(src.position.y);
	dst.velocity.x[i].push(src.velocity.x);
	dst.velocity.y[i].push(src.velocity.y);
	dst.damage[i].push(src.damage as i32);
	dst.timer[i].push(src.timer);
	if let Some(ref mut dst) = dst.v3_2 {
		transform_item_v3_2(&src.v3_2.unwrap(), dst, i);
	}
}

fn transform_start(src: &frame::Start, dst: &mut Start) {
	dst.random_seed.push(src.random_seed as i32);
}

fn transform_end_v3_7(src: &frame::EndV3_7, dst: &mut EndV3_7) {
	dst.latest_finalized_frame.push(src.latest_finalized_frame);
}

fn transform_end(src: &frame::End, dst: &mut End) {
	if let Some(ref mut dst) = dst.v3_7 {
		transform_end_v3_7(&src.v3_7.unwrap(), dst);
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
			transform_start(&f.start.unwrap(), start);
		}

		if let Some(ref mut end) = dst.end {
			transform_end(&f.end.unwrap(), end);
		}

		for (p_idx, p) in f.ports.iter().enumerate() {
			transform_port(&p.leader, &mut dst.leader, p_idx);
			if let Some(follower) = &p.follower {
				transform_port(&follower, &mut dst.follower, p_idx);
			}
		}

		if let Some(ref mut dst_item) = dst.item {
			let items = f.items.as_ref().unwrap();
			for src_item in items.iter() {
				transform_item(&src_item, dst_item, f_idx);
			}
		}
	}

	dst
}

pub fn transform(frames: &game::Frames) -> Frames {
	match &frames {
		game::Frames::P1(f) => transform_frames(f),
		game::Frames::P2(f) => transform_frames(f),
		game::Frames::P3(f) => transform_frames(f),
		game::Frames::P4(f) => transform_frames(f),
	}
}
