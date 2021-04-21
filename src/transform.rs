use peppi::{
	frame,
	frame::{
		StartCol,
		EndCol,
		PreCol,
		PostCol,
		ItemCol,
	},
	frame_data::{
		TransposeFrameCol,
		TransposeFrameRow,
		TransposePortCol,
		TransposePortRow,
		TransposeItemCol,
		TransposeItemRow,
	},
	game,
};

pub struct Port {
	pub pre: PreCol,
	pub post: PostCol,
}

pub struct Frames {
	pub leader: Port,
	pub follower: Port,
	pub start: Option<StartCol>,
	pub end: Option<EndCol>,
	pub item: Option<ItemCol>,
}

fn transform_port(src: &frame::Data, dst: &mut Port, i: usize) {
	src.pre.transpose(&mut dst.pre, i);
	src.post.transpose(&mut dst.post, i);
}

fn transpose_rows_<const N: usize>(src: &Vec<frame::Frame<N>>) -> Frames {
	let len = src.len();
	let dim = (N, len);

	let item_len = src.iter()
		.map(|f| f.items.as_ref()
			 .map(|i| i.len())
			 .unwrap_or(0))
		.sum();

	let first_item = {
		let mut first_item = None;
		for f in src {
			if let Some(items) = &f.items {
				if let Some(i) = items.iter().next() {
					first_item = Some(i);
					break;
				}
			}
		}
		first_item
	};

	let mut dst = Frames {
		start: src[0].start.map(|s| StartCol::new(len, s)),
		end: src[0].end.map(|e| EndCol::new(len, e)),
		leader: Port {
			pre: PreCol::new(dim, src[0].ports[0].leader.pre),
			post: PostCol::new(dim, src[0].ports[0].leader.post),
		},
		follower: Port {
			pre: PreCol::new(dim, src[0].ports[0].leader.pre),
			post: PostCol::new(dim, src[0].ports[0].leader.post),
		},
		item: first_item.map(|i| ItemCol::new(item_len, *i)),
	};

	for (f_idx, f) in src.iter().enumerate() {
		if let Some(ref mut start) = dst.start {
			f.start.unwrap().transpose(start);
		}

		if let Some(ref mut end) = dst.end {
			f.end.unwrap().transpose(end);
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
				src_item.transpose(dst_item, f_idx as i32 + game::FIRST_FRAME_INDEX);
			}
		}
	}

	dst
}

pub fn transpose_rows(frames: &game::Frames) -> Frames {
	use game::Frames::*;
	match &frames {
		P1(f) => transpose_rows_(f),
		P2(f) => transpose_rows_(f),
		P3(f) => transpose_rows_(f),
		P4(f) => transpose_rows_(f),
	}
}
