// This file is part of Substrate.

// Copyright (C) Parity Technologies (UK) Ltd.
// SPDX-License-Identifier: GPL-3.0-or-later WITH Classpath-exception-2.0

// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with this program. If not, see <https://www.gnu.org/licenses/>.

use crate::{
	litep2p::{
		peerstore::peerstore_handle_test,
		shim::notification::peerset::{
			Direction, PeerState, Peerset, PeersetCommand, PeersetNotificationCommand, Reserved,
		},
	},
	service::traits::{self, ValidationResult},
	ProtocolName,
};

use futures::prelude::*;

use sc_network_types::PeerId;

use std::collections::HashSet;

// outbound substream was initiated for a peer but an inbound substream from that same peer
// was receied while the `Peerset` was waiting for the outbound substream to be opened
//
// verify that the peer state is updated correctly
#[tokio::test]
async fn inbound_substream_for_outbound_peer() {
	let peerstore_handle = peerstore_handle_test();
	let peers = vec![PeerId::random(), PeerId::random(), PeerId::random()];
	let inbound_peer = peers.iter().next().unwrap().clone();

	let (mut peerset, _to_peerset) = Peerset::new(
		ProtocolName::from("/notif/1"),
		25,
		25,
		false,
		Default::default(),
		Default::default(),
		peerstore_handle,
	);
	assert_eq!(peerset.num_in(), 0usize);
	assert_eq!(peerset.num_out(), 0usize);

	match peerset.next().await {
		Some(PeersetNotificationCommand::OpenSubstream { peers: out_peers }) => {
			assert_eq!(out_peers.len(), 3usize);
			assert_eq!(peerset.num_in(), 0usize);
			assert_eq!(peerset.num_out(), 3usize);
			assert_eq!(
				peerset.peers().get(&inbound_peer),
				Some(&PeerState::Opening { direction: Direction::Outbound(Reserved::No) })
			);
		},
		event => panic!("invalid event: {event:?}"),
	}

	// inbound substream was received from peer who was marked outbound
	//
	// verify that the peer state and inbound/outbound counts are updated correctly
	assert_eq!(peerset.report_inbound_substream(inbound_peer), ValidationResult::Accept);
	assert_eq!(peerset.num_in(), 1usize);
	assert_eq!(peerset.num_out(), 2usize);
	assert_eq!(
		peerset.peers().get(&inbound_peer),
		Some(&PeerState::Opening { direction: Direction::Inbound(Reserved::No) })
	);
}

// substream was opening to peer but then it was canceled and before the substream
// was fully closed, the peer got banned
#[tokio::test]
async fn canceled_peer_gets_banned() {
	sp_tracing::try_init_simple();

	let peerstore_handle = peerstore_handle_test();
	let peers = HashSet::from_iter([PeerId::random(), PeerId::random(), PeerId::random()]);

	let (mut peerset, to_peerset) = Peerset::new(
		ProtocolName::from("/notif/1"),
		25,
		25,
		true,
		peers.clone(),
		Default::default(),
		peerstore_handle,
	);
	assert_eq!(peerset.num_in(), 0usize);
	assert_eq!(peerset.num_out(), 0usize);

	match peerset.next().await {
		Some(PeersetNotificationCommand::OpenSubstream { peers: out_peers }) => {
			assert_eq!(peerset.num_in(), 0usize);
			assert_eq!(peerset.num_out(), 0usize);

			for outbound_peer in &out_peers {
				assert!(peers.contains(outbound_peer));
				assert_eq!(
					peerset.peers().get(&outbound_peer),
					Some(&PeerState::Opening { direction: Direction::Outbound(Reserved::Yes) })
				);
			}
		},
		event => panic!("invalid event: {event:?}"),
	}

	// remove all reserved peers
	to_peerset
		.unbounded_send(PeersetCommand::RemoveReservedPeers { peers: peers.clone() })
		.unwrap();

	match peerset.next().await {
		Some(PeersetNotificationCommand::CloseSubstream { peers: out_peers }) => {
			assert!(out_peers.is_empty());
		},
		event => panic!("invalid event: {event:?}"),
	}

	// verify all reserved peers are canceled
	for (_, state) in peerset.peers() {
		assert_eq!(state, &PeerState::Canceled { direction: Direction::Outbound(Reserved::Yes) });
	}
}

// TODO: explain
#[tokio::test]
async fn peer_added_and_removed_from_peerset() {
	sp_tracing::try_init_simple();

	let peerstore_handle = peerstore_handle_test();
	let (mut peerset, to_peerset) = Peerset::new(
		ProtocolName::from("/notif/1"),
		25,
		25,
		true,
		Default::default(),
		Default::default(),
		peerstore_handle,
	);
	assert_eq!(peerset.num_in(), 0usize);
	assert_eq!(peerset.num_out(), 0usize);

	// add peers to reserved set
	let peers = HashSet::from_iter([PeerId::random(), PeerId::random(), PeerId::random()]);
	to_peerset
		.unbounded_send(PeersetCommand::AddReservedPeers { peers: peers.clone() })
		.unwrap();

	match peerset.next().await {
		Some(PeersetNotificationCommand::OpenSubstream { peers: out_peers }) => {
			assert_eq!(peerset.num_in(), 0usize);
			assert_eq!(peerset.num_out(), 0usize);

			for outbound_peer in &out_peers {
				assert!(peers.contains(outbound_peer));
				assert!(peerset.reserved_peers().contains(outbound_peer));
				assert_eq!(
					peerset.peers().get(&outbound_peer),
					Some(&PeerState::Opening { direction: Direction::Outbound(Reserved::Yes) })
				);
			}
		},
		event => panic!("invalid event: {event:?}"),
	}

	// report that all substreams were opened
	for peer in &peers {
		assert!(peerset.report_substream_opened(*peer, traits::Direction::Outbound));
		assert_eq!(
			peerset.peers().get(peer),
			Some(&PeerState::Connected { direction: Direction::Outbound(Reserved::Yes) })
		);
	}

	// remove all reserved peers
	to_peerset
		.unbounded_send(PeersetCommand::RemoveReservedPeers { peers: peers.clone() })
		.unwrap();

	match peerset.next().await {
		Some(PeersetNotificationCommand::CloseSubstream { peers: out_peers }) => {
			assert!(!out_peers.is_empty());

			for peer in &out_peers {
				assert!(peers.contains(peer));
				assert!(!peerset.reserved_peers().contains(peer));
				assert_eq!(
					peerset.peers().get(peer),
					Some(&PeerState::Closing { direction: Direction::Outbound(Reserved::Yes) }),
				);
			}
		},
		event => panic!("invalid event: {event:?}"),
	}

	// add the peers again and verify that the command is ignored because the substreams are closing
	to_peerset
		.unbounded_send(PeersetCommand::AddReservedPeers { peers: peers.clone() })
		.unwrap();

	match peerset.next().await {
		Some(PeersetNotificationCommand::OpenSubstream { peers: out_peers }) => {
			assert!(out_peers.is_empty());

			for peer in &peers {
				assert!(peerset.reserved_peers().contains(peer));
				assert_eq!(
					peerset.peers().get(peer),
					Some(&PeerState::Closing { direction: Direction::Outbound(Reserved::Yes) }),
				);
			}
		},
		event => panic!("invalid event: {event:?}"),
	}

	// remove the peers again and verify the state remains as `Closing`
	to_peerset
		.unbounded_send(PeersetCommand::RemoveReservedPeers { peers: peers.clone() })
		.unwrap();

	match peerset.next().await {
		Some(PeersetNotificationCommand::CloseSubstream { peers: out_peers }) => {
			assert!(out_peers.is_empty());

			for peer in &peers {
				assert!(!peerset.reserved_peers().contains(peer));
				assert_eq!(
					peerset.peers().get(peer),
					Some(&PeerState::Closing { direction: Direction::Outbound(Reserved::Yes) }),
				);
			}
		},
		event => panic!("invalid event: {event:?}"),
	}
}
