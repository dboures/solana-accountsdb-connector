use crate::{
    chain_data::{AccountData, ChainData, SlotData},
    AccountWrite, MarketConfig, SlotUpdate,
};
use log::*;
use serde::{ser::SerializeStruct, Serialize, Serializer};
use solana_sdk::{
    account::{ReadableAccount, WritableAccount},
    clock::Epoch,
    pubkey::Pubkey,
};
use std::{cmp::max, collections::HashMap, mem::size_of};

use arrayref::array_ref;
use mango::queue::{AnyEvent, EventQueueHeader, EventType, FillEvent};

// struct Slots {
//     // non-rooted only
//     slots: HashMap<u64, SlotUpdate>,
//     newest_processed_slot: Option<u64>,
//     newest_rooted_slot: Option<u64>,
// }

// #[derive(Default)]
// struct SlotPreprocessing {
//     discard_duplicate: bool,
//     discard_old: bool,
//     new_processed_head: bool,
//     new_rooted_head: bool,
//     parent_update: bool,
// }

// impl Slots {
//     fn new() -> Self {
//         Self {
//             slots: HashMap::new(),
//             newest_processed_slot: None,
//             newest_rooted_slot: None,
//         }
//     }

//     fn add(&mut self, update: &SlotUpdate) -> SlotPreprocessing {
//         let mut result = SlotPreprocessing::default();

//         if let Some(previous) = self.slots.get_mut(&update.slot) {
//             if previous.status == update.status && previous.parent == update.parent {
//                 result.discard_duplicate = true;
//             }

//             previous.status = update.status;
//             if update.parent.is_some() && previous.parent != update.parent {
//                 previous.parent = update.parent;
//                 result.parent_update = true;
//             }
//         } else if update.slot > self.newest_rooted_slot.unwrap_or(0) {
//             self.slots.insert(update.slot, update.clone());
//         } else {
//             result.discard_old = true;
//         }

//         if update.status == SlotStatus::Rooted {
//             let old_slots: Vec<u64> = self
//                 .slots
//                 .keys()
//                 .filter(|s| **s <= update.slot)
//                 .copied()
//                 .collect();
//             for old_slot in old_slots {
//                 self.slots.remove(&old_slot);
//             }
//             if self.newest_rooted_slot.unwrap_or(0) < update.slot {
//                 self.newest_rooted_slot = Some(update.slot);
//                 result.new_rooted_head = true;
//             }
//         }

//         if self.newest_processed_slot.unwrap_or(0) < update.slot {
//             self.newest_processed_slot = Some(update.slot);
//             result.new_processed_head = true;
//         }

//         result
//     }
// }

#[derive(Clone, Copy, Debug, Serialize)]
pub enum FillUpdateStatus {
    New,
    Revoke,
}

#[derive(Clone, Debug)]

pub struct FillUpdate {
    pub event: FillEvent,
    pub status: FillUpdateStatus,
    pub market: String,
    pub queue: String,
}

impl Serialize for FillUpdate {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let event = base64::encode_config(bytemuck::bytes_of(&self.event), base64::STANDARD);
        let mut state = serializer.serialize_struct("FillUpdate", 4)?;
        state.serialize_field("market", &self.market)?;
        state.serialize_field("queue", &self.queue)?;
        state.serialize_field("status", &self.status)?;
        state.serialize_field("event", &event)?;

        state.end()
    }
}

#[derive(Clone, Debug)]
pub struct FillCheckpoint {
    pub market: String,
    pub queue: String,
    pub events: Vec<FillEvent>,
}

impl Serialize for FillCheckpoint {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let events: Vec<String> = self
            .events
            .iter()
            .map(|e| base64::encode_config(bytemuck::bytes_of(e), base64::STANDARD))
            .collect();
        let mut state = serializer.serialize_struct("FillUpdate", 3)?;
        state.serialize_field("market", &self.market)?;
        state.serialize_field("queue", &self.queue)?;
        state.serialize_field("events", &events)?;
        state.end()
    }
}

pub enum FillEventFilterMessage {
    Update(FillUpdate),
    Checkpoint(FillCheckpoint),
}

pub async fn init(
    markets: Vec<MarketConfig>,
) -> anyhow::Result<(
    async_channel::Sender<AccountWrite>,
    async_channel::Sender<SlotUpdate>,
    async_channel::Receiver<FillEventFilterMessage>,
)> {
    // The actual message may want to also contain a retry count, if it self-reinserts on failure?
    let (account_write_queue_sender, account_write_queue_receiver) =
        async_channel::unbounded::<AccountWrite>();

    // Slot updates flowing from the outside into the single processing thread. From
    // there they'll flow into the postgres sending thread.
    let (slot_queue_sender, slot_queue_receiver) = async_channel::unbounded::<SlotUpdate>();

    // Fill updates can be consumed by client connections, they contain all fills for all markets
    let (fill_update_sender, fill_update_receiver) =
        async_channel::unbounded::<FillEventFilterMessage>();

    let account_write_queue_receiver_c = account_write_queue_receiver.clone();

    // let mut slots = Slots::new();
    let mut chain = ChainData::new();
    let mut events_cache: HashMap<String, [AnyEvent; 256]> = HashMap::new();
    let mut seq_num_cache = HashMap::new();

    // update handling thread, reads both sloths and account updates
    tokio::spawn(async move {
        loop {
            tokio::select! {
                Ok(account_write) = account_write_queue_receiver_c.recv() => {
                    chain.update_account(
                        account_write.pubkey,
                        AccountData {
                            slot: account_write.slot,
                            account: WritableAccount::create(
                                account_write.lamports,
                                account_write.data.clone(),
                                account_write.owner,
                                account_write.executable,
                                account_write.rent_epoch as Epoch,
                            ),
                        },
                    );
                }
                Ok(slot_update) = slot_queue_receiver.recv() => {

                    // info!("slot update {:?} -> {} {:?}", slot_update.parent, slot_update.slot, slot_update.status);

                    // // Check if we already know about the slot, or it is outdated
                    // let slot_preprocessing = slots.add(&slot_update);
                    // if slot_preprocessing.discard_duplicate || slot_preprocessing.discard_old {
                    //     continue;
                    // }


                    chain.update_slot(SlotData {
                        slot: slot_update.slot,
                        parent: slot_update.parent,
                        status: slot_update.status,
                        chain: 0,
                    });

                }
            }

            for mkt in markets.iter() {
                // TODO: only if account was written to or slot was updated

                let mkt_pk = mkt.event_queue.parse::<Pubkey>().unwrap();

                match chain.account(&mkt_pk) {
                    Ok(account) => {
                        const HEADER_SIZE: usize = size_of::<EventQueueHeader>();
                        let header_data = array_ref![account.data(), 0, HEADER_SIZE];
                        let header: &EventQueueHeader = bytemuck::from_bytes(header_data);

                        const EVENT_SIZE: usize = 200; //size_of::<AnyEvent>();
                        const QUEUE_LEN: usize = 256; // (data.length - header_size) / EVENT_SIZE
                        const QUEUE_SIZE: usize = EVENT_SIZE * QUEUE_LEN;

                        let events_data = array_ref![account.data(), HEADER_SIZE, QUEUE_SIZE];
                        let events: &[AnyEvent; QUEUE_LEN] = bytemuck::from_bytes(events_data);

                        match seq_num_cache.get(&mkt.event_queue) {
                            Some(old_seq_num) => match events_cache.get(&mkt.event_queue) {
                                Some(old_events) => {
                                    let start_seq_num =
                                        max(*old_seq_num, header.seq_num) + 1 - QUEUE_LEN;

                                    let mut checkpoint = Vec::new();

                                    for off in 0..QUEUE_LEN {
                                        let idx = (start_seq_num + off) % QUEUE_LEN;

                                        // there are three possible cases:
                                        // 1) the event is past the old seq num, hence guaranteed new event
                                        // 2) the event is not matching the old event queue
                                        // 3) all other events are matching the old event queue
                                        // the order of these checks is important so they are exhaustive
                                        if start_seq_num + off > *old_seq_num {
                                            trace!(
                                                "found new event {} idx {} type {}",
                                                mkt.name,
                                                idx,
                                                events[idx].event_type as u32
                                            );

                                            // new fills are published and recorded in checkpoint
                                            if events[idx].event_type == EventType::Fill as u8 {
                                                let fill: FillEvent = bytemuck::cast(events[idx]);

                                                fill_update_sender
                                                    .try_send(FillEventFilterMessage::Update(
                                                        FillUpdate {
                                                            event: fill,
                                                            status: FillUpdateStatus::New,
                                                            market: mkt.name.clone(),
                                                            queue: mkt.event_queue.clone(),
                                                        },
                                                    ))
                                                    .unwrap(); // TODO: use anyhow to bubble up error
                                                checkpoint.push(fill);
                                            }
                                        } else if old_events[idx].event_type
                                            != events[idx].event_type
                                            || old_events[idx].padding != events[idx].padding
                                        {
                                            info!("found changed event {} idx {}", mkt.name, idx);

                                            // first revoke old event if a fill
                                            if old_events[idx].event_type == EventType::Fill as u8 {
                                                let fill: FillEvent =
                                                    bytemuck::cast(old_events[idx]);
                                                fill_update_sender
                                                    .try_send(FillEventFilterMessage::Update(
                                                        FillUpdate {
                                                            event: fill,
                                                            status: FillUpdateStatus::Revoke,
                                                            market: mkt.name.clone(),
                                                            queue: mkt.event_queue.clone(),
                                                        },
                                                    ))
                                                    .unwrap(); // TODO: use anyhow to bubble up error
                                            }

                                            // then publish new if its a fill and record in checkpoint
                                            if events[idx].event_type == EventType::Fill as u8 {
                                                let fill: FillEvent = bytemuck::cast(events[idx]);

                                                fill_update_sender
                                                    .try_send(FillEventFilterMessage::Update(
                                                        FillUpdate {
                                                            event: fill,
                                                            status: FillUpdateStatus::New,
                                                            market: mkt.name.clone(),
                                                            queue: mkt.event_queue.clone(),
                                                        },
                                                    ))
                                                    .unwrap(); // TODO: use anyhow to bubble up error
                                                checkpoint.push(fill);
                                            }
                                        } else {
                                            // every already published event is recorded in checkpoint if a fill
                                            if events[idx].event_type == EventType::Fill as u8 {
                                                let fill: FillEvent = bytemuck::cast(events[idx]);
                                                checkpoint.push(fill);
                                            }
                                        }
                                    }

                                    fill_update_sender
                                        .try_send(FillEventFilterMessage::Checkpoint(
                                            FillCheckpoint {
                                                events: checkpoint,
                                                market: mkt.name.clone(),
                                                queue: mkt.event_queue.clone(),
                                            },
                                        ))
                                        .unwrap()
                                }
                                _ => info!("events_cache could not find {}", mkt.event_queue),
                            },
                            _ => info!("seq_num_cache could not find {}", mkt.event_queue),
                        }

                        seq_num_cache.insert(mkt.event_queue.clone(), header.seq_num.clone());
                        events_cache.insert(mkt.event_queue.clone(), events.clone());

                        trace!("event queue {} sequence num {}", mkt.name, header.seq_num)
                    }
                    Err(_) => trace!("waiting for event queue {}", mkt.name),
                }
            }
        }
    });

    Ok((
        account_write_queue_sender,
        slot_queue_sender,
        fill_update_receiver,
    ))
}
