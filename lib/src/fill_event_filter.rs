use crate::{
    chain_data::{AccountData, ChainData, SlotData},
    AccountWrite, MarketConfig, SlotStatus, SlotUpdate,
};
use log::*;
use serde::Serialize;
use solana_sdk::{
    account::{ReadableAccount, WritableAccount},
    clock::Epoch,
    pubkey::Pubkey,
};
use std::{
    cmp::max,
    collections::HashMap,
    mem::{self, size_of, ManuallyDrop},
};

use arrayref::array_ref;
use mango::queue::{AnyEvent, EventQueueHeader, EventType, FillEvent};

struct Slots {
    // non-rooted only
    slots: HashMap<u64, SlotUpdate>,
    newest_processed_slot: Option<u64>,
    newest_rooted_slot: Option<u64>,
}

#[derive(Default)]
struct SlotPreprocessing {
    discard_duplicate: bool,
    discard_old: bool,
    new_processed_head: bool,
    new_rooted_head: bool,
    parent_update: bool,
}

impl Slots {
    fn new() -> Self {
        Self {
            slots: HashMap::new(),
            newest_processed_slot: None,
            newest_rooted_slot: None,
        }
    }

    fn add(&mut self, update: &SlotUpdate) -> SlotPreprocessing {
        let mut result = SlotPreprocessing::default();

        if let Some(previous) = self.slots.get_mut(&update.slot) {
            if previous.status == update.status && previous.parent == update.parent {
                result.discard_duplicate = true;
            }

            previous.status = update.status;
            if update.parent.is_some() && previous.parent != update.parent {
                previous.parent = update.parent;
                result.parent_update = true;
            }
        } else if update.slot > self.newest_rooted_slot.unwrap_or(0) {
            self.slots.insert(update.slot, update.clone());
        } else {
            result.discard_old = true;
        }

        if update.status == SlotStatus::Rooted {
            let old_slots: Vec<u64> = self
                .slots
                .keys()
                .filter(|s| **s <= update.slot)
                .copied()
                .collect();
            for old_slot in old_slots {
                self.slots.remove(&old_slot);
            }
            if self.newest_rooted_slot.unwrap_or(0) < update.slot {
                self.newest_rooted_slot = Some(update.slot);
                result.new_rooted_head = true;
            }
        }

        if self.newest_processed_slot.unwrap_or(0) < update.slot {
            self.newest_processed_slot = Some(update.slot);
            result.new_processed_head = true;
        }

        result
    }
}

#[derive(Clone, Copy, Debug, Serialize)]
pub enum FillUpdateStatus {
    New,
    Revoke,
}

#[derive(Serialize)]
pub struct FillUpdate {
    pub event: FillEvent,
    pub status: FillUpdateStatus,
    pub queue: String,
}

pub struct FillCheckpoint {
    pub queue: String,
    pub events: Vec<FillEvent>,
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

    let mut chain = ChainData::new();

    let mut events_cache: HashMap<String, [AnyEvent; 256]> = HashMap::new();

    let mut seq_num_cache = HashMap::new();

    // update handling thread, reads both sloths and account updates
    tokio::spawn(async move {
        let mut slots = Slots::new();

        loop {
            // Retrieve up to batch_size account writes
            let mut write_batch = Vec::new();
            write_batch.push(
                account_write_queue_receiver_c
                    .recv()
                    .await
                    .expect("sender must stay alive"),
            );
            loop {
                match account_write_queue_receiver_c.try_recv() {
                    Ok(write) => write_batch.push(write),
                    Err(async_channel::TryRecvError::Empty) => break,
                    Err(async_channel::TryRecvError::Closed) => {
                        panic!("sender must stay alive")
                    }
                };
            }

            info!(
                "account write, batch {}, channel size {}",
                write_batch.len(),
                account_write_queue_receiver_c.len(),
            );

            for write in write_batch.iter() {
                trace!("write {}", write.pubkey.to_string());
                chain.update_account(
                    write.pubkey,
                    AccountData {
                        slot: write.slot,
                        account: WritableAccount::create(
                            write.lamports,
                            write.data.clone(),
                            write.owner,
                            write.executable,
                            write.rent_epoch as Epoch,
                        ),
                    },
                );
            }

            let update = slot_queue_receiver
                .recv()
                .await
                .expect("sender must stay alive");
            info!(
                "slot update {}, channel size {}",
                update.slot,
                slot_queue_receiver.len()
            );

            // Check if we already know about the slot, or it is outdated
            let slot_preprocessing = slots.add(&update);
            if slot_preprocessing.discard_duplicate || slot_preprocessing.discard_old {
                continue;
            }

            info!("newest slot {}", update.slot);
            chain.update_slot(SlotData {
                slot: update.slot,
                parent: update.parent,
                status: update.status,
                chain: 0,
            });

            for mkt in markets.iter() {
                // TODO: only if account was written to or slot was updated

                let mkt_pk = mkt.event_queue.parse::<Pubkey>().unwrap();

                match chain.account(&mkt_pk) {
                    Ok(account) => unsafe {
                        const header_size: usize = size_of::<EventQueueHeader>();
                        let header_data = array_ref![account.data(), 0, header_size];
                        let header =
                            mem::transmute::<&[u8; header_size], &EventQueueHeader>(header_data);

                        const event_size: usize = size_of::<AnyEvent>();
                        const queue_len: usize = 256; // (data.length - header_size) / event_size
                        const queue_size: usize = event_size * queue_len;

                        let events_data = array_ref![account.data(), header_size, queue_size];

                        let events = mem::transmute::<&[u8; queue_size], &[AnyEvent; queue_len]>(
                            events_data,
                        );

                        match seq_num_cache.get(&mkt.event_queue) {
                            Some(old_seq_num) => match events_cache.get(&mkt.event_queue) {
                                Some(old_events) => {
                                    let start_seq_num =
                                        max(*old_seq_num, header.seq_num) + 1 - queue_len;

                                    let mut checkpoint = Vec::new();

                                    for off in 0..queue_len {
                                        let idx = (start_seq_num + off) % queue_len;

                                        // there are three possible cases:
                                        // 1) the event is past the old seq num, hence guaranteed new event
                                        // 2) the event is not matching the old event queue
                                        // 3) all other events are matching the old event queue
                                        // the order of these checks is important so they are exhaustive
                                        if start_seq_num + off > *old_seq_num {
                                            // new fills are published and recorded in checkpoint
                                            info!("found new event {} idx {}", mkt.name, idx);
                                            if events[idx].event_type == EventType::Fill as u8 {
                                                let fill = mem::transmute::<&AnyEvent, &FillEvent>(
                                                    &events[idx],
                                                );
                                                fill_update_sender
                                                    .send(FillEventFilterMessage::Update(
                                                        FillUpdate {
                                                            event: fill.clone(),
                                                            status: FillUpdateStatus::New,
                                                            queue: mkt.event_queue.clone(),
                                                        }
                                                    ))
                                                    .await
                                                    .unwrap(); // TODO: use anyhow to bubble up error
                                                checkpoint.push(*fill);
                                            }
                                        } else if old_events[idx].event_type
                                            != events[idx].event_type
                                            && old_events[idx].padding != events[idx].padding
                                        {
                                            info!("found changed event {} idx {}", mkt.name, idx);

                                            // first revoke old event if a fill
                                            if old_events[idx].event_type == EventType::Fill as u8 {
                                                let fill = mem::transmute::<&AnyEvent, &FillEvent>(
                                                    &old_events[idx],
                                                );
                                                fill_update_sender
                                                    .send(FillEventFilterMessage::Update(
                                                        FillUpdate {
                                                            event: fill.clone(),
                                                            status: FillUpdateStatus::Revoke,
                                                            queue: mkt.event_queue.clone(),
                                                        }
                                                    ))
                                                    .await
                                                    .unwrap(); // TODO: use anyhow to bubble up error
                                            }

                                            // then publish new if its a fill and record in checkpoint
                                            if events[idx].event_type == EventType::Fill as u8 {
                                                let fill = mem::transmute::<&AnyEvent, &FillEvent>(
                                                    &events[idx],
                                                );
                                                fill_update_sender
                                                    .send(FillEventFilterMessage::Update(
                                                        FillUpdate {
                                                            event: fill.clone(),
                                                            status: FillUpdateStatus::New,
                                                            queue: mkt.event_queue.clone(),
                                                        }
                                                    ))
                                                    .await
                                                    .unwrap(); // TODO: use anyhow to bubble up error
                                                checkpoint.push(*fill);
                                            }
                                        } else {
                                            // every already published event is recorded in checkpoint if a fill
                                            if events[idx].event_type == EventType::Fill as u8 {
                                                let fill = mem::transmute::<&AnyEvent, &FillEvent>(
                                                    &events[idx],
                                                );
                                                checkpoint.push(*fill);
                                            }
                                        }
                                    }

                                    fill_update_sender
                                        .send(FillEventFilterMessage::Checkpoint(
                                            FillCheckpoint {
                                                events: checkpoint,
                                                queue: mkt.event_queue.clone(),
                                            }
                                        ))
                                        .await
                                        .unwrap()
                                }
                                _ => info!("events_cache could not find {}", mkt.event_queue),
                            },
                            _ => info!("seq_num_cache could not find {}", mkt.event_queue),
                        }

                        seq_num_cache.insert(mkt.event_queue.clone(), header.seq_num.clone());
                        events_cache.insert(mkt.event_queue.clone(), events.clone());

                        info!("event queue {} sequence num {}", mkt.name, header.seq_num)
                    },
                    Err(_) => info!("waiting for event queue {}", mkt.name),
                }
            }
            // get event queue
            // chain.account(pubkey)
        }
    });

    Ok((
        account_write_queue_sender,
        slot_queue_sender,
        fill_update_receiver,
    ))
}
