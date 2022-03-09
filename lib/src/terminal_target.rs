use crate::{
    chain_data::{AccountData, ChainData, SlotData},
    metrics, AccountWrite, SlotStatus, SlotUpdate,
};
use log::*;
use solana_sdk::{
    account::{AccountSharedData, WritableAccount},
    clock::Epoch,
};
use std::{collections::HashMap, sync::Arc};

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

pub async fn init() -> anyhow::Result<(
    async_channel::Sender<AccountWrite>,
    async_channel::Sender<SlotUpdate>,
)> {
    // The actual message may want to also contain a retry count, if it self-reinserts on failure?
    let (account_write_queue_sender, account_write_queue_receiver) =
        async_channel::unbounded::<AccountWrite>();

    // Slot updates flowing from the outside into the single processing thread. From
    // there they'll flow into the postgres sending thread.
    let (slot_queue_sender, slot_queue_receiver) = async_channel::unbounded::<SlotUpdate>();

    let account_write_queue_receiver_c = account_write_queue_receiver.clone();

    let mut chain = ChainData::new();

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
            })
        }
    });

    Ok((account_write_queue_sender, slot_queue_sender))
}
