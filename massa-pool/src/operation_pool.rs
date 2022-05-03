// Copyright (c) 2022 MASSA LABS <info@massa.net>

use crate::{settings::PoolConfig, PoolError};
use massa_models::prehash::{Map, Set};
use massa_models::SerializeCompact;
use massa_models::{
    Address, OperationId, OperationSearchResult, OperationSearchResultStatus, SignedOperation, Slot,
};
use massa_storage::Storage;
use num::rational::Ratio;
use std::{collections::BTreeSet, usize};

struct OperationIndex(Map<Address, Set<OperationId>>);

impl OperationIndex {
    fn new() -> OperationIndex {
        OperationIndex(Map::default())
    }
    fn insert_op(&mut self, addr: Address, op_id: OperationId) {
        self.0
            .entry(addr)
            .or_insert_with(Set::<OperationId>::default)
            .insert(op_id);
    }

    fn get_ops_for_address(&self, address: &Address) -> Option<&Set<OperationId>> {
        self.0.get(address)
    }

    fn remove_op_for_address(&mut self, address: &Address, op_id: &OperationId) {
        if let Some(old) = self.0.get_mut(address) {
            old.remove(op_id);
            if old.is_empty() {
                self.0.remove(address);
            }
        }
    }
}

struct WrappedOperation {
    byte_count: u64,
    thread: u8,
    expire_period: u64,
    ledger_involved_addresses: Set<Address>,
    /// The priority of the operation based on how much it profits the block producer
    /// vs how much space it takes in the block
    fee_density: Ratio<u64>,
}

impl WrappedOperation {
    fn new(
        thread: u8,
        byte_count: u64,
        expire_period: u64,
        ledger_involved_addresses: Set<Address>,
        fee_density: Ratio<u64>,
    ) -> Result<Self, PoolError> {
        Ok(WrappedOperation {
            byte_count,
            thread,
            expire_period,
            ledger_involved_addresses,
            fee_density,
        })
    }
}

pub struct OperationPool {
    ops: Map<OperationId, WrappedOperation>,
    /// one vector per thread
    ops_by_thread_and_interest:
        Vec<BTreeSet<(std::cmp::Reverse<num::rational::Ratio<u64>>, OperationId)>>, // [thread][order by: (rev rentability, OperationId)]
    /// Maps Address -> Op id
    ops_by_address: OperationIndex,
    /// latest final blocks periods
    last_final_periods: Vec<u64>,
    /// current slot
    current_slot: Option<Slot>,
    /// configuration
    cfg: &'static PoolConfig,
    /// ids of operations that are final with expire period and thread
    final_operations: Map<OperationId, (u64, u8)>,
    /// Shared storage.
    storage: Storage,
}

impl OperationPool {
    pub fn new(cfg: &'static PoolConfig, storage: Storage) -> OperationPool {
        OperationPool {
            ops: Default::default(),
            ops_by_thread_and_interest: vec![BTreeSet::new(); cfg.thread_count as usize],
            current_slot: None,
            last_final_periods: vec![0; cfg.thread_count as usize],
            cfg,
            final_operations: Default::default(),
            ops_by_address: OperationIndex::new(),
            storage,
        }
    }

    /// Add operations received via the API.
    /// Returns newly added.
    pub fn add_operations(
        &mut self,
        operations: Map<OperationId, SignedOperation>,
    ) -> Result<Set<OperationId>, PoolError> {
        let operation_ids = operations.keys().cloned().collect();

        // Add to shared storage.
        for (operation_id, operation) in operations {
            let serialized = operation.to_bytes_compact()?;
            self.storage
                .store_operation(operation_id, operation, serialized);
        }

        // Process the newly added operations.
        self.process_operations(operation_ids)
    }

    /// Process incoming operations.
    /// Returns newly added.
    pub fn process_operations(
        &mut self,
        operations: Set<OperationId>,
    ) -> Result<Set<OperationId>, PoolError> {
        let mut newly_added = Set::<OperationId>::default();

        for op_id in operations.into_iter() {
            massa_trace!("pool add_operations op", { "op_id": op_id });

            // Already present
            if self.ops.contains_key(&op_id) {
                massa_trace!("pool add_operations op already present", {});
                continue;
            }

            // already final
            if self.final_operations.contains_key(&op_id) {
                massa_trace!("pool add_operations op already final", {});
                continue;
            }

            // wrap
            let (wrapped_op, validity_start_period) = {
                let stored_operation =
                    self.storage.retrieve_operation(&op_id).ok_or_else(|| {
                        PoolError::MissingOperation(format!(
                            "missing operation when adding it to the pool: {}",
                            op_id
                        ))
                    })?;
                let stored_operation = stored_operation.read();

                let byte_count = stored_operation.serialized.len() as u64;

                // Fee density
                // add inclusion fee and gas fees
                let total_return = stored_operation
                    .operation
                    .content
                    .fee
                    .saturating_add(stored_operation.operation.content.get_gas_coins());
                // return ratio with size
                let fee_density = Ratio::new(total_return.to_raw(), byte_count);
                let thread =
                    Address::from_public_key(&stored_operation.operation.content.sender_public_key)
                        .get_thread(self.cfg.thread_count);
                let ledger_involved_addresses = stored_operation
                    .operation
                    .content
                    .get_ledger_involved_addresses()?;
                let wrapped = WrappedOperation::new(
                    thread,
                    byte_count,
                    stored_operation.operation.content.expire_period,
                    ledger_involved_addresses,
                    fee_density,
                )?;
                let validity_start_period = stored_operation
                    .operation
                    .content
                    .get_validity_range(self.cfg.operation_validity_periods)
                    .start()
                    .clone();
                (wrapped, validity_start_period)
            };

            // check if too much in the future
            if let Some(cur_slot) = self.current_slot {
                let cur_period_in_thread = if cur_slot.thread >= wrapped_op.thread {
                    cur_slot.period
                } else {
                    cur_slot.period.saturating_sub(1)
                };

                if validity_start_period.saturating_sub(cur_period_in_thread)
                    > self
                        .cfg
                        .settings
                        .max_operation_future_validity_start_periods
                {
                    massa_trace!("pool add_operations validity_start_period >  self.cfg.max_operation_future_validity_start_periods", {
                        "range": validity_start_period.saturating_sub(cur_period_in_thread),
                        "max_operation_future_validity_start_periods": self.cfg.settings.max_operation_future_validity_start_periods
                    });
                    continue;
                }
            }

            // check if expired
            if wrapped_op.expire_period <= self.last_final_periods[wrapped_op.thread as usize] {
                massa_trace!("pool add_operations wrapped_op.expire_period <= self.last_final_periods[wrapped_op.thread as usize]", {
                    "expire_period": wrapped_op.expire_period,
                    "self.last_final_periods[wrapped_op.thread as usize]": self.last_final_periods[wrapped_op.thread as usize]
                });
                continue;
            }

            // insert
            let interest = (std::cmp::Reverse(wrapped_op.fee_density), op_id);

            self.ops_by_thread_and_interest[wrapped_op.thread as usize].insert(interest);
            wrapped_op
                .ledger_involved_addresses
                .iter()
                .for_each(|addr| {
                    self.ops_by_address.insert_op(*addr, op_id);
                });
            self.ops.insert(op_id, wrapped_op);
            newly_added.insert(op_id);
        }

        // remove excess operations if pool is full
        for thread in 0..self.cfg.thread_count {
            while self.ops_by_thread_and_interest[thread as usize].len()
                > self.cfg.settings.max_pool_size_per_thread as usize
            {
                let (_removed_rentability, removed_id) = self.ops_by_thread_and_interest
                    [thread as usize]
                    .pop_last()
                    .unwrap(); // will not panic because of the while condition. complexity = log or better
                if let Some(removed_op) = self.ops.remove(&removed_id) {
                    // complexity: const
                    for addr in removed_op.ledger_involved_addresses {
                        self.ops_by_address
                            .remove_op_for_address(&addr, &removed_id);
                    }
                }
                newly_added.remove(&removed_id);
            }
        }

        Ok(newly_added)
    }

    pub fn new_final_operations(
        &mut self,
        ops: Map<OperationId, (u64, u8)>,
    ) -> Result<(), PoolError> {
        for (id, _) in ops.iter() {
            if let Some(wrapped) = self.ops.remove(id) {
                self.ops_by_thread_and_interest[wrapped.thread as usize]
                    .remove(&(std::cmp::Reverse(wrapped.fee_density), *id));
                for addr in wrapped.ledger_involved_addresses {
                    self.ops_by_address.remove_op_for_address(&addr, id);
                }
            } // else final op wasn't in pool.
        }
        self.final_operations.extend(ops);
        Ok(())
    }

    pub fn update_current_slot(&mut self, slot: Slot) {
        self.current_slot = Some(slot);
    }

    pub fn len(&self) -> usize {
        self.ops.len()
    }

    fn prune(&mut self) -> Result<(), PoolError> {
        let ids = self
            .ops
            .iter()
            .filter(|(_id, w_op)| {
                w_op.expire_period <= self.last_final_periods[w_op.thread as usize]
            })
            .map(|(id, _)| *id)
            .collect();

        self.remove_ops(ids)?;

        let ids = self
            .final_operations
            .iter()
            .filter(|(_, (exp, thread))| *exp <= self.last_final_periods[*thread as usize])
            .map(|(id, _)| *id);

        for id in ids.collect::<Vec<_>>() {
            self.final_operations.remove(&id);
        }

        Ok(())
    }

    pub fn update_latest_final_periods(&mut self, periods: Vec<u64>) -> Result<(), PoolError> {
        self.last_final_periods = periods;
        self.prune()
    }

    // Removes a list of operations from the pool.
    fn remove_ops(&mut self, op_ids: Vec<OperationId>) -> Result<(), PoolError> {
        // Remove from shared storage.
        self.storage.remove_operations(&op_ids);

        // Remove from internal structures.
        for op_id in op_ids.into_iter() {
            if let Some(wrapped_op) = self.ops.remove(&op_id) {
                // complexity: const
                let interest = (std::cmp::Reverse(wrapped_op.fee_density), op_id);
                self.ops_by_thread_and_interest[wrapped_op.thread as usize].remove(&interest);
                // complexity: log

                for addr in wrapped_op.ledger_involved_addresses {
                    self.ops_by_address.remove_op_for_address(&addr, &op_id);
                }
            }
        }

        Ok(())
    }

    /// Get `max_count` operation for thread `block_slot.thread`
    /// if vector is not full that means that there is no more interesting transactions left
    pub fn get_operation_batch(
        &mut self,
        block_slot: Slot,
        exclude: Set<OperationId>,
        batch_size: usize,
        max_size: u64,
    ) -> Result<Vec<(OperationId, SignedOperation, u64)>, PoolError> {
        self.ops_by_thread_and_interest[block_slot.thread as usize]
            .iter()
            .filter_map(|(_rentability, id)| {
                if exclude.contains(id) {
                    return None;
                }
                if let Some(w_op) = self.ops.get(id) {
                    let stored_operation = self
                        .storage
                        .retrieve_operation(id)?;
                    let stored_operation = stored_operation.read();
                    if !stored_operation.operation.content.get_validity_range(self.cfg.operation_validity_periods)
                        .contains(&block_slot.period) || w_op.byte_count > max_size {
                            massa_trace!("pool get_operation_batch not added to batch w_op.op.content.get_validity_range incorrect not added", {
                                "range": stored_operation.operation.content.get_validity_range(self.cfg.operation_validity_periods),
                                "block_slot.period": block_slot.period,
                                "operation_id": id,
                                "max_size_overflow": w_op.byte_count > max_size,
                                "byte_count": w_op.byte_count,
                            });
                        return None;
                    }
                    Some(Ok((*id, stored_operation.operation.clone(), w_op.byte_count)))
                } else {
                    Some(Err(PoolError::ContainerInconsistency(
                        format!("operation pool get_ops inconsistency: op_id={} is in ops_by_thread_and_interest but not in ops", id)
                    )))
                }
            })
            .take(batch_size)
            .collect()
    }

    pub fn get_operations(
        &self,
        operation_ids: &Set<OperationId>,
    ) -> Map<OperationId, SignedOperation> {
        operation_ids
            .iter()
            .filter_map(|op_id| {
                self.storage
                    .retrieve_operation(op_id)
                    .map(|stored| (*op_id, stored.read().operation.clone()))
            })
            .collect()
    }

    pub fn get_operations_involving_address(
        &self,
        address: &Address,
    ) -> Result<Map<OperationId, OperationSearchResult>, PoolError> {
        if let Some(ids) = self.ops_by_address.get_ops_for_address(address) {
            ids.iter()
                .take(self.cfg.settings.max_item_return_count)
                .map(|op_id| {
                    self.storage
                        .retrieve_operation(op_id)
                        .ok_or_else(|| {
                            PoolError::ContainerInconsistency(
                                "op in ops by address is not in ops".to_string(),
                            )
                        })
                        .map(|op| {
                            (
                                *op_id,
                                OperationSearchResult {
                                    op: op.read().operation.clone(),
                                    in_pool: true,
                                    in_blocks: Map::default(),
                                    status: OperationSearchResultStatus::Pending,
                                },
                            )
                        })
                })
                .collect()
        } else {
            Ok(Map::default())
        }
    }
}
