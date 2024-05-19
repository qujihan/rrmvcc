#[allow(unused)]
use crate::debug_info;
use crate::tx::*;
#[allow(unused)]
use crate::utils::*;
use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    rc::Rc,
};

pub type TxIdType = u64;
pub type KeyType = String;
pub type ValueType = String;
pub type KVListType = BTreeMap<KeyType, Vec<Value>>;
pub type TXListType = BTreeMap<TxIdType, Rc<RefCell<Transaction>>>;

#[derive(PartialEq, Clone, Debug)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Snapshot,
    Serializable,
}

#[derive(Debug)]
pub struct Value {
    pub data: String,
    pub tx_start_id: TxIdType,
    pub tx_end_id: TxIdType,
}

pub struct TxInfo {
    pub next_tx_id: TxIdType,
    pub txs: TXListType,
}

pub struct Database {
    pub kvs_info: Rc<RefCell<KVListType>>,
    pub txs_info: Rc<RefCell<TxInfo>>,
    pub default_isolation_level: IsolationLevel,
}

impl Database {
    pub fn new() -> Self {
        Database {
            kvs_info: Rc::new(RefCell::new(Default::default())),
            txs_info: Rc::new(RefCell::new(TxInfo {
                next_tx_id: 1,
                txs: Default::default(),
            })),
            default_isolation_level: IsolationLevel::ReadUncommitted,
        }
    }

    pub fn new_connection(&self) -> Connection {
        Connection { tx: None, db: self }
    }

    pub fn new_transaction(&self) -> Rc<RefCell<Transaction>> {
        let tx_id = self.txs_info.as_ref().borrow().next_tx_id.clone();
        let isolation_level = self.default_isolation_level.clone();
        let tx = Rc::new(RefCell::new(Transaction {
            id: tx_id,
            state: TransactionState::Active,
            isolation_level: isolation_level,
            inprogress: self.get_active_tx(),
            write_set: Default::default(),
            read_set: Default::default(),
        }));
        self.txs_info.borrow_mut().next_tx_id += 1;
        self.txs_info.borrow_mut().txs.insert(tx_id, Rc::clone(&tx));
        tx
    }

    fn get_active_tx(&self) -> BTreeSet<TxIdType> {
        self.txs_info
            .as_ref()
            .borrow()
            .txs
            .iter()
            .filter(|(_, tx)| tx.as_ref().borrow().state == TransactionState::Active)
            .map(|(tx_id, _)| *tx_id)
            .collect()
    }

    fn set_share_item(set1: &BTreeSet<KeyType>, set2: &BTreeSet<KeyType>) -> bool {
        set1.iter().any(|item| set2.contains(item))
    }

    fn conflict_check<F>(&self, tx: &Rc<RefCell<Transaction>>, conflict_func: F) -> bool
    where
        F: Fn(Rc<RefCell<Transaction>>, Rc<RefCell<Transaction>>) -> bool,
    {
        {
            for tx_id in tx.as_ref().borrow().inprogress.iter() {
                if let Some(tx_other) = self.txs_info.as_ref().borrow().txs.get(&tx_id) {
                    if tx_other.as_ref().borrow().state == TransactionState::Committed
                        && conflict_func(Rc::clone(&tx), Rc::clone(tx_other))
                    {
                        return true;
                    }
                }
            }
        }

        {
            for tx_id in tx.as_ref().borrow().id..self.txs_info.as_ref().borrow().next_tx_id {
                if let Some(tx_other) = self.txs_info.as_ref().borrow().txs.get(&tx_id) {
                    if tx_other.as_ref().borrow().state == TransactionState::Committed
                        && conflict_func(Rc::clone(&tx), Rc::clone(tx_other))
                    {
                        return true;
                    }
                }
            }
        }

        false
    }

    pub fn complete_transaction(
        &self,
        tx_id: TxIdType,
        state: TransactionState,
    ) -> Result<(), String> {
        if let Some(tx) = self.txs_info.as_ref().borrow().txs.get(&tx_id) {
            match state {
                TransactionState::Committed => {
                    {
                        if tx.as_ref().borrow().isolation_level == IsolationLevel::Snapshot
                            && self.conflict_check(tx, |t1, t2| {
                                Database::set_share_item(
                                    &t1.as_ref().borrow().write_set,
                                    &t2.as_ref().borrow().write_set,
                                )
                            })
                        {
                            self.complete_transaction(tx_id, TransactionState::Aborted)?;
                            return Err("Write-Write Conflict".to_string());
                        }

                        if tx.as_ref().borrow().isolation_level == IsolationLevel::Serializable
                            && self.conflict_check(tx, |t1, t2| {
                                Database::set_share_item(
                                    &t1.as_ref().borrow().read_set,
                                    &t2.as_ref().borrow().write_set,
                                )
                            })
                        {
                            self.complete_transaction(tx_id, TransactionState::Aborted)?;
                            return Err("Read-Write Conflict".to_string());
                        }
                    }
                    {
                        tx.borrow_mut().state = state.clone()
                    }
                }
                TransactionState::Aborted => tx.borrow_mut().state = state.clone(),
                _ => return Err("Invalid transaction state".to_string()),
            }
            return Ok(());
        }
        return Err("Transaction not found".to_string());
    }

    fn get_transaction_state(&self, tx_id: TxIdType) -> Option<TransactionState> {
        if let Some(tx) = self.txs_info.as_ref().borrow().txs.get(&tx_id) {
            return Some(tx.as_ref().borrow().state.clone());
        }
        None
    }

    pub fn assert_transaction(&self, tx_id: TxIdType) {
        assert!(tx_id > 0, "Invalid transaction id, must be greater than 0");
        assert!(
            self.get_transaction_state(tx_id) == Some(TransactionState::Active),
            "Transaction is not active"
        );
    }

    pub fn is_visible(&self, tx: &Rc<RefCell<Transaction>>, val: &Value) -> bool {
        let tx = tx.borrow_mut();
        match tx.isolation_level {
            IsolationLevel::ReadUncommitted => true,
            IsolationLevel::ReadCommitted => {
                if val.tx_end_id != tx.id
                    && self.get_transaction_state(val.tx_end_id)
                        != Some(TransactionState::Committed)
                {
                    return false;
                }

                if val.tx_end_id == tx.id {
                    return false;
                }

                if val.tx_end_id > 0
                    && self.get_transaction_state(val.tx_end_id)
                        == Some(TransactionState::Committed)
                {
                    return true;
                }

                true
            }
            IsolationLevel::RepeatableRead
            | IsolationLevel::Snapshot
            | IsolationLevel::Serializable => {
                if val.tx_start_id > tx.id {
                    return false;
                }

                if tx.inprogress.contains(&val.tx_start_id) {
                    return false;
                }

                if val.tx_start_id != tx.id
                    && self.get_transaction_state(val.tx_start_id)
                        != Some(TransactionState::Committed)
                {
                    return false;
                }

                if val.tx_end_id == tx.id {
                    return false;
                }

                if val.tx_end_id < tx.id
                    && val.tx_end_id > 0
                    && self.get_transaction_state(val.tx_end_id)
                        == Some(TransactionState::Committed)
                    && !tx.inprogress.contains(&val.tx_end_id)
                {
                    return false;
                }

                true
            }
        }
    }
}
