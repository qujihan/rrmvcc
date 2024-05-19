use crate::db::*;
#[allow(unused)]
use crate::debug_info;
#[allow(unused)]
use crate::utils::*;
use std::{cell::RefCell, collections::BTreeSet, rc::Rc};

#[derive(PartialEq, Debug, Clone)]
pub enum Command {
    Begin,
    Abort,
    Commit,
    Get(KeyType),
    Set(KeyType, ValueType),
    Delete(KeyType),
}

#[derive(PartialEq, Clone, Debug)]
pub enum TransactionState {
    Active,
    Committed,
    Aborted,
}

#[derive(Debug)]
pub struct Transaction {
    pub id: u64,
    pub state: TransactionState,
    pub isolation_level: IsolationLevel,
    pub inprogress: BTreeSet<TxIdType>,
    pub write_set: BTreeSet<KeyType>,
    pub read_set: BTreeSet<KeyType>,
}

pub struct Connection<'a> {
    pub tx: Option<Rc<RefCell<Transaction>>>,
    pub db: &'a Database,
}

impl<'a> Connection<'a> {
    pub fn exec_command(&mut self, command: Command) -> Result<String, String> {
        match command {
            Command::Begin => {
                self.tx = Some(self.db.new_transaction());
                if let Some(tx) = &self.tx {
                    let tx_id: TxIdType = tx.as_ref().borrow().id.clone();
                    self.db.assert_transaction(tx_id);
                    return Ok("[BEGIN] finish".to_string());
                }
                return Err("[BEGIN] no active transaction".to_string());
            }
            Command::Abort => {
                if let Some(tx) = &self.tx {
                    let tx_id: TxIdType = tx.as_ref().borrow().id.clone();
                    self.db.assert_transaction(tx_id);
                    let tx_id: TxIdType = tx.as_ref().borrow().id.clone();
                    self.db
                        .complete_transaction(tx_id, TransactionState::Aborted)?;
                    return Ok("[ABORT] finish".to_string());
                }
                return Err("[ABORT] no active transaction".to_string());
            }
            Command::Commit => {
                if let Some(tx) = &self.tx {
                    let tx_id: TxIdType = tx.as_ref().borrow().id.clone();
                    self.db.assert_transaction(tx_id);
                    self.db
                        .complete_transaction(tx_id, TransactionState::Committed)?;
                    return Ok("[COMMIT] finish".to_string());
                }
                return Err("[COMMIT] no active transaction".to_string());
            }
            Command::Get(key) => {
                if let Some(tx) = &self.tx {
                    {
                        let mut tx_mut = tx.as_ref().borrow_mut();
                        tx_mut.read_set.insert(key.clone());
                    }
                    let tx_id: TxIdType = tx.as_ref().borrow().id.clone();
                    self.db.assert_transaction(tx_id);
                    let kvlist = self.db.kvs_info.as_ref().borrow();
                    if let Some(values) = kvlist.get(&key) {
                        if let Some(val) = values.iter().rfind(|v| self.db.is_visible(&tx, v)) {
                            return Ok(format!("[GET] key:{}, val:{}", key, val.data));
                        }
                    }
                    return Err(format!("[GET] key {} not found", key.clone()));
                }
                return Err("[GET] no active transaction".to_string());
            }
            Command::Set(key, val) => {
                if let Some(tx) = &self.tx {
                    let tx_id: TxIdType = tx.as_ref().borrow().id.clone();
                    self.db.assert_transaction(tx_id);
                    {
                        let mut tx_mut = tx.as_ref().borrow_mut();
                        tx_mut.write_set.insert(key.clone());
                    }
                    let mut kvlist = self.db.kvs_info.as_ref().borrow_mut();
                    if let Some(values) = kvlist.get_mut(&key) {
                        values
                            .iter_mut()
                            .rev()
                            .filter(|v| self.db.is_visible(&tx, *v))
                            .for_each(|v| v.tx_end_id = tx.as_ref().borrow().id);

                        values.push(Value {
                            data: val.clone(),
                            tx_start_id: tx.as_ref().borrow().id,
                            tx_end_id: 0,
                        });
                        return Ok(format!("[SET] key:{}, val:{}", key, val));
                    } else {
                        kvlist.insert(
                            key.clone(),
                            vec![Value {
                                data: val.clone(),
                                tx_start_id: tx.as_ref().borrow().id,
                                tx_end_id: 0,
                            }],
                        );
                        return Ok(format!("[SET] key:{}, val:{}", key, val));
                    }
                }
                return Err("[SET] no active transaction".to_string());
            }
            Command::Delete(key) => {
                if let Some(tx) = &self.tx {
                    let tx_id: TxIdType = tx.as_ref().borrow().id.clone();
                    self.db.assert_transaction(tx_id);

                    let mut kvlist = self.db.kvs_info.as_ref().borrow_mut();
                    if let Some(values) = kvlist.get_mut(&key) {
                        let mut fonnd = false;
                        values
                            .iter_mut()
                            .rev()
                            .filter(|v| self.db.is_visible(&tx, *v))
                            .for_each(|v| {
                                v.tx_end_id = tx.as_ref().borrow().id;
                                fonnd = true;
                            });

                        if !fonnd {
                            return Err(format!("[DELETE] key {} not found", key));
                        }
                        {
                            let mut tx_mut = tx.as_ref().borrow_mut();
                            tx_mut.write_set.insert(key.clone());
                        }
                    }
                    return Ok(format!("[DELETE] key:{}", key));
                }
                return Err("[DELETE] no active transaction".to_string());
            }
        }
    }
}
