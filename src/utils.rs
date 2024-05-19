#![allow(unused)]

use std::{cell::RefCell, rc::Rc};
#[derive(Debug, PartialEq)]
pub enum BorrowState {
    Unused,
    Reading,
    Writing,
}

#[macro_export]
macro_rules! debug_info {
    ($val:expr) => {
        println!(
            "[{}:{}:{}] {} = {:#?}",
            file!(),
            line!(),
            column!(),
            stringify!($val),
            $val
        );
    };
    ($($val:expr),+) => {
        $(
            debug_info!($val);
        )+
    };
}

pub fn debug_get_borrow_state<T>(rc_refcell: &Rc<RefCell<T>>) -> BorrowState {
    if rc_refcell.try_borrow_mut().is_ok() {
        BorrowState::Unused
    } else if rc_refcell.try_borrow().is_ok() {
        BorrowState::Reading
    } else {
        BorrowState::Writing
    }
}
