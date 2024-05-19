#[cfg(test)]
mod tests {
    use rrmvcc::db::*;
    use rrmvcc::tx::*;

    #[test]
    fn test_snapshot() {
        let mut db = Database::new();
        db.default_isolation_level = IsolationLevel::Snapshot;

        let mut c1 = db.new_connection();
        c1.exec_command(Command::Begin).unwrap();

        let mut c2 = db.new_connection();
        c2.exec_command(Command::Begin).unwrap();

        let mut c3 = db.new_connection();
        c3.exec_command(Command::Begin).unwrap();

        if let Ok(ret) = c1.exec_command(Command::Set("x".to_string(), "hey".to_string())) {
            assert_eq!(ret, "[SET] key:x, val:hey");
        }

        if let Ok(ret) = c1.exec_command(Command::Commit) {
            assert_eq!(ret, "[COMMIT] finish");
        }

        if let Ok(ret) = c2.exec_command(Command::Set("x".to_string(), "hey".to_string())) {
            assert_eq!(ret, "[SET] key:x, val:hey");
        }

        if let Err(ret) = c2.exec_command(Command::Commit) {
            assert_eq!(ret, "Write-Write Conflict");
        }

        if let Ok(ret) = c3.exec_command(Command::Set("y".to_string(), "no conflict".to_string())){
            assert_eq!(ret, "[SET] key:y, val:no conflict");
        }
    }
}
