#[cfg(test)]
mod tests {
    use rrmvcc::db::*;
    use rrmvcc::tx::*;

    #[test]
    fn test_read_committed() {
        let mut db = Database::new();
        db.default_isolation_level = IsolationLevel::ReadCommitted;

        let mut c1 = db.new_connection();
        c1.exec_command(Command::Begin).unwrap();

        let mut c2 = db.new_connection();
        c2.exec_command(Command::Begin).unwrap();

        if let Ok(ret) = c1.exec_command(Command::Set("x".to_string(), "hey".to_string())) {
            assert_eq!(ret, "[SET] key:x, val:hey");
        }

        if let Ok(ret) = c1.exec_command(Command::Get("x".to_string())) {
            assert_eq!(ret, "[GET] key:x, val:hey");
        }

        if let Err(ret) = c2.exec_command(Command::Get("x".to_string())) {
            assert_eq!(ret, "[GET] key x not found");
        }

        if let Ok(ret) = c1.exec_command(Command::Commit) {
            assert_eq!(ret, "[COMMIT] finish");
        }

        if let Ok(ret) = c2.exec_command(Command::Get("x".to_string())) {
            assert_eq!(ret, "[GET] key:x, val:hey");
        }

        let mut c3 = db.new_connection();
        c3.exec_command(Command::Begin).unwrap();

        if let Ok(ret) = c3.exec_command(Command::Set("x".to_string(), "yall".to_string())) {
            assert_eq!(ret, "[SET] key:x, val:yall")
        }

        if let Ok(ret) = c3.exec_command(Command::Get("x".to_string())) {
            assert_eq!(ret, "[GET] key:x, val:yall");
        }

        if let Ok(ret) = c2.exec_command(Command::Get("x".to_string())) {
            assert_eq!(ret, "[GET] key:x, val:hey");
        }

        if let Ok(ret) = c3.exec_command(Command::Abort) {
            assert_eq!(ret, "[ABORT] finish");
        }

        if let Ok(ret) = c2.exec_command(Command::Get("x".to_string())) {
            assert_eq!(ret, "[GET] key:x, val:hey");
        }

        if let Ok(ret) = c2.exec_command(Command::Delete("x".to_string())) {
            assert_eq!(ret, "[DELETE] key:x");
        }

        if let Err(ret) = c2.exec_command(Command::Get("x".to_string())) {
            assert_eq!(ret, "[GET] key x not found");
        }

        if let Ok(ret) = c2.exec_command(Command::Commit) {
            assert_eq!(ret, "[COMMIT] finish");
        }

        let mut c4 = db.new_connection();
        c4.exec_command(Command::Begin).unwrap();

        if let Err(ret) = c4.exec_command(Command::Get("x".to_string())) {
            assert_eq!(ret, "[GET] key x not found");
        }
    }
}
