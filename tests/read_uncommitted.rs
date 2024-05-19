#[cfg(test)]
mod tests {
    use rrmvcc::db::*;
    use rrmvcc::tx::*;

    #[test]
    fn test_read_uncommitted() {
        let mut db = Database::new();
        db.default_isolation_level = IsolationLevel::ReadUncommitted;

        let mut c1 = db.new_connection();
        c1.exec_command(Command::Begin).unwrap();

        let mut c2 = db.new_connection();
        c2.exec_command(Command::Begin).unwrap();

        if let Ok(ret) = c1.exec_command(Command::Set("hello".to_string(), "world".to_string())) {
            assert_eq!(ret, "[SET] key:hello, val:world");
        }

        if let Ok(ret) = c1.exec_command(Command::Get("hello".to_string())) {
            assert_eq!(ret, "[GET] key:hello, val:world");
        }

        if let Ok(ret) = c2.exec_command(Command::Get("hello".to_string())) {
            assert_eq!(ret, "[GET] key:hello, val:world");
        }
    }
}
