pub fn resp_encode(commands: &Vec<String>) -> String {
    let mut result = format!("*{}\r\n", commands.len());
    for command in commands {
        result.push_str(format!("${}\r\n{}\r\n", command.len(), command).as_str())
    }
    result
}