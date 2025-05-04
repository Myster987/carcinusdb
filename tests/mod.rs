use std::path::PathBuf;

#[test]
fn main_test() -> anyhow::Result<()> {
    let path_a = PathBuf::from("./base");
    let path_b = PathBuf::from("12412.0");

    let path_c = path_a.join(path_b);

    println!("a: {:?}", path_a);
    println!("c: {:?}", path_c);

    Ok(())
}
