use std::path::Path;

fn main() {
    flatc_rust::run(flatc_rust::Args {
        lang: "rust",
        inputs: &[Path::new("./api/flat/messages.fbs")],
        out_dir: Path::new("./src/messages/"),
        ..Default::default()
    })
    .expect("flatc");
}
