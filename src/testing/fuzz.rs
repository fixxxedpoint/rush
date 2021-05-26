use std::path::Path;

#[tokio::test(max_threads = 1)]
pub(crate) fn generate_fuzz(path: Path) {
    let file = File::create(path);
}
