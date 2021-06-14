use aleph_bft::testing::fuzz;
use std::io;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "fuzz-helper", about = "generator of fuzz data")]
struct Opt {
    /// Verify data provided on stdin by calling member::run.
    #[structopt(short, long)]
    check_fuzz: bool,

    /// Generate data for a given number of members.
    /// When used with the 'check_fuzz' flag it verifies data assuming this number of members.
    #[structopt(default_value = "4")]
    members: usize,

    /// Generate a given number of batches.
    #[structopt(default_value = "30")]
    batches: usize,

    /// When used with the 'check_fuzz' flag it will verify if we are able to generate at least this number of batches.
    verify_batches: Option<usize>,
}

fn main() {
    let opt = Opt::from_args();
    if opt.check_fuzz {
        fuzz::check_fuzz_from_input(io::stdin(), opt.members, opt.verify_batches);
    } else {
        fuzz::generate_fuzz(io::stdout(), opt.members, opt.batches);
    }
}
