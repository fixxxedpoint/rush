use std::{fs::File, path::Path};

use futures::StreamExt;

use crate::{
    nodes::NodeIndex,
    testing::mock::{node_ix_to_peer, spawn_honest_member, AddressedMessage, FilteringHook},
    SpawnHandle,
};

use crate::testing::mock::{configure_network, EvesDroppingHook, Spawner};

#[tokio::test(max_threads = 1)]
#[ignore]
async fn generate_fuzz_corpus() {
    let fuzz_output = Path::new("./fuzz.corpus");
    generate_fuzz(fuzz_output, 4, 10).await;
}

pub(crate) async fn generate_fuzz(path: &Path, n_members: usize, n_batches: usize) {
    let spawner = Spawner::new();
    let mut batch_rxs = Vec::new();
    let file = File::create(path).expect("ubable to create a corpus file");
    let peer_id = node_ix_to_peer(NodeIndex(0));
    let network_hook = EvesDroppingHook::new(file);
    let filtering_hook = FilteringHook::new(network_hook, move |msg: &AddressedMessage| {
        msg.receiver == peer_id
    });
    let (mut router, mut networks) = configure_network(4, 1.0, filtering_hook);

    // spawner.spawn("network", async move { router.run().await });
    spawner.spawn("network", router);

    for (ix, network) in networks.drain(..).enumerate() {
        let (batch_rx, _) = spawn_honest_member(
            spawner.clone(),
            ix,
            n_members,
            network.expect("unable to retrieve a network handle"),
        );
        batch_rxs.push(batch_rx);
    }

    // TODO na razie wyglada jakby wszystko wisialo i zaden batch nie byl produkowany
    for mut rx in batch_rxs.drain(..) {
        for _ in 0..n_batches {
            rx.next().await.expect("unable to retrieve a batch");
        }
    }
}
