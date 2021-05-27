use std::{fs::File, path::Path};

use futures::StreamExt;

use crate::{
    nodes::NodeIndex,
    testing::mock::{new_for_peer_id, spawn_honest_member},
    SpawnHandle,
};

use crate::testing::mock::{configure_network, EvesDroppingHook, Spawner};

#[tokio::test(max_threads = 1)]
#[ignore]
async fn generate_fuzz_corpus() {
    let fuzz_output = Path::new("fuzz.corpus");
    generate_fuzz(fuzz_output, 4, 30).await;
}

pub(crate) async fn generate_fuzz(path: &Path, n_members: usize, n_batches: usize) {
    let spawner = Spawner::new();
    let mut batch_rxs = Vec::new();
    let file = File::create(path).expect("ubable to create a corpus file");
    let peer_id = NodeIndex(0);
    let network_hook = EvesDroppingHook::new(file);
    let filtering_hook = new_for_peer_id(network_hook, peer_id);
    let (mut router, mut networks) = configure_network(n_members, 1.0, filtering_hook);

    spawner.spawn("network", async move { router.run().await });

    {
        let mut exits = Vec::new();
        for (ix, network) in networks.drain(..).enumerate() {
            let (batch_rx, exit_tx) = spawn_honest_member(
                spawner.clone(),
                ix,
                n_members,
                network.expect("unable to retrieve a network handle"),
            );
            exits.push(exit_tx);
            batch_rxs.push(batch_rx);
        }

        for mut rx in batch_rxs.drain(..) {
            for _ in 0..n_batches {
                rx.next().await.expect("unable to retrieve a batch");
            }
        }
        // 'exits' is dropped here and allows members to gracefully exit
    }
    spawner.wait().await;
    // TODO uzyj fuzzer'a aby podac vector danych wejsciowych, zamiast po prostu danych wejsciowych``
    // TODO zaimplementuje Network ktory wysyla tylko rzeczy podane z zewnatrz
}

// struct RecorderNetwork<I: Iterator<Item = Vec<u8>>, N: Network> {
//     data: I,
//     wrapped: N,
// }

// #[async_trait::async_trait]
// impl<I: Iterator<Item = Vec<u8>> + Send, N: Network + Send> Network for RecorderNetwork<I, N> {
//     type Error = ();

//     fn send(&self, command: crate::NetworkCommand) -> Result<(), Self::Error> {
//         todo!()
//     }

//     async fn next_event(&mut self) -> Option<crate::NetworkEvent> {
//         todo!()
//     }
// }
