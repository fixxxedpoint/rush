use std::{
    fs::File,
    io::{BufWriter, Read},
    path::Path,
};

use codec::{Decode, Encode};
use futures::{channel::mpsc::channel, SinkExt, Stream, StreamExt};

use crate::{
    nodes::NodeIndex,
    testing::mock::{new_for_peer_id, spawn_honest_member, NetworkDataEncoderDecoder},
    Data, Hasher, Network, NetworkData, SpawnHandle,
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
    let buffered = BufWriter::new(file);
    let enc_dec = NetworkDataEncoderDecoder {};
    let peer_id = NodeIndex(0);
    let store = move |data, sender, recipient| {
        enc_dec.encode_into((*data, *sender, *recipient), buffered);
    };
    let network_hook = EvesDroppingHook::new(store);
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

fn read_session_data<R: Read, H, D, S, Spawn: SpawnHandle>(
    reader: R,
    spawn_handle: Spawn,
) -> impl Stream<Item = (NetworkData<H, D, S>, NodeIndex, NodeIndex)> + Send + Unpin {
    let (sender, receiver) = channel(0);
    let enc_dec = NetworkDataEncoderDecoder {};
    spawn_handle.spawn("reader", async move {
        while let Ok(v) = enc_dec.decode_from(reader) {
            sender.send(v);
        }
    });
    receiver
}

struct RecorderNetwork<
    H: Hasher,
    D: Data,
    S: Encode + Decode,
    I: Stream<Item = NetworkData<H, D, S>>,
    // N: Network<H, D, S>,
> {
    data: I,
    // wrapped: N,
}

#[async_trait::async_trait]
impl<
        H: Hasher,
        D: Data,
        S: Encode + Decode,
        I: Stream<Item = NetworkData<H, D, S>> + Send + Unpin,
    > Network<H, D, S> for RecorderNetwork<H, D, S, I>
{
    type Error = ();

    fn send(&self, _: NetworkData<H, D, S>, _: NodeIndex) -> Result<(), Self::Error> {
        Ok(())
    }

    fn broadcast(&self, _: NetworkData<H, D, S>) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn next_event(&mut self) -> Option<NetworkData<H, D, S>> {
        self.data.next().await
    }
}

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

// fn send(&self, data: NetworkData<H, D, S>, node: NodeIndex) -> Result<(), Self::Error>;
// fn broadcast(&self, data: NetworkData<H, D, S>) -> Result<(), Self::Error>;
// async fn next_event(&mut self) -> Option<NetworkData<H, D, S>>;
