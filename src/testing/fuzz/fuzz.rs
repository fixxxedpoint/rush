use std::{
    fs::File,
    io::{BufRead, BufReader, BufWriter, Read, Result},
    path::Path,
    sync::atomic::AtomicBool,
    time::Duration,
};

use codec::{Decode, Encode, Error};
use futures::{
    channel::{
        mpsc::{channel, unbounded, Sender},
        oneshot,
    },
    FutureExt, SinkExt, Stream, StreamExt,
};
use tokio::{
    select,
    time::{delay_for, Delay},
};

use crate::{
    nodes::NodeIndex,
    testing::mock::{
        new_for_peer_id, spawn_honest_member, Data, Hasher64, NetworkDataEncoderDecoder, Signature,
        StoredNetworkData,
    },
    Hasher, Network, NetworkData, SpawnHandle,
};

use crate::testing::mock::{configure_network, EvesDroppingHook, Spawner};

use libfuzzer_sys::arbitrary::Arbitrary;

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
    let enc_dec = NetworkDataEncoderDecoder {};
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

fn after_iter<I>(iter: impl Iterator<Item = I>, action: impl FnMut()) -> impl Iterator {
    struct EndSignal<S: FnMut()> {
        signal: S,
    }
    impl<S, I> IntoIterator for EndSignal<S> {
        type Item = StoredNetworkData;

        type IntoIter = std::vec::IntoIter<I>;

        fn into_iter(self) -> Self::IntoIter {
            self.signal();
            vec![].into_iter()
        }
    }
    iter.chain(EndSignal { signal: action })
}

async fn fuzz(data: impl Iterator<Item = StoredNetworkData>, n_members: usize) {
    const NETWORK_DELAY: u64 = 200;
    let spawner = Spawner::new();
    let (empty_tx, empty_rx) = oneshot::channel();
    let data = after_iter(data, move || empty_tx.send(()));

    let network = RecorderNetwork::new(data, NETWORK_DELAY);
    let mut batch_rxs = Vec::new();

    let (batch_rx, exit_tx) = spawn_honest_member(spawner.clone(), 0, n_members, network);

    empty_rx.await;
    exit_tx.send(());

    // 'exits' is dropped here and allows members to gracefully exit
    spawner.wait().await;
}

struct NetworkDataIterator<R: Read> {
    // input: R,
    buf: BufReader<R>,
    enc_dec: NetworkDataEncoderDecoder,
}

impl<R: Read> NetworkDataIterator<R> {
    fn new(read: R) -> Self {
        NetworkDataIterator {
            // input: read,
            buf: BufReader::new(read),
            enc_dec: NetworkDataEncoderDecoder {},
        }
    }
}

impl<R: Read> Iterator for NetworkDataIterator<R> {
    type Item = Result<StoredNetworkData>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.buf.fill_buf() {
            Ok(buf) => {
                if buf.is_empty() {
                    return None;
                }
            }
            Err(e) => return Some(Err(e)),
        }
        let next = self.enc_dec.decode_from(&mut self.input);
        match next {
            Ok(next) => Some(Ok(StoredNetworkData {
                data: next.0,
                sender: next.1,
                recipient: next.2,
            })),
            Err(e) => Some(Err(e)),
        }
    }
}

// TODO implement also saving of sent messages, like in the asynchronous model, so you can one-to-one record the protocols execution
struct RecorderNetwork<I: Iterator<Item = NetworkData<Hasher64, Data, Signature>>> {
    data: I,
    delay_millis: u64,
    next_delay: Delay,
}

impl<I: Iterator<Item = NetworkData<Hasher64, Data, Signature>>> RecorderNetwork<I> {
    fn new(data: I, delay_millis: u64) -> Self {
        RecorderNetwork {
            data,
            delay_millis,
            next_delay: delay_for(Duration::ZERO),
        }
    }
}

#[async_trait::async_trait]
impl<I: Iterator<Item = NetworkData<Hasher64, Data, Signature>> + Send>
    Network<Hasher64, Data, Signature> for RecorderNetwork<I>
{
    type Error = ();

    fn send(
        &self,
        _: NetworkData<Hasher64, Data, Signature>,
        _: NodeIndex,
    ) -> std::result::Result<(), Self::Error> {
        Ok(())
    }

    fn broadcast(
        &self,
        _: NetworkData<Hasher64, Data, Signature>,
    ) -> std::result::Result<(), Self::Error> {
        Ok(())
    }

    async fn next_event(&mut self) -> Option<NetworkData<Hasher64, Data, Signature>> {
        self.next_delay.await;
        self.next_delay = delay_for(Duration::from_millis(self.delay_millis));
        self.data.next()
    }
}
