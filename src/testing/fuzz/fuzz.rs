use std::{
    fs::File,
    io::{BufReader, BufWriter, Read},
    marker::PhantomData,
    path::Path,
    time::Duration,
};

use futures::{channel::oneshot, StreamExt};
use futures_timer::Delay;
use log::info;

use crate::{
    nodes::NodeIndex,
    testing::mock::{
        new_for_peer_id, spawn_honest_member, Data, Hasher64, NetworkData as ND,
        NetworkDataEncoderDecoder, PartialMultisignature, Signature,
    },
    Network, NetworkData, SpawnHandle,
};

use crate::testing::mock::{configure_network, EvesDroppingHook, Spawner};

#[tokio::test]
#[ignore]
async fn generate_fuzz_corpus() {
    let fuzz_output = Path::new("fuzz.corpus");
    generate_fuzz(fuzz_output, 4, 30).await
}

#[tokio::test]
#[ignore]
async fn load_fuzz_corpus() {
    let fuzz_input = Path::new("fuzz.corpus");
    load_fuzz(fuzz_input, 4).await
}

async fn load_fuzz(path: &Path, n_members: usize) {
    let reader = BufReader::new(File::open(path).expect("unable to open a corpus file"));
    let data_iter = NetworkDataIterator::new(reader).collect();
    fuzz(data_iter, n_members, 30).await;
}

pub(crate) async fn generate_fuzz(path: &Path, n_members: usize, n_batches: usize) {
    let spawner = Spawner::new();
    let mut batch_rxs = Vec::new();
    let file = BufWriter::new(File::create(path).expect("ubable to create a corpus file"));
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

fn after_iter<'a, I>(
    iter: impl Iterator<Item = I> + 'a,
    action: impl FnOnce(),
) -> impl Iterator<Item = I> {
    struct EndSignal<S, I> {
        signal: S,
        _phantom: PhantomData<I>,
    }
    struct EmptyIterator<S, I> {
        action: Option<S>,
        _marker: PhantomData<I>,
    }
    impl<S: FnOnce(), I> Iterator for EmptyIterator<S, I> {
        type Item = I;

        fn next(&mut self) -> Option<Self::Item> {
            match self.action.take() {
                Some(a) => (a)(),
                None => panic!("empty iterator was called a second time"),
            }
            None
        }
    }

    impl<S: FnOnce(), I> IntoIterator for EndSignal<S, I> {
        type Item = I;

        type IntoIter = EmptyIterator<S, I>;

        fn into_iter(self) -> Self::IntoIter {
            EmptyIterator {
                action: Some(self.signal),
                _marker: PhantomData,
            }
        }
    }
    iter.chain(EndSignal {
        signal: action,
        _phantom: PhantomData,
    })
}

pub async fn fuzz(data: Vec<ND>, n_members: usize, n_batches: usize) {
    const NETWORK_DELAY: u64 = 200;
    let spawner = Spawner::new();
    let (empty_tx, empty_rx) = oneshot::channel();
    let data = after_iter(data.into_iter(), move || {
        // panic!("wot");
        empty_tx.send(()).expect("empty_rx was already closed");
    });

    let network = RecorderNetwork::new(data, NETWORK_DELAY);

    let (mut batch_rx, exit_tx) = spawn_honest_member(spawner.clone(), 0, n_members, network);

    empty_rx.await.expect("empty_tx was unexpectedly dropped");

    let mut batches_count = 0;
    loop {
        match batch_rx.next().await {
            Some(_) => {
                batches_count += 1;
            }
            None => break,
        }
    }
    if n_batches != batches_count {
        info!(target: "rush", "Expected {:?} batches, received {:?}", n_batches, batches_count);
    }

    exit_tx
        .send(())
        .unwrap_or(info!("exit_rx is already closed"));

    // 'exits' is dropped here and allows members to gracefully exit
    spawner.wait().await;
}

struct NetworkDataIterator<R: Read> {
    input: R,
    enc_dec: NetworkDataEncoderDecoder,
}

impl<R: Read> NetworkDataIterator<R> {
    fn new(read: R) -> Self {
        NetworkDataIterator {
            input: read,
            enc_dec: NetworkDataEncoderDecoder {},
        }
    }
}

impl<R: Read> Iterator for NetworkDataIterator<R> {
    type Item = ND;

    fn next(&mut self) -> Option<Self::Item> {
        match self.enc_dec.decode_from(&mut self.input) {
            Ok(v) => Some(v),
            Err(_) => None,
        }
    }
}

// TODO implement also saving of sent messages, like in the asynchronous model, so you can one-to-one record the protocols execution
struct RecorderNetwork<I: Iterator<Item = ND>> {
    data: I,
    delay: Duration,
    next_delay: Delay,
}

impl<I: Iterator<Item = ND> + Send> RecorderNetwork<I> {
    fn new(data: I, delay_millis: u64) -> Self {
        RecorderNetwork {
            data,
            delay: Duration::from_millis(delay_millis),
            next_delay: Delay::new(Duration::from_millis(0)),
        }
    }
}

#[async_trait::async_trait]
impl<I: Iterator<Item = ND> + Send> Network<Hasher64, Data, Signature, PartialMultisignature>
    for RecorderNetwork<I>
{
    type Error = ();

    fn send(
        &self,
        _: NetworkData<Hasher64, Data, Signature, PartialMultisignature>,
        _: NodeIndex,
    ) -> std::result::Result<(), Self::Error> {
        Ok(())
    }

    fn broadcast(
        &self,
        _: NetworkData<Hasher64, Data, Signature, PartialMultisignature>,
    ) -> std::result::Result<(), Self::Error> {
        Ok(())
    }

    async fn next_event(
        &mut self,
    ) -> Option<NetworkData<Hasher64, Data, Signature, PartialMultisignature>> {
        (&mut self.next_delay).await;
        self.next_delay.reset(self.delay);
        self.data.next()
    }
}
