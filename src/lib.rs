#[cfg(test)]
mod tests {
    use {
        agave_banking_stage_ingress_types::BankingPacketBatch,
        crossbeam_channel::unbounded,
        solana_core::{
            banking_stage::{
                BankingStage, committer::Committer, consumer::Consumer,
                transaction_scheduler::scheduler_controller::SchedulerConfig,
            },
            banking_trace::{BankingTracer, Channels},
            validator::{BlockProductionMethod, SchedulerPacing},
        },
        solana_entry::entry_or_marker::EntryOrMarker,
        solana_ledger::{
            blockstore::Blockstore,
            genesis_utils::{
                GenesisConfigInfo, bootstrap_validator_stake_lamports,
                create_genesis_config_with_leader,
            },
            get_tmp_ledger_path_auto_delete,
        },
        solana_perf::packet::to_packet_batches,
        solana_poh::{
            poh_recorder::create_test_recorder, record_channels::record_channels,
            transaction_recorder::TransactionRecorder,
        },
        solana_runtime::bank::Bank,
        solana_runtime_transaction::runtime_transaction::RuntimeTransaction,
        solana_system_transaction as system_transaction,
        solana_transaction::{Transaction, sanitized::SanitizedTransaction},
        std::{
            num::NonZeroUsize,
            sync::{Arc, atomic::Ordering},
            thread::sleep,
            time::{Duration, Instant},
        },
        tokio::sync::mpsc,
    };

    // Votor per-round timing from Alpenglow Figure 2
    // Timeout(i) = clock() + delta_timeout + (i − s + 1) · delta_block
    // Finalization: 1 round ≥ 80% stake OR 2 rounds ≥ 60% stake.
    const VOTOR_DELTA_BLOCK_MS: u64 = 400;
    const VOTOR_DELTA_MS: u64 = 50; // assuming 50ms

    // Number of consecutive slot executions to measure; jitter = max − min.
    const N_ITERATIONS: usize = 10;

    fn create_slow_genesis_config(lamports: u64) -> GenesisConfigInfo {
        let validator_pubkey = solana_pubkey::new_rand();
        let mut info = create_genesis_config_with_leader(
            lamports,
            &validator_pubkey,
            bootstrap_validator_stake_lamports(),
        );
        // Extend ticks so the slot doesn't expire across N iterations.
        info.genesis_config.ticks_per_slot *= 1024;
        info
    }

    fn stats(vals: &[u64]) -> (u64, u64, u64) {
        let min = *vals.iter().min().unwrap_or(&0);
        let max = *vals.iter().max().unwrap_or(&0);
        let mean = if vals.is_empty() {
            0
        } else {
            vals.iter().sum::<u64>() / vals.len() as u64
        };
        (min, mean, max)
    }

    // sanitize_transactions is pub(crate) in agave's test module — not importable externally.
    fn sanitize_transactions(
        txs: Vec<Transaction>,
    ) -> Vec<RuntimeTransaction<SanitizedTransaction>> {
        txs.into_iter()
            .map(RuntimeTransaction::from_transaction_for_tests)
            .collect()
    }

    // Phase timing from consumer.rs debug logs execute_and_commit_transactions_locked emits two debug! lines:
    //
    //   "bank: {slot} lock: {lock_us} us unlock: {unlock_us} us txs_len: {n}"
    //   "bank: {slot} process_and_record_locked: {load_execute_us} us"
    //   "record: {record_us} us commit: {commit_us} us txs_len: {n}"
    //
    // These are the same fields that end up in LeaderExecuteAndCommitTimings.
    #[test]
    fn measure_banking_stage_slot_timing() {
        agave_logger::setup();

        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_slow_genesis_config(1_000_000_000);

        let (bank, bank_forks) = Bank::new_with_bank_forks_for_tests(&genesis_config);
        let start_hash = bank.last_blockhash();

        let banking_tracer = BankingTracer::new_disabled();
        let Channels {
            non_vote_sender,
            non_vote_receiver,
            tpu_vote_sender,
            tpu_vote_receiver,
            gossip_vote_sender,
            gossip_vote_receiver,
        } = banking_tracer.create_channels();

        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Arc::new(Blockstore::open(ledger_path.path()).expect("open blockstore"));

        let (
            exit,
            poh_recorder,
            _poh_controller,
            transaction_recorder,
            poh_service,
            entry_receiver,
        ) = create_test_recorder(bank, blockstore, None, None);

        let (replay_vote_sender, _replay_vote_receiver) = unbounded();

        let banking_stage = BankingStage::new_num_threads(
            BlockProductionMethod::CentralSchedulerGreedy,
            poh_recorder.clone(),
            transaction_recorder,
            non_vote_receiver,
            tpu_vote_receiver,
            gossip_vote_receiver,
            mpsc::channel(1).1,
            NonZeroUsize::new(4).unwrap(),
            SchedulerConfig {
                scheduler_pacing: SchedulerPacing::Disabled,
            },
            None,
            replay_vote_sender,
            None,
            bank_forks,
            None,
            Arc::default(),
        );

        // N iterations through the same banking stage
        // Each iteration sends one transfer to a fresh recipient and records
        // how long until the committed entry arrives.  The slot stays open
        // because ticks_per_slot was multiplied by 1024.
        let mut latencies_ms: Vec<u64> = Vec::with_capacity(N_ITERATIONS);

        for i in 0..N_ITERATIONS {
            let recipient = solana_pubkey::new_rand();
            let tx = system_transaction::transfer(&mint_keypair, &recipient, 1, start_hash);
            let batches = to_packet_batches(&[tx], 1);

            let t0 = Instant::now();

            non_vote_sender
                .send(BankingPacketBatch::new(batches))
                .unwrap();

            let elapsed_ms = loop {
                if let Ok((_bank, (EntryOrMarker::Entry(entry), _))) = entry_receiver.try_recv() {
                    if !entry.transactions.is_empty() {
                        break t0.elapsed().as_millis() as u64;
                    }
                }
                sleep(Duration::from_millis(1));
            };

            eprintln!("[iter {:>2}] send to commit: {:>4} ms", i, elapsed_ms);
            latencies_ms.push(elapsed_ms);
        }

        let delta_timeout = 3 * VOTOR_DELTA_MS;

        let timeout = delta_timeout + VOTOR_DELTA_BLOCK_MS;

        // Teardown
        drop(non_vote_sender);
        drop(tpu_vote_sender);
        drop(gossip_vote_sender);
        banking_stage.join().unwrap();

        exit.store(true, Ordering::Relaxed);
        poh_service.join().unwrap();
        drop(poh_recorder);

        for (_bank, (eom, _)) in entry_receiver.iter() {
            let _ = eom.unwrap_entry();
        }

        // Report
        let (min, mean, max) = stats(&latencies_ms);
        let jitter = max - min;
        println!("\nBanking Jitter Report ({N_ITERATIONS} runs)\n");

        println!("from send to committed entry (ms):");
        println!("  min {min}  mean {mean}  max {max}  jitter {jitter}\n");

        println!("Votor timeout: {}", timeout);
        println!("Votor budget (Delta_block = {VOTOR_DELTA_BLOCK_MS} ms):");
        println!(
            "  mean: {}",
            if mean <= VOTOR_DELTA_BLOCK_MS {
                "PASS"
            } else {
                "OVER"
            }
        );
        println!(
            "  max:  {}",
            if max <= VOTOR_DELTA_BLOCK_MS {
                "PASS"
            } else {
                "OVER"
            }
        );

        assert_eq!(latencies_ms.len(), N_ITERATIONS);
        assert!(latencies_ms.iter().all(|&ms| ms > 0));
    }

    #[test]
    fn measure_pre_phase_slot_timing() {
        // the genesis config
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_slow_genesis_config(1_000_000_000);

        // create the bank and transaction recorder
        let (bank, _forks) = Bank::new_with_bank_forks_for_tests(&genesis_config);

        let (record_sender, mut record_receiver) = record_channels(false);
        let recorder = TransactionRecorder::new(record_sender);
        record_receiver.restart(bank.bank_id());

        // create the replay vote sender and committer
        let (replay_vote_sender, _) = unbounded();
        let committer = Committer::new(None, replay_vote_sender, None);

        // create the consumer
        let consumer = Consumer::new(committer, recorder, None);

        let mut load_exec_all = Vec::with_capacity(N_ITERATIONS);
        let mut freeze_all = Vec::with_capacity(N_ITERATIONS);
        let mut record_all = Vec::with_capacity(N_ITERATIONS);
        let mut commit_all = Vec::with_capacity(N_ITERATIONS);

        for i in 0..N_ITERATIONS {
            let start_hash = bank.last_blockhash();
            let recipient = solana_pubkey::new_rand();
            let tx = system_transaction::transfer(&mint_keypair, &recipient, 1, start_hash);
            let txs = sanitize_transactions(vec![tx]);
            let output = consumer.process_and_record_transactions(&bank, &txs);

            let t = &output
                .execute_and_commit_transactions_output
                .execute_and_commit_timings;

            println!(
                "[iter {:>2}]  load_execute={:>6}µs  freeze_lock={:>6}µs  record={:>6}µs  \
                 commit={:>6}µs",
                i, t.load_execute_us, t.freeze_lock_us, t.record_us, t.commit_us,
            );

            load_exec_all.push(t.load_execute_us);
            freeze_all.push(t.freeze_lock_us);
            record_all.push(t.record_us);
            commit_all.push(t.commit_us);
        }

        println!("\nLeaderExecuteAndCommitTimings — {N_ITERATIONS} runs (µs):");
        println!("  phase         min    mean     max  jitter");
        for (name, vals) in [
            ("load_execute", &load_exec_all),
            ("freeze_lock ", &freeze_all),
            ("record      ", &record_all),
            ("commit      ", &commit_all),
        ] {
            let (min, mean, max) = stats(vals);
            println!(
                "  {name}  {:>6}  {:>6}  {:>6}  {:>6}",
                min,
                mean,
                max,
                max.saturating_sub(min)
            );
        }
    }
}
