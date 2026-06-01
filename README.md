# Banking Jitter Harness

Two test harnesses measuring Agave's banking-stage timing against Votor's slot budget — one for end-to-end latency, one for per-phase breakdown (load_execute_us, freeze_lock_us, record_us, commit_us).

## Problem

In this harness, we need to extract the votor's per-round timing budget and build a harness measuring agave's banking stage against it.

## The Knowledge Required:

Before moving on to the code part we need to have a good knowledge of the `banking_stage`, `consumer` and also about the `Votor`.

- Banking stage = Is for block production which resides on the TPU side.
- Consumer = This turns scheduled transactions into committed entries.
- Votor = It is the main part of the Alpenglow consensus protocol, where the actual voting and consensus logic occurs. But all this needs to happen inside the slot deadline.

## Votor Budget:

As mentioned earlier, the Votor budget is the amount of time allowed for the Votor to perform its operations within the slot deadline. And this is particularly mentioned in the Alpenglow whitepaper, Which goes like:

```bash
Timeout(i) = delta_timeout + delta_block

where
- delta_block = 400ms, the protocol-specified block time (normal Solana block time).
- delta_timeout ≤ 3 * delta, a conservative bound from the paper, made of two parts:
    - time for the leader to observe certificates + time for block dissemination through Rotor.
- delta = network latency of a staked node (we assume 50ms).

So delta_timeout = 3 * 50 = 150ms, and Timeout(i) = delta_timeout + delta_block = 550ms.
```

- Banking-stage production is judged against delta_block (400ms); the full Timeout(i) = 550ms is the consensus-layer deadline.

## My Approach:

- I have two harnesses in this project, one for the banking stage latency and the other pre-phase metrics through consumer
- The entire project is completely build inside the `agave` repository. as we can't get all the dependencies locally, we need to build everything inside the `agave` repository.
- Building the banking stage latency harness is straightforward as we just need to build the `bank` and simulate the `banking_stage` we can get all the required metrics by looping through it.
- But the `pre-phase` harness took more design decision to simulate as these consumer methods and all not public and we can't call them directly from the harness.
- What i exactly did was expose `consumer` and `committer` mods + the `execute_and_commit_timings` field via a dev-context-only-utils gated change (reverted after measuring).

- so by this i was able to simulate the pre-phase stage and get all the required metrics.

## Results:

The command to run these tests is `cargo test --p banking-jitter -- --nocapture`.

By running these test harness we get these results:

- `measure_banking_stage_slot_timing()`

```bash
[iter  0] send to commit:   32 ms
[iter  1] send to commit:    1 ms
[iter  2] send to commit:    2 ms
[iter  3] send to commit:    2 ms
[iter  4] send to commit:    2 ms
[iter  5] send to commit:    2 ms
[iter  6] send to commit:    1 ms
[iter  7] send to commit:    1 ms
[iter  8] send to commit:    2 ms
[iter  9] send to commit:    1 ms

Banking Jitter Report (10 runs)

from send to committed entry (ms):
  min 1  mean 4  max 32  jitter 31

Votor timeout: 550
Votor budget (Delta_block = 400 ms):
  mean: PASS
  max:  PASS
```

- `measure_pre_phase_slot_timing()`

```bash
[iter  0]  load_execute=  1573µs  freeze_lock=     0µs  record=   108µs  commit=   814µs
[iter  1]  load_execute=   115µs  freeze_lock=     0µs  record=    15µs  commit=    32µs
[iter  2]  load_execute=    81µs  freeze_lock=     0µs  record=    13µs  commit=    26µs
[iter  3]  load_execute=    74µs  freeze_lock=     0µs  record=    13µs  commit=    35µs
[iter  4]  load_execute=    68µs  freeze_lock=     0µs  record=    13µs  commit=    24µs
[iter  5]  load_execute=    79µs  freeze_lock=     0µs  record=    12µs  commit=    22µs
[iter  6]  load_execute=    63µs  freeze_lock=     0µs  record=    13µs  commit=    22µs
[iter  7]  load_execute=    62µs  freeze_lock=     0µs  record=    12µs  commit=    29µs
[iter  8]  load_execute=    58µs  freeze_lock=     0µs  record=    12µs  commit=    24µs
[iter  9]  load_execute=    61µs  freeze_lock=     0µs  record=    12µs  commit=    25µs

LeaderExecuteAndCommitTimings — 10 runs (µs):
  phase         min    mean     max  jitter
  load_execute      58     223    1573    1515
  freeze_lock        0       0       0       0
  record            12      22     108      96
  commit            22     105     814     792
```

## Diagnosis

- The spike is on iter 0 only: first run pays for lazy program loading, cache warming, and allocator growth.
- It lands in load_execute and commit because those do the SVM execution and state writes (CPU + allocator bound), not record (I/O/PoH) or freeze_lock (contention).
- Steady-state banking is ~150µs total, far inside the 400ms Δ_block budget. So under normal conditions banking comfortably meets Votor's deadline; jitter only appears at cold start.

## Limitations

- Single-tx batches.
- Isolated Consumer.
- No multi-thread scheduler.
- `delta` is an assumption of 50ms.
