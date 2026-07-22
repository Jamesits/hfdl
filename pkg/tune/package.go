// Package tune implements the adaptive download-concurrency controller: a
// hill-climbing state machine that seeds the process-wide connection budget at
// half the configured ceiling and then grows it while extra connections keep
// raising the measured global rate, holding (or reverting) otherwise.
//
// The controller is pure and clock-driven: the caller (sched's tune loop)
// samples the world once per tick and feeds it to Observe, which returns the
// budget to enforce. Probes work by raising the budget, waiting a settle
// window long enough for the caller's windowed rate to fully reflect the new
// budget, then comparing against the pre-probe baseline: an insufficient
// gain reverts the budget and backs the next probe off exponentially, so a
// link at capacity (or a configured bandwidth ceiling) is not permanently
// churned by probe/revert cycles.
//
// Probing is skipped entirely while the bandwidth bucket reports the
// configured ceiling as the active constraint, while the current budget is
// not even fully assigned (more connections could not help), and for a
// freeze window after a stall kill (the kill's own rate perturbation would
// poison the measurement).
package tune
