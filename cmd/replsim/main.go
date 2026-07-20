// Command replsim is a self-contained CLI for the replication-strategies
// simulator. It embeds the orchestrator in-process (no HTTP server) so you can
// explore scenarios, real-system presets, and correctness checks straight from
// the terminal.
//
// Usage:
//
//	replsim list-scenarios
//	replsim list-presets
//	replsim run -scenario ReplicationLag
//	replsim check -strategy leaderless -nodes 5 -w 3 -r 3
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"replication-strategies/internal/events"
	"replication-strategies/internal/node"
	"replication-strategies/internal/quorum"
	"replication-strategies/internal/simulation"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

// run dispatches on the first argument (the subcommand) and returns the process
// exit code. Splitting this out of main keeps os.Exit at a single call site and
// makes the dispatcher testable.
func run(args []string) int {
	if len(args) < 1 {
		usage()
		return 2
	}

	// A fresh orchestrator backed by an in-process event bus. No server, no ports.
	orch := simulation.NewOrchestrator(events.NewEventBus(1024))

	switch args[0] {
	case "list-scenarios":
		return cmdListScenarios(args[1:])
	case "list-presets":
		return cmdListPresets(args[1:])
	case "run":
		return cmdRun(orch, args[1:])
	case "check":
		return cmdCheck(orch, args[1:])
	case "-h", "--help", "help":
		usage()
		return 0
	default:
		fmt.Fprintf(os.Stderr, "replsim: unknown subcommand %q\n\n", args[0])
		usage()
		return 2
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `replsim — in-process CLI for the replication-strategies simulator

Usage:
  replsim <subcommand> [flags]

Subcommands:
  list-scenarios          List the built-in teaching scenarios.
  list-presets            List the real-system presets (Cassandra, etcd, ...).
  run -scenario NAME      Run a scenario, then print convergence,
                          linearizability, and invariant reports.
  check -strategy S -nodes N [-w W -r R]
                          Provision a cluster, do a few writes+reads, then
                          assert the always-on invariants. Exits non-zero on
                          any invariant violation.

Examples:
  replsim list-scenarios
  replsim run -scenario ReplicationLag
  replsim check -strategy leaderless -nodes 5 -w 3 -r 3
`)
}

// cmdListScenarios prints the built-in scenario catalog.
func cmdListScenarios(args []string) int {
	fs := flag.NewFlagSet("list-scenarios", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return 2
	}
	fmt.Printf("Scenarios (%d):\n\n", len(simulation.Scenarios))
	for _, s := range simulation.Scenarios {
		fmt.Printf("  %-22s [%s, %d nodes]\n", s.Name, s.Strategy, s.NodeCount)
		fmt.Printf("      %s\n\n", s.Description)
	}
	return 0
}

// cmdListPresets prints the real-system presets.
func cmdListPresets(args []string) int {
	fs := flag.NewFlagSet("list-presets", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return 2
	}
	presets := simulation.ListPresets()
	fmt.Printf("Presets (%d):\n\n", len(presets))
	for _, p := range presets {
		fmt.Printf("  %-14s %s\n", p.Name, p.System)
		fmt.Printf("      %s\n\n", p.Description)
	}
	return 0
}

// cmdRun creates and runs the named scenario, waits for its background setup to
// play out, then prints the three correctness reports. Exits 0 when invariants
// hold, 1 otherwise.
func cmdRun(orch *simulation.Orchestrator, args []string) int {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	scenario := fs.String("scenario", "", "name of the scenario to run (see list-scenarios)")
	wait := fs.Duration("wait", 2*time.Second, "how long to let the scenario play out before checking")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *scenario == "" {
		fmt.Fprintln(os.Stderr, "replsim run: -scenario is required")
		names := make([]string, 0, len(simulation.Scenarios))
		for _, s := range simulation.Scenarios {
			names = append(names, s.Name)
		}
		fmt.Fprintf(os.Stderr, "available: %s\n", strings.Join(names, ", "))
		return 2
	}

	clusterID, err := orch.RunScenario(*scenario)
	if err != nil {
		fmt.Fprintf(os.Stderr, "replsim run: %v\n", err)
		return 2
	}
	fmt.Printf("Running scenario %q (cluster %s)\n", *scenario, clusterID)
	fmt.Printf("Waiting %s for the scenario to play out...\n\n", *wait)
	time.Sleep(*wait)

	conv, err := orch.CheckConvergence(clusterID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "replsim run: convergence check: %v\n", err)
		return 2
	}
	printConvergence(conv)

	lin, err := orch.CheckLinearizable(clusterID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "replsim run: linearizability check: %v\n", err)
		return 2
	}
	printLinearizability(lin)

	inv, err := orch.CheckInvariants(clusterID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "replsim run: invariant check: %v\n", err)
		return 2
	}
	printInvariants(inv)

	if inv.OK {
		return 0
	}
	return 1
}

// cmdCheck provisions a cluster from the given flags, runs a short write/read
// workload, and asserts the invariants. Exits non-zero on any violation.
func cmdCheck(orch *simulation.Orchestrator, args []string) int {
	fs := flag.NewFlagSet("check", flag.ContinueOnError)
	strategy := fs.String("strategy", "single_leader", "replication strategy: single_leader|multi_leader|leaderless|raft")
	nodes := fs.Int("nodes", 3, "number of nodes in the cluster")
	w := fs.Int("w", 0, "write quorum W (leaderless only; 0 = default)")
	r := fs.Int("r", 0, "read quorum R (leaderless only; 0 = default)")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	cfg := simulation.ClusterConfig{
		Strategy:  node.ReplicationStrategy(*strategy),
		NodeCount: *nodes,
	}
	if cfg.Strategy == node.StrategyLeaderless {
		cfg.QuorumN = *nodes
		cfg.QuorumW = *w
		cfg.QuorumR = *r
	}

	ok, report := runCheck(orch, cfg)
	fmt.Print(report)
	if ok {
		return 0
	}
	return 1
}

// runCheck provisions a cluster from cfg, performs a few writes and reads with a
// single client, then evaluates the always-on invariants. It returns whether the
// invariants held together with a human-readable report. Factored out of cmdCheck
// so it can be exercised directly in tests without touching os.Exit or flags.
func runCheck(orch *simulation.Orchestrator, cfg simulation.ClusterConfig) (bool, string) {
	var b strings.Builder

	fmt.Fprintf(&b, "check: strategy=%s nodes=%d", cfg.Strategy, cfg.NodeCount)
	if cfg.Strategy == node.StrategyLeaderless {
		q := quorum.QuorumConfig{N: cfg.QuorumN, W: cfg.QuorumW, R: cfg.QuorumR}
		if q.W == 0 {
			q.W = q.N/2 + 1
		}
		if q.R == 0 {
			q.R = q.N/2 + 1
		}
		fmt.Fprintf(&b, " N=%d W=%d R=%d", q.N, q.W, q.R)
		if q.IsStronglyConsistent() {
			fmt.Fprintf(&b, " (strong: W+R>N, overlap=%d)", q.OverlapCount())
		} else {
			fmt.Fprintf(&b, " (eventual: W+R<=N, ~%.1f%% stale-read risk)", q.StaleReadProbability()*100)
		}
	}
	b.WriteString("\n\n")

	cluster, err := orch.CreateCluster(cfg)
	if err != nil {
		fmt.Fprintf(&b, "FAIL: could not create cluster: %v\n", err)
		return false, b.String()
	}
	defer func() { _ = orch.DeleteCluster(cluster.ID) }()

	// Give leaderless/raft clusters a moment to elect/settle before traffic.
	time.Sleep(300 * time.Millisecond)

	const clientID = "replsim-check"
	const nWrites = 5
	for i := 0; i < nWrites; i++ {
		key := fmt.Sprintf("check-key-%d", i)
		val := []byte(fmt.Sprintf("value-%d", i))
		if _, werr := orch.Write(context.Background(), cluster.ID, "", key, val, clientID); werr != nil {
			fmt.Fprintf(&b, "FAIL: write %s: %v\n", key, werr)
			return false, b.String()
		}
	}

	// Let asynchronous replication / read-repair converge.
	time.Sleep(500 * time.Millisecond)

	for i := 0; i < nWrites; i++ {
		key := fmt.Sprintf("check-key-%d", i)
		if _, werr := orch.Read(context.Background(), cluster.ID, "", key, clientID); werr != nil {
			fmt.Fprintf(&b, "FAIL: read %s: %v\n", key, werr)
			return false, b.String()
		}
	}

	// Allow any read-repair triggered by the reads to settle before checking.
	time.Sleep(300 * time.Millisecond)

	inv, err := orch.CheckInvariants(cluster.ID)
	if err != nil {
		fmt.Fprintf(&b, "FAIL: invariant check: %v\n", err)
		return false, b.String()
	}

	fmt.Fprintf(&b, "wrote %d keys, read them back, then checked invariants:\n", nWrites)
	fmt.Fprintf(&b, "  converged:    %v\n", inv.Converged)
	fmt.Fprintf(&b, "  linearizable: %v\n", inv.Linearizable)
	for _, v := range inv.Violations {
		fmt.Fprintf(&b, "  violation:    %s\n", v)
	}
	if inv.OK {
		b.WriteString("\nPASS: all invariants hold\n")
	} else {
		b.WriteString("\nFAIL: invariant violation(s) detected\n")
	}
	return inv.OK, b.String()
}

func printConvergence(rep simulation.ConvergenceReport) {
	fmt.Println("Convergence:")
	fmt.Printf("  converged: %v  (keys=%d)\n", rep.Converged, rep.Keys)
	if rep.Note != "" {
		fmt.Printf("  note: %s\n", rep.Note)
	}
	for _, d := range rep.Diverged {
		fmt.Printf("  diverged key %q:\n", d.Key)
		for nodeID, v := range d.Values {
			fmt.Printf("      %s = %s\n", nodeID, v)
		}
	}
	fmt.Println()
}

func printLinearizability(rep simulation.LinearizabilityReport) {
	fmt.Println("Linearizability:")
	fmt.Printf("  linearizable: %v  (ops=%d)\n", rep.Linearizable, rep.Ops)
	if rep.Note != "" {
		fmt.Printf("  note: %s\n", rep.Note)
	}
	if rep.Violation != nil {
		fmt.Printf("  violation: %s of key %q (value %q) by client %s\n",
			rep.Violation.Kind, rep.Violation.Key, rep.Violation.Value, rep.Violation.ClientID)
	}
	fmt.Println()
}

func printInvariants(rep simulation.InvariantReport) {
	fmt.Println("Invariants:")
	fmt.Printf("  ok:           %v\n", rep.OK)
	fmt.Printf("  converged:    %v\n", rep.Converged)
	fmt.Printf("  linearizable: %v\n", rep.Linearizable)
	for _, v := range rep.Violations {
		fmt.Printf("  violation:    %s\n", v)
	}
	fmt.Println()
}
