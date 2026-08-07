// Package main implements lzt-broker-stall-reaper.
//
// Watches the GitHub Actions self-hosted runner's TCP connection(s) to
// broker.actions.githubusercontent.com:443. If a connection stays ESTABLISHED
// but receives zero bytes for >STALL_THRESHOLD_SEC while the runner process
// is alive and idle, force-closes the socket via `ss --kill` and optionally
// restarts the systemd runner service.
//
// Emits Prometheus textfile metrics to PROMETHEUS_TEXTFILE_DIR.
//
// Usage:
//
//	lzt-broker-stall-reaper -once          # single check, exit 0
//	lzt-broker-stall-reaper -interval=30s  # daemon mode, default
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

var (
	interval      = flag.Duration("interval", 30*time.Second, "watch loop period")
	stallThresh   = flag.Duration("stall", 5*time.Minute, "no-bytes-received threshold")
	runnerProc    = flag.String("runner-proc", "Runner.Listener", "substring of the runner process command line")
	runnerSvc     = flag.String("runner-service", "actions.runner.LOUST-PRO.vps-runner-1.service", "systemd service to restart after kill")
	stateDir      = flag.String("state-dir", "/var/run/lzt-broker-stall-reaper", "state directory (per-connection last-bytes-received files)")
	promDir       = flag.String("prom-dir", "/var/lib/prometheus/node-exporter", "Prometheus textfile collector directory")
	brokerHost    = flag.String("broker-host", "20.85.130.105", "broker IPv4 address or hostname")
	brokerPort    = flag.String("broker-port", "443", "broker TCP port")
	appStallCtr   = flag.String("app-stall-counter-file", "/var/run/lzt-runner-stall-counter", "path to C#-patch stall counter; > 0 forces kill regardless of TCP bytes")
	once          = flag.Bool("once", false, "single check, exit 0")
	restartCooldn = flag.Duration("restart-cooldown", 30*time.Minute, "min time between runner service restarts")
	logToJournald = flag.Bool("syslog", true, "log via logger(1) to syslog/journald")
)

type connState struct {
	bytesReceived int64
	lastUpdate    time.Time
	lastSeen      time.Time
}

func main() {
	flag.Parse()

	if err := os.MkdirAll(*stateDir, 0o755); err != nil {
		log.Fatalf("state dir: %v", err)
	}
	if err := os.MkdirAll(*promDir, 0o755); err != nil {
		log.Fatalf("prom dir: %v", err)
	}

	if *once {
		check()
		return
	}

	logf("starting daemon interval=%s stall=%s", *interval, *stallThresh)
	t := time.NewTicker(*interval)
	defer t.Stop()
	check()
	for range t.C {
		check()
	}
}

// check is one iteration: enumerate runner→broker connections, update state,
// emit metrics, kill stalled sockets.
func check() {
	now := time.Now()

	appStall := readAppStallCounter(*appStallCtr)
	if appStall > 0 {
		logf("app-stall-counter=%d forcing kill on all matched conns", appStall)
	}

	conns, err := listRunnerToBrokerConns()
	if err != nil {
		logf("list conns: %v", err)
		return
	}

	// Load previous state per inode
	prev := loadState()

	// Per-connection tracking
	current := make(map[string]*connState)
	var stalled []string

	for _, c := range conns {
		cs := &connState{
			bytesReceived: c.bytesReceived,
			lastUpdate:    now,
			lastSeen:      now,
		}
		current[c.inode] = cs

		if p, ok := prev[c.inode]; ok {
			if c.bytesReceived == p.bytesReceived {
				stallDuration := now.Sub(p.lastUpdate)
				if stallDuration > *stallThresh {
					stalled = append(stalled, c.inode)
				}
			}
		}

		// If C# patch reported a stall, force-kill this connection
		if appStall > 0 {
			stalled = append(stalled, c.inode)
		}
	}

	// Persist current state
	saveState(current)

	// Take action on stalled connections
	killed := killStalledConns(stalled)

	// Restart runner service if connections were killed and still no job
	restarted := false
	if killed > 0 {
		restarted = maybeRestartRunner(now)
	}

	emitMetrics(metrics{
		runnerProc:     *runnerProc,
		brokerHost:     *brokerHost,
		brokerPort:     *brokerPort,
		connCount:      len(conns),
		stallCount:     len(stalled),
		killedCount:    killed,
		restarted:      restarted,
		lastRun:        now.Unix(),
		hasWorker:      workerProcessExists(),
		appStallCount:  appStall,
	})
}

type metrics struct {
	runnerProc, brokerHost, brokerPort string
	connCount, stallCount, killedCount int
	restarted                          bool
	lastRun                            int64
	hasWorker                          bool
	appStallCount                      int64
}

func emitMetrics(m metrics) {
	path := filepath.Join(*promDir, "lzt-broker-stall-reaper.prom")
	f, err := os.Create(path)
	if err != nil {
		logf("create prom: %v", err)
		return
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	fmt.Fprintf(w, "# HELP lzt_broker_stall_reaper_conn_count Connections runner→broker currently ESTABLISHED\n")
	fmt.Fprintf(w, "# TYPE lzt_broker_stall_reaper_conn_count gauge\n")
	fmt.Fprintf(w, "lzt_broker_stall_reaper_conn_count{runner_proc=%q,broker=%s:%s} %d\n",
		m.runnerProc, m.brokerHost, m.brokerPort, m.connCount)

	fmt.Fprintf(w, "# HELP lzt_broker_stall_reaper_stalled_count Connections stalled (no bytes received in stall window)\n")
	fmt.Fprintf(w, "# TYPE lzt_broker_stall_reaper_stalled_count gauge\n")
	fmt.Fprintf(w, "lzt_broker_stall_reaper_stalled_count{runner_proc=%q,broker=%s:%s} %d\n",
		m.runnerProc, m.brokerHost, m.brokerPort, m.stallCount)

	fmt.Fprintf(w, "# HELP lzt_broker_stall_reaper_killed_total Sockets force-killed since startup\n")
	fmt.Fprintf(w, "# TYPE lzt_broker_stall_reaper_killed_total counter\n")
	fmt.Fprintf(w, "lzt_broker_stall_reaper_killed_total{runner_proc=%q,broker=%s:%s} %d\n",
		m.runnerProc, m.brokerHost, m.brokerPort, m.killedCount)

	fmt.Fprintf(w, "# HELP lzt_broker_stall_reaper_restarted 1 if runner service was restarted this cycle\n")
	fmt.Fprintf(w, "# TYPE lzt_broker_stall_reaper_restarted gauge\n")
	fmt.Fprintf(w, "lzt_broker_stall_reaper_restarted{runner_proc=%q,broker=%s:%s} %d\n",
		m.runnerProc, m.brokerHost, m.brokerPort, boolToInt(m.restarted))

	fmt.Fprintf(w, "# HELP lzt_broker_stall_reaper_last_run_timestamp Unix timestamp of last check\n")
	fmt.Fprintf(w, "# TYPE lzt_broker_stall_reaper_last_run_timestamp gauge\n")
	fmt.Fprintf(w, "lzt_broker_stall_reaper_last_run_timestamp %d\n", m.lastRun)

	fmt.Fprintf(w, "# HELP lzt_broker_stall_reaper_worker_present 1 if a Runner.Worker process is alive (job in flight)\n")
	fmt.Fprintf(w, "# TYPE lzt_broker_stall_reaper_worker_present gauge\n")
	fmt.Fprintf(w, "lzt_broker_stall_reaper_worker_present %d\n", boolToInt(m.hasWorker))

	fmt.Fprintf(w, "# HELP lzt_broker_stall_reaper_app_stall_count C# patch stall counter value (0=healthy, >0=broker returning nulls)\n")
	fmt.Fprintf(w, "# TYPE lzt_broker_stall_reaper_app_stall_count gauge\n")
	fmt.Fprintf(w, "lzt_broker_stall_reaper_app_stall_count %d\n", m.appStallCount)

	if err := w.Flush(); err != nil {
		logf("write prom: %v", err)
	}
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// connInfo is one runner→broker TCP4 connection.
type connInfo struct {
	inode         string
	bytesReceived int64
}

// listRunnerToBrokerConns runs `ss -tni dst <broker>:<port>` and parses every
// ESTABLISHED entry. We drop `ss -p` because iproute2 omits users:(...) info
// when CAP_DAC_OVERRIDE is not in the calling process permitted set — which
// is the case under the systemd unit's CapabilityBoundingSet restriction.
// On a single-runner VPS these are necessarily the runner's connections.
func listRunnerToBrokerConns() ([]connInfo, error) {
	out, err := exec.Command("ss", "-tni", "dst", *brokerHost+":"+*brokerPort).Output()
	if err != nil {
		return nil, fmt.Errorf("ss: %w", err)
	}

	var conns []connInfo
	sc := bufio.NewScanner(strings.NewReader(string(out)))
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "ESTAB") {
			continue
		}
		ci := parseConnInfo(line)
		if ci != nil {
			conns = append(conns, *ci)
		}
	}
	return conns, nil
}

// parseConnInfo extracts inode + bytes_received from an `ss` line.
// Example:
//
//	ESTAB 0 0 167.88.38.25:19318 20.85.130.105:443 users:(("Runner.Listener",pid=407003,fd=12)) ...
//	  bbr wscale:10,9 rto:216 rtt:15.645/2.359 ato:40 mss:1436 pmtu:1500 rcvmss:1436 advmss:1460
//	  cwnd:18 bytes_sent:10205 bytes_acked:10206 bytes_received:6128 ...
func parseConnInfo(line string) *connInfo {
	// Inode from `users:(("name",pid=N,fd=N))` is in /proc/<pid>/fd/<N>.
	// ss doesn't print inode directly. We need to map fd → inode via /proc/<pid>/fd/.
	// For simplicity, use the local_port as a unique-enough key for ESTAB to broker:443
	// (only one runner connection to that endpoint expected at a time).
	fields := strings.Fields(line)
	if len(fields) < 4 {
		return nil
	}
	localAddr := fields[3]
	ci := &connInfo{inode: localAddr} // use local_addr as key
	// extract bytes_received:N
	for _, f := range strings.Fields(line) {
		if strings.HasPrefix(f, "bytes_received:") {
			v := strings.TrimPrefix(f, "bytes_received:")
			n, err := strconv.ParseInt(v, 10, 64)
			if err == nil {
				ci.bytesReceived = n
			}
		}
	}
	return ci
}

func loadState() map[string]*connState {
	out := make(map[string]*connState)
	files, err := filepath.Glob(filepath.Join(*stateDir, "*.state"))
	if err != nil {
		return out
	}
	for _, fp := range files {
		key := strings.TrimSuffix(filepath.Base(fp), ".state")
		data, err := os.ReadFile(fp)
		if err != nil {
			continue
		}
		parts := strings.SplitN(strings.TrimSpace(string(data)), "|", 2)
		if len(parts) != 2 {
			continue
		}
		br, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			continue
		}
		ts, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			continue
		}
		out[key] = &connState{
			bytesReceived: br,
			lastUpdate:    time.Unix(ts, 0),
			lastSeen:      time.Unix(ts, 0),
		}
	}
	return out
}

func saveState(s map[string]*connState) {
	// Clean up state files for connections that are no longer present
	existing, _ := filepath.Glob(filepath.Join(*stateDir, "*.state"))
	for _, fp := range existing {
		key := strings.TrimSuffix(filepath.Base(fp), ".state")
		if _, ok := s[key]; !ok {
			os.Remove(fp)
		}
	}
	for k, cs := range s {
		path := filepath.Join(*stateDir, k+".state")
		data := fmt.Sprintf("%d|%d", cs.bytesReceived, cs.lastUpdate.Unix())
		os.WriteFile(path, []byte(data), 0o644)
	}
}

// killStalledConns sends `ss --kill` for each stalled connection's local addr.
// Returns count of connections successfully killed.
func killStalledConns(inodes []string) int {
	killed := 0
	for _, inode := range inodes {
		// Inode is local_addr:port format. Build a ss filter.
		// `ss --kill state established dst 20.85.130.105:443`
		// Note: ss --kill may need root/CAP_NET_ADMIN.
		cmd := exec.Command("ss", "--kill", "state", "established",
			"dst", *brokerHost+":"+*brokerPort)
		// ss --kill doesn't accept per-inode filter; it kills all matching.
		// We accept over-kill: any stalled conn → kill all matching.
		out, err := cmd.CombinedOutput()
		if err != nil {
			logf("ss --kill (inode=%s): %v out=%s", inode, err, strings.TrimSpace(string(out)))
			continue
		}
		killed++
		logf("killed stalled connection inode=%s", inode)
	}
	return killed
}

func maybeRestartRunner(now time.Time) bool {
	// Cooldown: check last restart file
	lastRestartFile := filepath.Join(*stateDir, "last_restart")
	if data, err := os.ReadFile(lastRestartFile); err == nil {
		if ts, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64); err == nil {
			if now.Sub(time.Unix(ts, 0)) < *restartCooldn {
				logf("restart cooldown active, skipping")
				return false
			}
		}
	}
	// Only restart if no worker is alive (otherwise we abort a real job)
	if workerProcessExists() {
		logf("worker process alive, not restarting")
		return false
	}
	logf("restarting runner service %s", *runnerSvc)
	cmd := exec.Command("sudo", "systemctl", "restart", *runnerSvc)
	if out, err := cmd.CombinedOutput(); err != nil {
		logf("restart: %v out=%s", err, strings.TrimSpace(string(out)))
		return false
	}
	os.WriteFile(lastRestartFile, []byte(strconv.FormatInt(now.Unix(), 10)), 0o644)
	return true
}

func workerProcessExists() bool {
	out, err := exec.Command("pgrep", "-fc", "Runner.Worker").Output()
	if err != nil {
		return false
	}
	n, err := strconv.Atoi(strings.TrimSpace(string(out)))
	if err != nil {
		return false
	}
	return n > 0
}

func logf(format string, a ...interface{}) {
	msg := fmt.Sprintf(format, a...)
	if *logToJournald {
		// Best-effort logger(1) to syslog with tag
		_ = exec.Command("logger", "-t", "lzt-broker-stall-reaper", msg).Run()
	}
	log.Println(msg)
}

// readAppStallCounter parses the C#-patch stall counter file. Returns the
// stall signal value: > 0 means force-kill needed.
//
// Supports two formats written by runner-1's LZT stall detector:
//   - Pure integer (e.g., "1\n"): C# patch wrote File.WriteAllText with
//     just a count. If > 0, file is truncated to "0" after read (consumed).
//   - Structured log lines (current C# patch, AppendAllText):
//     "{ISO8601 timestamp}|stall_detected|threshold=N|time_limit_sec=N"
//     Multiple lines append over time. Presence of any |stall_detected|
//     marker triggers force-kill. File is NOT truncated.
func readAppStallCounter(path string) int64 {
	if path == "" {
		return 0
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	content := strings.TrimSpace(string(data))
	if content == "" {
		return 0
	}

	// Format 1: pure integer. Reject if content also contains "|"
	// (otherwise Sscanf would happily parse "2026" from "2026-...|stall...")
	var n int64
	if _, err := fmt.Sscanf(content, "%d", &n); err == nil && !strings.Contains(content, "|") {
		_ = os.WriteFile(path, []byte("0\n"), 0o644)
		return n
	}

	// Format 2: structured log lines. Count |stall_detected| occurrences.
	var stallLines int64
	for _, line := range strings.Split(content, "\n") {
		if strings.Contains(line, "|stall_detected|") {
			stallLines++
		}
	}
	return stallLines
}