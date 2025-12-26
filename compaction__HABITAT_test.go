package pebble

import (
	"bytes"
	"context"
	"fmt"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
	"math/rand/v2"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestCompaction__HABITAT(t *testing.T) {
	var mem vfs.FS
	var d *DB
	defer func() {
		if d != nil {
			require.NoError(t, closeAllSnapshots(d))
			require.NoError(t, d.Close())
		}
	}()

	seed := uint64(time.Now().UnixNano())
	rng := rand.New(rand.NewPCG(0, seed))
	t.Logf("seed: %d", seed)

	randVersion := func(min, max FormatMajorVersion) FormatMajorVersion {
		return FormatMajorVersion(int(min) + rng.IntN(int(max)-int(min)+1))
	}

	var compactionLog bytes.Buffer
	compactionLogEventListener := &EventListener{
		CompactionEnd: func(info CompactionInfo) {
			// Ensure determinism.
			info.JobID = 1
			info.Duration = time.Second
			info.TotalDuration = time.Second
			fmt.Fprintln(&compactionLog, info.String())
		},
	}
	reset := func(minVersion, maxVersion FormatMajorVersion) {
		compactionLog.Reset()
		if d != nil {
			require.NoError(t, closeAllSnapshots(d))
			require.NoError(t, d.Close())
		}
		mem = vfs.NewMem()
		require.NoError(t, mem.MkdirAll("ext", 0755))

		opts := &Options{
			FS:                          mem,
			DebugCheck:                  DebugCheckLevels,
			DisableAutomaticCompactions: true,
			EventListener:               compactionLogEventListener,
			FormatMajorVersion:          randVersion(minVersion, maxVersion),
		}
		opts.WithFSDefaults()
		opts.Experimental.EnableColumnarBlocks = func() bool { return true }
		opts.Experimental.CompactionScheduler = NewConcurrencyLimitSchedulerWithNoPeriodicGrantingForTest()

		var err error
		d, err = Open("", opts)
		require.NoError(t, err)
	}

	// d.mu must be held when calling.
	createOngoingCompaction := func(start, end []byte, startLevel, outputLevel int) (ongoingCompaction *tableCompaction) {
		ongoingCompaction = &tableCompaction{
			inputs: []compactionLevel{{level: startLevel}, {level: outputLevel}},
			bounds: base.UserKeyBoundsInclusive(start, end),
		}
		ongoingCompaction.startLevel = &ongoingCompaction.inputs[0]
		ongoingCompaction.outputLevel = &ongoingCompaction.inputs[1]
		// Mark files as compacting.
		curr := d.mu.versions.currentVersion()
		ongoingCompaction.startLevel.files = curr.Overlaps(startLevel, base.UserKeyBoundsInclusive(start, end))
		ongoingCompaction.outputLevel.files = curr.Overlaps(outputLevel, base.UserKeyBoundsInclusive(start, end))
		for _, cl := range ongoingCompaction.inputs {
			for f := range cl.files.All() {
				f.CompactionState = manifest.CompactionStateCompacting
			}
		}
		d.mu.compact.inProgress[ongoingCompaction] = struct{}{}
		d.mu.compact.compactingCount++
		return
	}

	// d.mu must be held when calling.
	deleteOngoingCompaction := func(ongoingCompaction *tableCompaction) {
		for _, cl := range ongoingCompaction.inputs {
			for f := range cl.files.All() {
				f.CompactionState = manifest.CompactionStateNotCompacting
			}
		}
		delete(d.mu.compact.inProgress, ongoingCompaction)
		d.mu.compact.compactingCount--
	}

	runTest := func(t *testing.T, testData string, minVersion, maxVersion FormatMajorVersion, verbose bool) {
		reset(minVersion, maxVersion)
		var ongoingCompaction *tableCompaction
		datadriven.RunTest(t, testData, func(t *testing.T, td *datadriven.TestData) string {
			switch td.Cmd {
			case "reset":
				reset(minVersion, maxVersion)
				return ""

			case "batch":
				b := d.NewIndexedBatch()
				if err := runBatchDefineCmd(td, b); err != nil {
					return err.Error()
				}
				require.NoError(t, b.Commit(nil))
				return ""

			case "build":
				if err := runBuildCmd(td, d, mem); err != nil {
					return err.Error()
				}
				return ""

			case "compact":
				if err := runCompactCmd(td, d); err != nil {
					return err.Error()
				}
				s := describeLSM(d, verbose)
				if td.HasArg("hide-file-num") {
					re := regexp.MustCompile(`([0-9]*):\[`)
					s = re.ReplaceAllString(s, "[")
				}
				if td.HasArg("hide-size") {
					re := regexp.MustCompile(` size:([0-9]*)`)
					s = re.ReplaceAllString(s, "")
				}
				return s

			case "define":
				if d != nil {
					if err := closeAllSnapshots(d); err != nil {
						return err.Error()
					}
					if err := d.Close(); err != nil {
						return err.Error()
					}
				}

				mem = vfs.NewMem()
				opts := &Options{
					FS:                          mem,
					DebugCheck:                  DebugCheckLevels,
					EventListener:               compactionLogEventListener,
					FormatMajorVersion:          randVersion(minVersion, maxVersion),
					DisableAutomaticCompactions: true,
				}
				opts.WithFSDefaults()
				opts.Experimental.EnableColumnarBlocks = func() bool { return true }
				opts.Experimental.CompactionScheduler = NewConcurrencyLimitSchedulerWithNoPeriodicGrantingForTest()

				var err error
				if d, err = runDBDefineCmd(td, opts); err != nil {
					return err.Error()
				}

				s := d.mu.versions.currentVersion().String()
				if verbose {
					s = d.mu.versions.currentVersion().DebugString()
				}
				if td.HasArg("hide-size") {
					re := regexp.MustCompile(` size:([0-9]*)`)
					s = re.ReplaceAllString(s, "")
				}
				return s

			case "excise-dryrun":
				ve, err := runExciseDryRunCmd(td, d)
				if err != nil {
					td.Fatalf(t, "%s", err.Error())
				}
				return fmt.Sprintf("would excise %d files.\n%s", len(ve.DeletedTables), ve.DebugString(base.DefaultFormatter))

			case "file-sizes":
				return runTableFileSizesCmd(td, d)

			case "flush":
				if err := d.Flush(); err != nil {
					return err.Error()
				}
				return describeLSM(d, verbose)

			case "get":
				return runGetCmd(t, td, d)

			case "ingest":
				if err := runIngestCmd(td, d, mem); err != nil {
					return err.Error()
				}
				d.mu.Lock()
				s := d.mu.versions.currentVersion().String()
				if verbose {
					s = d.mu.versions.currentVersion().DebugString()
				}
				d.mu.Unlock()
				return s

			case "iter":
				// TODO(peter): runDBDefineCmd doesn't properly update the visible
				// sequence number. So we have to use a snapshot with a very large
				// sequence number, otherwise the DB appears empty.
				snap := Snapshot{
					db:     d,
					seqNum: base.SeqNumMax,
				}
				iter, _ := snap.NewIter(nil)
				return runIterCmd(td, iter, true)

			case "lsm":
				return runLSMCmd(td, d)

			case "metrics":
				d.mu.Lock()
				d.waitTableStatsInitialLoad()
				d.mu.Unlock()
				m := d.Metrics()
				return m.StringForTests()

			case "populate":
				b := d.NewBatch()
				runPopulateCmd(t, td, b)
				count := b.Count()
				require.NoError(t, b.Commit(nil))
				return fmt.Sprintf("wrote %d keys\n", count)

			case "auto-compact":
				expectedCount := int64(1)
				td.MaybeScanArgs(t, "count", &expectedCount)
				err := func() error {
					d.mu.Lock()
					defer d.mu.Unlock()
					prevCount := d.mu.versions.metrics.Compact.Count
					prev := d.opts.DisableAutomaticCompactions
					d.opts.DisableAutomaticCompactions = false
					err := try(100*time.Microsecond, 60*time.Second, func() error {
						d.maybeScheduleCompaction()
						for d.mu.compact.compactingCount > 0 {
							d.mu.compact.cond.Wait()
						}
						compactions := d.mu.versions.metrics.Compact.Count - prevCount
						if compactions < expectedCount {
							return errors.Errorf("expectedCount at least %d automatic compaction(s), got %d, total: %d",
								expectedCount, compactions, d.mu.versions.metrics.Compact.Count)
						}
						return nil
					})
					d.opts.DisableAutomaticCompactions = prev
					return err
				}()
				if err != nil {
					return err.Error() + "\n" + describeLSM(d, verbose)
				}
				return describeLSM(d, verbose)

			case "set-disable-auto-compact":
				var v bool
				td.ScanArgs(t, "v", &v)
				d.mu.Lock()
				d.opts.DisableAutomaticCompactions = v
				d.mu.Unlock()
				return ""

			case "async-compact":
				var s string
				ch := make(chan error, 1)
				go func() {
					if err := runCompactCmd(td, d); err != nil {
						ch <- err
						close(ch)
						return
					}
					d.mu.Lock()
					s = d.mu.versions.currentVersion().String()
					d.mu.Unlock()
					close(ch)
				}()

				// Wait until the manual compaction is queued.
				err := try(100*time.Microsecond, 20*time.Second, func() error {
					select {
					case <-ch:
						td.Fatalf(t, "manual compaction did not block for ongoing\n%s", s)
					default:
					}

					d.mu.Lock()
					defer d.mu.Unlock()
					if len(d.mu.compact.manual) == 0 {
						return errors.New("no manual compaction queued")
					}
					return nil
				})
				if err != nil {
					return err.Error()
				}

				// Make sure the manual compaction doesn't complete.
				select {
				case <-ch:
					td.Fatalf(t, "manual compaction did not block for ongoing\n%s", s)
				case <-time.After(10 * time.Millisecond):
				}

				d.mu.Lock()
				deleteOngoingCompaction(ongoingCompaction)
				ongoingCompaction = nil
				d.mu.Unlock()
				d.opts.Experimental.CompactionScheduler.(*ConcurrencyLimitScheduler).
					adjustRunningCompactionsForTesting(-1)
				// If the ongoing compaction conflicted with the manual compaction,
				// the CompactionScheduler may believe there is no waiting compaction.
				// So explicitly call maybeScheduleCompaction.
				d.mu.Lock()
				d.maybeScheduleCompaction()
				d.mu.Unlock()
				if err := <-ch; err != nil {
					return err.Error()
				}
				return "manual compaction blocked until ongoing finished\n" + s

			case "async-compact-with-cancellation":
				// Cancels a manual compaction that is blocked by an ongoing
				// compaction. There can be multiple manual compactions created when
				// parallel is specified.
				var s string
				ch := make(chan error, 1)
				// Number of blocked manual compactions.
				var numBlocked int
				td.ScanArgs(t, "num-blocked", &numBlocked)
				var cancelFunc atomic.Pointer[context.CancelFunc]
				go func() {
					compactFunc, cf, err := runCompactCmdAsync(td, d, true)
					if err == nil {
						cancelFunc.Store(&cf)
						err = compactFunc()
					}
					if err != nil {
						ch <- err
						close(ch)
						return
					}
					d.mu.Lock()
					s = d.mu.versions.currentVersion().String()
					d.mu.Unlock()
					close(ch)
				}()
				var compErr error
				var compDone bool
				err := try(100*time.Microsecond, 20*time.Second, func() error {
					select {
					case compErr = <-ch:
						// Unexpected, since compaction should be blocked.
						compDone = true
						return nil
					default:
					}
					if cancelFunc.Load() == nil {
						return errors.New("not yet attempted to run manual compactions")
					}
					d.mu.Lock()
					defer d.mu.Unlock()
					if len(d.mu.compact.manual) != numBlocked {
						return errors.Errorf("expectedCount %d waiting manual compactions, versus actual %d",
							numBlocked, len(d.mu.compact.manual))
					}
					// Expect to be back to the fake ongoing compactions when the
					// non-blocked manual compactions are done.
					if d.mu.compact.compactingCount != 1 {
						return errors.Errorf("expectedCount 1 ongoing compaction, versus actual %d",
							d.mu.compact.compactingCount)
					}
					return nil
				})
				if err != nil {
					return err.Error()
				}
				if compDone {
					td.Fatalf(t, "manual compaction did not block for ongoing\n%s", s)
				}
				// Cancel the manual compaction.
				(*cancelFunc.Load())()
				// Wait for the cancellation to succeed.
				compErr = <-ch
				if compErr == nil {
					return "manual compaction did not have an error\n" + s
				}
				d.mu.Lock()
				deleteOngoingCompaction(ongoingCompaction)
				ongoingCompaction = nil
				numQueuedManualCompactions := len(d.mu.compact.manual)
				d.mu.Unlock()
				d.opts.Experimental.CompactionScheduler.(*ConcurrencyLimitScheduler).
					adjustRunningCompactionsForTesting(-1)
				return fmt.Sprintf(
					"manual compaction cancelled: %s, current queued compactions: %d\n%s",
					compErr.Error(), numQueuedManualCompactions, s)

			case "add-ongoing-compaction":
				var startLevel int
				var outputLevel int
				var start string
				var end string
				td.ScanArgs(t, "startLevel", &startLevel)
				td.ScanArgs(t, "outputLevel", &outputLevel)
				td.ScanArgs(t, "start", &start)
				td.ScanArgs(t, "end", &end)
				d.mu.Lock()
				ongoingCompaction = createOngoingCompaction([]byte(start), []byte(end), startLevel, outputLevel)
				d.mu.Unlock()
				d.opts.Experimental.CompactionScheduler.(*ConcurrencyLimitScheduler).
					adjustRunningCompactionsForTesting(+1)
				return ""

			case "remove-ongoing-compaction":
				d.mu.Lock()
				deleteOngoingCompaction(ongoingCompaction)
				ongoingCompaction = nil
				d.mu.Unlock()
				d.opts.Experimental.CompactionScheduler.(*ConcurrencyLimitScheduler).
					adjustRunningCompactionsForTesting(-1)

				return ""

			case "set-concurrent-compactions":
				lower := 1
				upper := 1
				td.MaybeScanArgs(t, "max", &upper)
				td.MaybeScanArgs(t, "range", &lower, upper)
				d.opts.CompactionConcurrencyRange = func() (int, int) {
					return lower, upper
				}
				return ""

			case "sstable-properties":
				return runSSTablePropertiesCmd(t, td, d)

			case "wait-pending-table-stats":
				return runTableStatsCmd(td, d)

			case "close-snapshots":
				d.mu.Lock()
				// Re-enable automatic compactions if they were disabled so that
				// closing snapshots can trigger elision-only compactions if
				// necessary.
				d.opts.DisableAutomaticCompactions = false

				var ss []*Snapshot
				l := &d.mu.snapshots
				for i := l.root.next; i != &l.root; i = i.next {
					ss = append(ss, i)
				}
				d.mu.Unlock()
				for i := range ss {
					if err := ss[i].Close(); err != nil {
						return err.Error()
					}
				}
				return ""

			case "compaction-log":
				defer compactionLog.Reset()
				s := compactionLog.String()
				if td.HasArg("sort") {
					lines := strings.Split(s, "\n")
					sort.Strings(lines)
					// Remove empty lines.
					i := 0
					for ; i < len(lines); i++ {
						if len(lines[i]) != 0 {
							break
						}
					}
					lines = lines[i:]
					s = strings.Join(lines, "\n")
				}
				return s

			default:
				return fmt.Sprintf("unknown command: %s", td.Cmd)
			}
		})
	}

	type testConfig struct {
		minVersion FormatMajorVersion // inclusive, FormatMinSupported if unspecified.
		maxVersion FormatMajorVersion // inclusive, internalFormatNewest if unspecified.
		verbose    bool
	}
	testConfigs := map[string]testConfig{
		"singledel_set_with_del": {},
		"range_keys":             {verbose: true},
		"file_boundaries_delsized": {
			minVersion: FormatDeleteSizedAndObsolete,
			maxVersion: FormatFlushableIngestExcises,
		},
		"set_with_del_sstable_Pebblev4": {
			minVersion: FormatDeleteSizedAndObsolete,
			maxVersion: FormatFlushableIngestExcises,
		},
		"multilevel": {},
		"set_with_del_sstable_Pebblev5": {
			minVersion: FormatColumnarBlocks,
			maxVersion: FormatColumnarBlocks,
		},
		"set_with_del_sstable_Pebblev6": {
			minVersion: FormatTableFormatV6,
			maxVersion: FormatTableFormatV6,
		},
		"set_with_del_sstable_Pebblev7": {
			minVersion: formatFooterAttributes,
			maxVersion: formatFooterAttributes,
		},
		"value_separation": {
			minVersion: FormatValueSeparation,
			maxVersion: FormatValueSeparation,
			verbose:    true,
		},
		"score_compaction_picked_before_manual": {
			// Run at a specific version, so that a single sstable format is used,
			// since the test prints the compaction log which includes file sizes.
			minVersion: formatDeprecatedExperimentalValueSeparation,
			maxVersion: formatDeprecatedExperimentalValueSeparation,
		},
		"compaction_cancellation": {
			// Run at a specific version, so that a single sstable format is used,
			// since the test prints the compaction log which includes file sizes.
			minVersion: formatDeprecatedExperimentalValueSeparation,
			maxVersion: formatDeprecatedExperimentalValueSeparation,
		},
		"l0_to_lbase_compaction": {
			minVersion: formatDeprecatedExperimentalValueSeparation,
			maxVersion: formatDeprecatedExperimentalValueSeparation,
		},
	}
	datadriven.Walk(t, "testdata/compaction", func(t *testing.T, path string) {
		filename := filepath.Base(path)
		tc, ok := testConfigs[filename]
		if !ok {
			t.Fatalf("unknown test config: %s", filename)
		}
		minVersion, maxVersion := tc.minVersion, tc.maxVersion
		if minVersion == 0 {
			minVersion = FormatMinSupported
		}
		if maxVersion == 0 {
			maxVersion = internalFormatNewest
		}
		runTest(t, path, minVersion, maxVersion, tc.verbose)
	})
}
