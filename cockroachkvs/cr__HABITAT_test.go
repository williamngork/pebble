package cockroachkvs

import (
	"bytes"
	"fmt"
	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/crlib/testutils/leaktest"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/colblk"
	"strconv"
	"testing"
	"unsafe"
)

func TestKeySchema_KeySeeker__HABITAT(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var buf bytes.Buffer
	var enc colblk.DataBlockEncoder
	var dec colblk.DataBlockDecoder
	var ks colblk.KeySeeker
	var maxKeyLen int
	enc.Init(&KeySchema)

	initKeySeeker := func() {
		ksPointer := &cockroachKeySeeker{}
		KeySchema.InitKeySeekerMetadata((*colblk.KeySeekerMetadata)(unsafe.Pointer(ksPointer)), &dec)
		ks = KeySchema.KeySeeker((*colblk.KeySeekerMetadata)(unsafe.Pointer(ksPointer)))
	}

	datadriven.RunTest(t, "testdata/key_schema_key_seeker__HABITAT", func(t *testing.T, td *datadriven.TestData) string {
		buf.Reset()
		switch td.Cmd {
		case "define-block":
			enc.Reset()
			maxKeyLen = 0
			var rows int
			for _, line := range crstrings.Lines(td.Input) {
				k := parseUserKey(line)
				fmt.Fprintf(&buf, "Parse(%s) = hex:%x\n", line, k)
				maxKeyLen = max(maxKeyLen, len(k))
				kcmp := enc.KeyWriter.ComparePrev(k)
				ikey := base.InternalKey{
					UserKey: k,
					Trailer: pebble.MakeInternalKeyTrailer(0, base.InternalKeyKindSet),
				}
				enc.Add(ikey, k, block.InPlaceValuePrefix(false), kcmp, false /* isObsolete */)
				rows++
			}
			blk, _ := enc.Finish(rows, enc.Size())
			dec.Init(&KeySchema, blk)
			return buf.String()
		case "is-lower-bound":
			initKeySeeker()
			syntheticSuffix, syntheticSuffixStr, _ := getSyntheticSuffix(t, td)

			for _, line := range crstrings.Lines(td.Input) {
				k := parseUserKey(line)
				got := ks.IsLowerBound(k, syntheticSuffix)
				fmt.Fprintf(&buf, "IsLowerBound(%s, %q) = %t\n", line, syntheticSuffixStr, got)
			}
			return buf.String()
		case "seek-ge":
			initKeySeeker()
			for _, line := range crstrings.Lines(td.Input) {
				k := parseUserKey(line)
				boundRow := -1
				searchDir := 0
				row, equalPrefix := ks.SeekGE(k, boundRow, int8(searchDir))

				fmt.Fprintf(&buf, "SeekGE(%s, boundRow=%d, searchDir=%d) = (row=%d, equalPrefix=%t)",
					line, boundRow, searchDir, row, equalPrefix)
				if row >= 0 && row < dec.BlockDecoder().Rows() {
					var kiter colblk.PrefixBytesIter
					kiter.Buf = make([]byte, maxKeyLen+1)
					key := ks.MaterializeUserKey(&kiter, -1, row)
					fmt.Fprintf(&buf, " [hex:%x]", key)
				}
				fmt.Fprintln(&buf)
			}
			return buf.String()
		case "materialize-user-key":
			initKeySeeker()
			syntheticSuffix, syntheticSuffixStr, syntheticSuffixOk := getSyntheticSuffix(t, td)

			var kiter colblk.PrefixBytesIter
			kiter.Buf = make([]byte, maxKeyLen+len(syntheticSuffix)+1)
			prevRow := -1
			for _, line := range crstrings.Lines(td.Input) {
				row, err := strconv.Atoi(line)
				if err != nil {
					t.Fatalf("bad row number %q: %s", line, err)
				}
				if syntheticSuffixOk {
					key := ks.MaterializeUserKeyWithSyntheticSuffix(&kiter, syntheticSuffix, prevRow, row)
					fmt.Fprintf(&buf, "MaterializeUserKeyWithSyntheticSuffix(%d, %d, %s) = hex:%x\n", prevRow, row, syntheticSuffixStr, key)
				} else {
					key := ks.MaterializeUserKey(&kiter, prevRow, row)
					fmt.Fprintf(&buf, "MaterializeUserKey(%d, %d) = hex:%x\n", prevRow, row, key)
				}
				prevRow = row
			}
			return buf.String()
		default:
			panic(fmt.Sprintf("unrecognized command %q", td.Cmd))
		}
	})
}
