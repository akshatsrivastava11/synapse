package main

import (
	"akshat/synapse/internal/objectstore"
	"akshat/synapse/internal/storage"
	"context"
	"fmt"
	"log"
	"os"
)

func main() {
	ctx := context.Background()

	dataDir, err := os.MkdirTemp("", "streamdb-demo-")
	if err != nil {
		log.Fatalf("create scratch dir: %v", err)
	}
	defer os.RemoveAll(dataDir)
	fmt.Printf("[1] local data directory (stand-in for NVMe): %s\n\n", dataDir)
	store := objectstore.NewMemoryStore()
	fmt.Println("[2] object store ready (in-memory fake standing in for MinIO/S3)")
	fmt.Println()

	partitionID := 0
	p, err := storage.OpenPartition(dataDir, partitionID, storage.PartitionObject{
		MaxSegmentBytes: 200,
		Store:           store,
	})
	if err != nil {
		log.Fatalf("open partition: %v", err)
	}
	defer p.Close()
	fmt.Printf("[3] partition %d opened (max segment size: 200 bytes, tiny on purpose)\n\n", partitionID)
	const numRecords = 60
	fmt.Printf("[4] appending %d records...\n", numRecords)
	for i := 0; i < numRecords; i++ {
		payload := []byte(fmt.Sprintf("event-%03d", i))
		offset, err := p.Append(payload)
		if err != nil {
			log.Fatalf("append record %d: %v", i, err)
		}
		if offset != int64(i) {
			log.Fatalf("unexpected offset: got %d, want %d", offset, i)
		}
	}
	fmt.Printf("    done. next offset to be assigned: %d\n", p.NextOffset())
	fmt.Printf("    segments created by rolling: %d (only the last one is still active/writable)\n\n", p.SegmentCount())

	if err := p.Sync(); err != nil {
		log.Fatalf("sync: %v", err)
	}
	fmt.Println("[5] fsync'd the active segment - all 60 records are now crash-durable on local disk")
	fmt.Println()

	fmt.Printf("[6] before flush: %d of %d segments are resident on local disk\n", p.LocalSegmentCount(), p.SegmentCount())
	fmt.Printf("    object store Put calls so far: %d\n\n", store.PutCount)

	flushed, err := p.FlushSealedSegments(ctx)
	if err != nil {
		log.Fatalf("flush sealed segments: %v", err)
	}
	fmt.Printf("[7] flushed %d sealed segment(s) to object storage\n", flushed)
	fmt.Printf("    object store Put calls now: %d\n", store.PutCount)
	fmt.Printf("    after flush: only %d of %d segments remain resident on local disk (the active one)\n\n",
		p.LocalSegmentCount(), p.SegmentCount())

	hotOffset := p.NextOffset() - 1 // most recently written record; guaranteed to be in the still-active segment
	getsBefore := store.GetCount
	hotPayload, err := p.ReadFrom(ctx, hotOffset)
	if err != nil {
		log.Fatalf("hot read at offset %d: %v", hotOffset, err)
	}
	fmt.Printf("[8] HOT read at offset %d -> %q\n", hotOffset, hotPayload)
	fmt.Printf("    object store Get calls: %d -> %d (unchanged: served entirely from local disk)\n\n",
		getsBefore, store.GetCount)

	coldOffset := int64(0) // the very first record, in a segment that was rolled and then flushed away
	getsBefore = store.GetCount
	coldPayload, err := p.ReadFrom(ctx, coldOffset)
	if err != nil {
		log.Fatalf("cold read at offset %d: %v", coldOffset, err)
	}
	fmt.Printf("[9] COLD read at offset %d -> %q\n", coldOffset, coldPayload)
	fmt.Printf("    object store Get calls: %d -> %d (+1: had to download the segment)\n", getsBefore, store.GetCount)
	fmt.Printf("    that segment is now re-cached locally: %d of %d segments resident on disk\n\n",
		p.LocalSegmentCount(), p.SegmentCount())

	getsBefore = store.GetCount
	cachedPayload, err := p.ReadFrom(ctx, coldOffset)
	if err != nil {
		log.Fatalf("cached read at offset %d: %v", coldOffset, err)
	}
	fmt.Printf("[10] repeat read at offset %d -> %q\n", coldOffset, cachedPayload)
	fmt.Printf("     object store Get calls: %d -> %d (unchanged: served from the local re-download cache)\n\n",
		getsBefore, store.GetCount)

	fmt.Println("summary: write path lands on NVMe first and acks fast; a background flush")
	fmt.Println("moves sealed data to durable object storage and reclaims local disk; reads")
	fmt.Println("stay off the network for hot data and pay the network cost exactly once per")
	fmt.Println("cold segment, not once per record.")

}
