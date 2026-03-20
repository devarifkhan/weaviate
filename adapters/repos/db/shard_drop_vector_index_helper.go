//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/models"
)

type vectorDropIndexHelper struct{}

func newVectorDropIndexHelper() *vectorDropIndexHelper {
	return &vectorDropIndexHelper{}
}

// ensureFilesAreRemovedForDroppedVectorIndexes removes vector index files
// for vectors that are no longer defined in the class's VectorConfig.
// This handles two cases:
// - tenant was inactive during a drop vector index operation, so files remain on disk
// - an error occurred during the drop operation and files were not fully cleaned up
func (h *vectorDropIndexHelper) ensureFilesAreRemovedForDroppedVectorIndexes(
	indexPath, shardName string, class *models.Class,
) error {
	configuredVectors := make(map[string]struct{}, len(class.VectorConfig))
	for name := range class.VectorConfig {
		configuredVectors[name] = struct{}{}
	}

	orphaned, err := h.findOrphanedVectorNames(indexPath, shardName, configuredVectors)
	if err != nil {
		return err
	}

	for _, vectorName := range orphaned {
		if err := h.removeVectorIndexFiles(indexPath, shardName, vectorName); err != nil {
			return fmt.Errorf("failed to remove dropped vector index %q files for class %s: %w",
				vectorName, class.Class, err)
		}
	}

	return nil
}

// findOrphanedVectorNames scans both the LSM directory (for flat vector buckets
// stored as "vectors_{name}") and the shard directory (for HNSW commit log
// directories named "vectors_{name}.hnsw.commitlog.d") to discover named
// vectors that are no longer present in configuredVectors.
func (h *vectorDropIndexHelper) findOrphanedVectorNames(
	indexPath, shardName string, configuredVectors map[string]struct{},
) ([]string, error) {
	seen := make(map[string]struct{})

	// Scan LSM directory for vector buckets (flat indexes).
	lsmDir := filepath.Join(indexPath, shardName, "lsm")
	if entries, err := os.ReadDir(lsmDir); err == nil {
		prefix := helpers.VectorsBucketLSM + "_"
		compressedPrefix := helpers.VectorsCompressedBucketLSM + "_"
		for _, entry := range entries {
			name := entry.Name()
			if !strings.HasPrefix(name, prefix) || strings.HasPrefix(name, compressedPrefix) {
				continue
			}
			vectorName := strings.TrimPrefix(name, prefix)
			if _, configured := configuredVectors[vectorName]; !configured {
				seen[vectorName] = struct{}{}
			}
		}
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("read lsm directory: %w", err)
	}

	// Scan shard directory for HNSW commit log directories.
	shardDir := filepath.Join(indexPath, shardName)
	if entries, err := os.ReadDir(shardDir); err == nil {
		prefix := helpers.VectorsBucketLSM + "_"
		suffix := ".hnsw.commitlog.d"
		for _, entry := range entries {
			name := entry.Name()
			if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, suffix) {
				continue
			}
			vectorName := strings.TrimPrefix(name, prefix)
			vectorName = strings.TrimSuffix(vectorName, suffix)
			if _, configured := configuredVectors[vectorName]; !configured {
				seen[vectorName] = struct{}{}
			}
		}
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("read shard directory: %w", err)
	}

	if len(seen) == 0 {
		return nil, nil
	}

	orphaned := make([]string, 0, len(seen))
	for name := range seen {
		orphaned = append(orphaned, name)
	}
	return orphaned, nil
}

// removeVectorIndexFiles removes all on-disk artifacts for a named vector index:
// - LSM bucket: vectors_{name}
// - LSM compressed bucket: vectors_compressed_{name}
// - HNSW commit log directory: vectors_{name}.hnsw.commitlog.d
// - HNSW snapshot directory: vectors_{name}.hnsw.snapshot.d
func (h *vectorDropIndexHelper) removeVectorIndexFiles(indexPath, shardName, vectorName string) error {
	lsmDir := filepath.Join(indexPath, shardName, "lsm")
	shardDir := filepath.Join(indexPath, shardName)

	vectorsBucket := fmt.Sprintf("%s_%s", helpers.VectorsBucketLSM, vectorName)
	compressedBucket := helpers.GetCompressedBucketName(vectorName)

	paths := []string{
		filepath.Join(lsmDir, vectorsBucket),
		filepath.Join(lsmDir, compressedBucket),
		filepath.Join(shardDir, fmt.Sprintf("%s.hnsw.commitlog.d", vectorsBucket)),
		filepath.Join(shardDir, fmt.Sprintf("%s.hnsw.snapshot.d", vectorsBucket)),
	}

	for _, p := range paths {
		if err := os.RemoveAll(p); err != nil {
			return fmt.Errorf("remove %s: %w", p, err)
		}
	}

	return nil
}
