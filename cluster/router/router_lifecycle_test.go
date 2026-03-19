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

package router_test

import (
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/cluster/router"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	ucluster "github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/schema"
)

// TestPartitionByLifecycle_AllActive verifies that all-ACTIVE nodes pass through reads and writes unchanged.
// No ReadOnlyClass call expected: warmingUp is empty so the async check is skipped entirely.
func TestPartitionByLifecycle_AllActive(t *testing.T) {
	mockSchemaReader := schema.NewMockSchemaReader(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockNodeSelector := ucluster.NewMockNodeSelector(t)

	mockNodeSelector.EXPECT().NodeLifecycle("node1").Return(ucluster.NodeLifecycleActive)
	mockNodeSelector.EXPECT().NodeLifecycle("node2").Return(ucluster.NodeLifecycleActive)
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("", true).Maybe()

	state := createShardingStateWithShards([]string{"shard1"})
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(state.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas("TestClass", "shard1").Return([]string{"node1", "node2"}, nil).Times(2)

	mockReplicationFSM.EXPECT().
		FilterOneShardReplicasRead("TestClass", "shard1", []string{"node1", "node2"}).
		Return([]string{"node1", "node2"})
	mockReplicationFSM.EXPECT().
		FilterOneShardReplicasWrite("TestClass", "shard1", []string{"node1", "node2"}).
		Return([]string{"node1", "node2"}, []string{})

	r := router.NewBuilder("TestClass", false, mockNodeSelector, nil, mockSchemaReader, mockReplicationFSM).Build()
	rs, ws, err := r.GetReadWriteReplicasLocation("TestClass", "", "shard1")

	require.NoError(t, err)
	require.Len(t, rs.Replicas, 2, "all ACTIVE nodes should be readable")
	require.Len(t, ws.Replicas, 2, "all ACTIVE nodes should be writable")
	require.Empty(t, ws.AdditionalReplicas)
}

// TestPartitionByLifecycle_WarmingUpAsyncOn verifies that a WARMING_UP node is included in reads
// alongside ACTIVE nodes, and placed in AdditionalReplicas for writes when async replication is enabled.
func TestPartitionByLifecycle_WarmingUpAsyncOn(t *testing.T) {
	mockSchemaReader := schema.NewMockSchemaReader(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockNodeSelector := ucluster.NewMockNodeSelector(t)

	mockNodeSelector.EXPECT().NodeLifecycle("node1").Return(ucluster.NodeLifecycleActive)
	mockNodeSelector.EXPECT().NodeLifecycle("node2").Return(ucluster.NodeLifecycleWarmingUp)
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("", true).Maybe()

	mockSchemaReader.EXPECT().ReadOnlyClass("TestClass").Return(classWithAsync(true))

	state := createShardingStateWithShards([]string{"shard1"})
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(state.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas("TestClass", "shard1").Return([]string{"node1", "node2"}, nil).Times(2)

	// WARMING_UP node is included in reads to keep replica counts consistent across shards.
	mockReplicationFSM.EXPECT().
		FilterOneShardReplicasRead("TestClass", "shard1", []string{"node1", "node2"}).
		Return([]string{"node1", "node2"})
	mockReplicationFSM.EXPECT().
		FilterOneShardReplicasWrite("TestClass", "shard1", []string{"node1"}).
		Return([]string{"node1"}, []string{})

	r := router.NewBuilder("TestClass", false, mockNodeSelector, nil, mockSchemaReader, mockReplicationFSM).Build()
	rs, ws, err := r.GetReadWriteReplicasLocation("TestClass", "", "shard1")

	require.NoError(t, err)
	require.Len(t, rs.Replicas, 2, "WARMING_UP node must be included in reads")
	require.Len(t, ws.Replicas, 1, "only ACTIVE node counts for write quorum")
	require.Len(t, ws.AdditionalReplicas, 1, "WARMING_UP node goes to AdditionalReplicas when async is on")
	require.Equal(t, "node2", ws.AdditionalReplicas[0].NodeName)
}

// TestPartitionByLifecycle_WarmingUpAsyncOff verifies that a WARMING_UP node is included in reads
// and also in the quorum write set when async replication is disabled. Without a repair loop,
// including it in quorum (even at the cost of a brief block) is safer than silent data loss.
func TestPartitionByLifecycle_WarmingUpAsyncOff(t *testing.T) {
	mockSchemaReader := schema.NewMockSchemaReader(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockNodeSelector := ucluster.NewMockNodeSelector(t)

	mockNodeSelector.EXPECT().NodeLifecycle("node1").Return(ucluster.NodeLifecycleActive)
	mockNodeSelector.EXPECT().NodeLifecycle("node2").Return(ucluster.NodeLifecycleWarmingUp)
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("", true).Maybe()

	mockSchemaReader.EXPECT().ReadOnlyClass("TestClass").Return(classWithAsync(false))

	state := createShardingStateWithShards([]string{"shard1"})
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(state.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas("TestClass", "shard1").Return([]string{"node1", "node2"}, nil).Times(2)

	// WARMING_UP node is included in reads to keep replica counts consistent across shards.
	mockReplicationFSM.EXPECT().
		FilterOneShardReplicasRead("TestClass", "shard1", []string{"node1", "node2"}).
		Return([]string{"node1", "node2"})
	mockReplicationFSM.EXPECT().
		FilterOneShardReplicasWrite("TestClass", "shard1", []string{"node1"}).
		Return([]string{"node1"}, []string{})

	r := router.NewBuilder("TestClass", false, mockNodeSelector, nil, mockSchemaReader, mockReplicationFSM).Build()
	rs, ws, err := r.GetReadWriteReplicasLocation("TestClass", "", "shard1")

	require.NoError(t, err)
	require.Len(t, rs.Replicas, 2, "WARMING_UP node must be included in reads")
	require.Len(t, ws.Replicas, 2, "WARMING_UP node joins quorum when async is off to prevent data loss")
	require.Empty(t, ws.AdditionalReplicas)
}

// TestPartitionByLifecycle_ShuttingDownExcludedFromBoth verifies that a SHUTTING_DOWN node is excluded
// from reads and writes regardless of async replication setting.
func TestPartitionByLifecycle_ShuttingDownExcludedFromBoth(t *testing.T) {
	mockSchemaReader := schema.NewMockSchemaReader(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockNodeSelector := ucluster.NewMockNodeSelector(t)

	mockNodeSelector.EXPECT().NodeLifecycle("node1").Return(ucluster.NodeLifecycleActive)
	mockNodeSelector.EXPECT().NodeLifecycle("node2").Return(ucluster.NodeLifecycleShuttingDown)
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("", true).Maybe()

	// No warmingUp nodes → ReadOnlyClass is never called.
	state := createShardingStateWithShards([]string{"shard1"})
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(state.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas("TestClass", "shard1").Return([]string{"node1", "node2"}, nil).Times(2)

	mockReplicationFSM.EXPECT().
		FilterOneShardReplicasRead("TestClass", "shard1", []string{"node1"}).
		Return([]string{"node1"})
	mockReplicationFSM.EXPECT().
		FilterOneShardReplicasWrite("TestClass", "shard1", []string{"node1"}).
		Return([]string{"node1"}, []string{})

	r := router.NewBuilder("TestClass", false, mockNodeSelector, nil, mockSchemaReader, mockReplicationFSM).Build()
	rs, ws, err := r.GetReadWriteReplicasLocation("TestClass", "", "shard1")

	require.NoError(t, err)
	require.Len(t, rs.Replicas, 1, "SHUTTING_DOWN node must be excluded from reads")
	require.Len(t, ws.Replicas, 1, "SHUTTING_DOWN node must be excluded from write quorum")
	require.Empty(t, ws.AdditionalReplicas, "SHUTTING_DOWN node must not appear in AdditionalReplicas")
}

// TestPartitionByLifecycle_MixedStates covers a realistic mix: one active, one warming-up (async on), one shutting-down.
// ACTIVE + WARMING_UP nodes participate in reads; only SHUTTING_DOWN is excluded.
func TestPartitionByLifecycle_MixedStates(t *testing.T) {
	mockSchemaReader := schema.NewMockSchemaReader(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockNodeSelector := ucluster.NewMockNodeSelector(t)

	mockNodeSelector.EXPECT().NodeLifecycle("node1").Return(ucluster.NodeLifecycleActive)
	mockNodeSelector.EXPECT().NodeLifecycle("node2").Return(ucluster.NodeLifecycleWarmingUp)
	mockNodeSelector.EXPECT().NodeLifecycle("node3").Return(ucluster.NodeLifecycleShuttingDown)
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("", true).Maybe()

	mockSchemaReader.EXPECT().ReadOnlyClass("TestClass").Return(classWithAsync(true))

	state := createShardingStateWithShards([]string{"shard1"})
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(state.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas("TestClass", "shard1").Return([]string{"node1", "node2", "node3"}, nil).Times(2)

	// ACTIVE + WARMING_UP are readable; SHUTTING_DOWN is excluded.
	mockReplicationFSM.EXPECT().
		FilterOneShardReplicasRead("TestClass", "shard1", []string{"node1", "node2"}).
		Return([]string{"node1", "node2"})
	mockReplicationFSM.EXPECT().
		FilterOneShardReplicasWrite("TestClass", "shard1", []string{"node1"}).
		Return([]string{"node1"}, []string{})

	r := router.NewBuilder("TestClass", false, mockNodeSelector, nil, mockSchemaReader, mockReplicationFSM).Build()
	rs, ws, err := r.GetReadWriteReplicasLocation("TestClass", "", "shard1")

	require.NoError(t, err)

	require.Len(t, rs.Replicas, 2, "ACTIVE and WARMING_UP nodes should both be readable")

	require.Len(t, ws.Replicas, 1)
	require.Equal(t, "node1", ws.Replicas[0].NodeName)

	require.Len(t, ws.AdditionalReplicas, 1, "only WARMING_UP node (async on) should be in AdditionalReplicas")
	require.Equal(t, "node2", ws.AdditionalReplicas[0].NodeName)
}

// TestPartitionByLifecycle_WriteReplicaSetDirectly exercises GetWriteReplicasLocation in isolation.
func TestPartitionByLifecycle_WriteReplicaSetDirectly(t *testing.T) {
	tests := []struct {
		name           string
		replicas       []string
		lifecycles     map[string]ucluster.NodeLifecycle
		asyncEnabled   bool
		fsmWrite       []string
		fsmAdditional  []string
		wantWrite      []string
		wantAdditional []string
	}{
		{
			name:     "all active",
			replicas: []string{"n1", "n2"},
			lifecycles: map[string]ucluster.NodeLifecycle{
				"n1": ucluster.NodeLifecycleActive,
				"n2": ucluster.NodeLifecycleActive,
			},
			// no warmingUp → ReadOnlyClass never called, asyncEnabled irrelevant
			fsmWrite:      []string{"n1", "n2"},
			fsmAdditional: []string{},
			wantWrite:     []string{"n1", "n2"},
		},
		{
			name:     "one warming-up, async on",
			replicas: []string{"n1", "n2"},
			lifecycles: map[string]ucluster.NodeLifecycle{
				"n1": ucluster.NodeLifecycleActive,
				"n2": ucluster.NodeLifecycleWarmingUp,
			},
			asyncEnabled:   true,
			fsmWrite:       []string{"n1"},
			fsmAdditional:  []string{},
			wantWrite:      []string{"n1"},
			wantAdditional: []string{"n2"},
		},
		{
			name:     "one warming-up, async off",
			replicas: []string{"n1", "n2"},
			lifecycles: map[string]ucluster.NodeLifecycle{
				"n1": ucluster.NodeLifecycleActive,
				"n2": ucluster.NodeLifecycleWarmingUp,
			},
			asyncEnabled:  false,
			fsmWrite:      []string{"n1"},
			fsmAdditional: []string{},
			wantWrite:     []string{"n1", "n2"}, // warming-up joins quorum to prevent data loss
		},
		{
			name:     "one shutting-down excluded",
			replicas: []string{"n1", "n2"},
			lifecycles: map[string]ucluster.NodeLifecycle{
				"n1": ucluster.NodeLifecycleActive,
				"n2": ucluster.NodeLifecycleShuttingDown,
			},
			// no warmingUp → ReadOnlyClass never called
			fsmWrite:      []string{"n1"},
			fsmAdditional: []string{},
			wantWrite:     []string{"n1"},
		},
		{
			name:     "all warming-up, async on — falls back to quorum",
			replicas: []string{"n1", "n2"},
			lifecycles: map[string]ucluster.NodeLifecycle{
				"n1": ucluster.NodeLifecycleWarmingUp,
				"n2": ucluster.NodeLifecycleWarmingUp,
			},
			// async ON but no ACTIVE quorum nodes → WARMING_UP must join quorum to avoid
			// "cannot reach enough replicas" failures at cluster startup.
			asyncEnabled:  true,
			fsmWrite:      nil, // FSM is called with nil activeReplicas
			fsmAdditional: nil,
			wantWrite:     []string{"n1", "n2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockSchemaReader := schema.NewMockSchemaReader(t)
			mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
			mockNodeSelector := ucluster.NewMockNodeSelector(t)

			for node, lc := range tt.lifecycles {
				mockNodeSelector.EXPECT().NodeLifecycle(node).Return(lc)
			}
			mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("", true).Maybe()

			// ReadOnlyClass is only called when there are warming-up nodes.
			hasWarmingUp := false
			for _, lc := range tt.lifecycles {
				if lc == ucluster.NodeLifecycleWarmingUp {
					hasWarmingUp = true
					break
				}
			}
			if hasWarmingUp {
				mockSchemaReader.EXPECT().ReadOnlyClass("C").Return(classWithAsync(tt.asyncEnabled))
			}

			mockSchemaReader.EXPECT().Shards(mock.Anything).Return([]string{"s"}, nil)
			mockSchemaReader.EXPECT().ShardReplicas("C", "s").Return(tt.replicas, nil)
			mockReplicationFSM.EXPECT().
				FilterOneShardReplicasWrite("C", "s", tt.fsmWrite).
				Return(tt.fsmWrite, tt.fsmAdditional)

			r := router.NewBuilder("C", false, mockNodeSelector, nil, mockSchemaReader, mockReplicationFSM).Build()
			ws, err := r.GetWriteReplicasLocation("C", "", "s")

			require.NoError(t, err)
			require.ElementsMatch(t, tt.wantWrite, replicaNames(ws.Replicas))
			require.ElementsMatch(t, tt.wantAdditional, replicaNames(ws.AdditionalReplicas))
		})
	}
}

// TestPartitionByLifecycle_AllWarmingUpRead verifies that when all nodes are WARMING_UP,
// they are all included in reads — there is no longer a special "fallback"; WARMING_UP nodes
// always participate in reads alongside ACTIVE nodes to maintain consistent replica counts.
func TestPartitionByLifecycle_AllWarmingUpRead(t *testing.T) {
	mockSchemaReader := schema.NewMockSchemaReader(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockNodeSelector := ucluster.NewMockNodeSelector(t)

	mockNodeSelector.EXPECT().NodeLifecycle("node1").Return(ucluster.NodeLifecycleWarmingUp)
	mockNodeSelector.EXPECT().NodeLifecycle("node2").Return(ucluster.NodeLifecycleWarmingUp)
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("", true).Maybe()

	state := createShardingStateWithShards([]string{"shard1"})
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(state.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas("TestClass", "shard1").Return([]string{"node1", "node2"}, nil)

	// Both WARMING_UP nodes are passed to the FSM for reads.
	mockReplicationFSM.EXPECT().
		FilterOneShardReplicasRead("TestClass", "shard1", []string{"node1", "node2"}).
		Return([]string{"node1", "node2"})

	r := router.NewBuilder("TestClass", false, mockNodeSelector, nil, mockSchemaReader, mockReplicationFSM).Build()
	rs, err := r.GetReadReplicasLocation("TestClass", "", "shard1")

	require.NoError(t, err)
	require.Len(t, rs.Replicas, 2, "all WARMING_UP nodes should be readable")
}

func replicaNames(replicas []types.Replica) []string {
	names := make([]string, len(replicas))
	for i, r := range replicas {
		names[i] = r.NodeName
	}
	return names
}

func classWithAsync(enabled bool) *models.Class {
	return &models.Class{
		ReplicationConfig: &models.ReplicationConfig{AsyncEnabled: enabled},
	}
}
