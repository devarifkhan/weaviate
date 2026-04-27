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

package dynusers

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey/keys"
)

type stubNamespaces struct {
	exists map[string]struct{}
}

func (s *stubNamespaces) Exists(name string) bool {
	_, ok := s.exists[name]
	return ok
}

func newTestManager(t *testing.T, ns NamespacesExister) (*Manager, *apikey.DBUser) {
	t.Helper()
	logger, _ := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	dynUser, err := apikey.NewDBUser(t.TempDir(), false, logger)
	require.NoError(t, err)
	return &Manager{dynUser: dynUser, namespaces: ns, logger: logger}, dynUser
}

func mustMarshalJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}

// seedUser creates a user directly via the underlying DBUser so apply-method
// tests can exercise update/delete paths without going through CreateUser.
func seedUser(t *testing.T, dynUser *apikey.DBUser, userId, namespace string) {
	t.Helper()
	_, hash, identifier, err := keys.CreateApiKeyAndHash()
	require.NoError(t, err)
	require.NoError(t, dynUser.CreateUser(userId, hash, identifier, "", namespace, time.Now()))
}

func TestNewManager_NilNamespacesPanics(t *testing.T) {
	logger, _ := test.NewNullLogger()
	dynUser, err := apikey.NewDBUser(t.TempDir(), false, logger)
	require.NoError(t, err)
	assert.Panics(t, func() { NewManager(dynUser, nil, logger) })
}

func TestManager_CreateUser(t *testing.T) {
	_, hash, identifier, err := keys.CreateApiKeyAndHash()
	require.NoError(t, err)

	tests := []struct {
		name      string
		namespace string
		nsKnown   bool
		wantErr   bool
	}{
		{name: "namespace exists", namespace: "ns1", nsKnown: true},
		{name: "empty namespace skips check", namespace: ""},
		{name: "namespace deleted before apply", namespace: "ns1", nsKnown: false, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ns := &stubNamespaces{exists: map[string]struct{}{}}
			if tc.nsKnown {
				ns.exists[tc.namespace] = struct{}{}
			}
			m, dynUser := newTestManager(t, ns)

			apply := &cmd.ApplyRequest{SubCommand: mustMarshalJSON(t, cmd.CreateUsersRequest{
				UserId:         "u1",
				SecureHash:     hash,
				UserIdentifier: identifier,
				Namespace:      tc.namespace,
				CreatedAt:      time.Now(),
			})}
			err := m.CreateUser(apply)
			if tc.wantErr {
				require.Error(t, err)
				users, _ := dynUser.GetUsers("u1")
				assert.Empty(t, users, "user must not be persisted on rejection")
				return
			}
			require.NoError(t, err)
			users, err := dynUser.GetUsers("u1")
			require.NoError(t, err)
			require.NotNil(t, users["u1"])
			assert.Equal(t, tc.namespace, users["u1"].Namespace)
		})
	}
}

func TestManager_DeleteUser(t *testing.T) {
	m, dynUser := newTestManager(t, &stubNamespaces{})
	seedUser(t, dynUser, "u1", "")

	apply := &cmd.ApplyRequest{SubCommand: mustMarshalJSON(t, cmd.DeleteUsersRequest{UserId: "u1"})}
	require.NoError(t, m.DeleteUser(apply))
	users, err := dynUser.GetUsers("u1")
	require.NoError(t, err)
	assert.Empty(t, users)
}

func TestManager_ActivateAndSuspendUser(t *testing.T) {
	tests := []struct {
		name       string
		method     string // "activate" | "suspend"
		seedActive bool
		wantActive bool
	}{
		{name: "suspend active user", method: "suspend", seedActive: true, wantActive: false},
		{name: "activate suspended user", method: "activate", seedActive: false, wantActive: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m, dynUser := newTestManager(t, &stubNamespaces{})
			seedUser(t, dynUser, "u1", "")
			if !tc.seedActive {
				require.NoError(t, dynUser.DeactivateUser("u1", false))
			}

			var (
				apply *cmd.ApplyRequest
				err   error
			)
			switch tc.method {
			case "activate":
				apply = &cmd.ApplyRequest{SubCommand: mustMarshalJSON(t, cmd.ActivateUsersRequest{UserId: "u1"})}
				err = m.ActivateUser(apply)
			case "suspend":
				apply = &cmd.ApplyRequest{SubCommand: mustMarshalJSON(t, cmd.SuspendUserRequest{UserId: "u1"})}
				err = m.SuspendUser(apply)
			}
			require.NoError(t, err)
			users, err := dynUser.GetUsers("u1")
			require.NoError(t, err)
			require.NotNil(t, users["u1"])
			assert.Equal(t, tc.wantActive, users["u1"].Active)
		})
	}
}

func TestManager_GetUsers(t *testing.T) {
	m, dynUser := newTestManager(t, &stubNamespaces{})
	seedUser(t, dynUser, "u1", "ns1")

	payload, err := m.GetUsers(&cmd.QueryRequest{
		SubCommand: mustMarshalJSON(t, cmd.QueryGetUsersRequest{UserIds: []string{"u1"}}),
	})
	require.NoError(t, err)

	resp := cmd.QueryGetUsersResponse{}
	require.NoError(t, json.Unmarshal(payload, &resp))
	require.NotNil(t, resp.Users["u1"])
	assert.Equal(t, "ns1", resp.Users["u1"].Namespace)
}

// TestManager_MalformedJSON exercises every apply/query method's defensive
// json.Unmarshal path. Each method must wrap the underlying error with
// ErrBadRequest so the FSM dispatcher can classify it distinctly.
func TestManager_MalformedJSON(t *testing.T) {
	bad := []byte("not-json")
	apply := &cmd.ApplyRequest{SubCommand: bad}
	query := &cmd.QueryRequest{SubCommand: bad}

	tests := []struct {
		name string
		call func(m *Manager) error
	}{
		{name: "CreateUser", call: func(m *Manager) error { return m.CreateUser(apply) }},
		{name: "DeleteUser", call: func(m *Manager) error { return m.DeleteUser(apply) }},
		{name: "ActivateUser", call: func(m *Manager) error { return m.ActivateUser(apply) }},
		{name: "SuspendUser", call: func(m *Manager) error { return m.SuspendUser(apply) }},
		{name: "RotateKey", call: func(m *Manager) error { return m.RotateKey(apply) }},
		{name: "CreateUserWithKeyRequest", call: func(m *Manager) error { return m.CreateUserWithKeyRequest(apply) }},
		{name: "GetUsers", call: func(m *Manager) error { _, err := m.GetUsers(query); return err }},
		{name: "CheckUserIdentifierExists", call: func(m *Manager) error { _, err := m.CheckUserIdentifierExists(query); return err }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m, _ := newTestManager(t, &stubNamespaces{})
			err := tc.call(m)
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrBadRequest)
		})
	}
}
