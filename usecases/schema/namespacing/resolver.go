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

package namespacing

import (
	"errors"
	"fmt"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// ErrNamespaceOnDisabledCluster is returned when a principal carries a
// non-empty Namespace while the cluster has namespaces disabled. This state
// should be unreachable — WS1 startup invariants, WS3 DB-user create
// validation, and WS6 OIDC classification all reject it upstream — so a
// principal reaching the resolver in this shape indicates a bug or a
// mis-issued token. Fail closed rather than silently ignoring the namespace.
var ErrNamespaceOnDisabledCluster = errors.New("principal carries a namespace but namespaces are disabled on this cluster")

// SchemaManager is a single-method interface exposing alias resolution.
// It allows the resolver to look up aliases without depending on the full
// schema reader.
type SchemaManager interface {
	ResolveAlias(alias string) string
}

// qualify prepends principal.Namespace to name.
func qualify(principal *models.Principal, name string) string {
	if principal == nil || principal.Namespace == "" {
		return name
	}
	return principal.Namespace + schema.NamespaceSeparator + name
}

// Resolve is the read-side entry point used everywhere a user-supplied
// class/alias name needs to become an internal class name:
//
//  1. If namespaces are disabled but the principal carries a namespace,
//     fail closed with ErrNamespaceOnDisabledCluster. The state should be
//     unreachable; surfacing it as an error rather than silently ignoring
//     the namespace prevents a stray principal.Namespace (carryover from a
//     JWT, mis-issued token, etc.) from going undetected.
//  2. If namespaces are enabled cluster-wide, qualify the input with the
//     principal's namespace.
//  3. Look the (possibly qualified) name up as an alias via the existing
//     in-memory resolver; if it matches an alias, return the alias target.
//
// Returns (class, originalAlias, err). originalAlias is the caller-supplied
// short name when an alias was hit, "" otherwise — used by the objects
// layer to preserve existing alias-aware flows. Sites that do not need
// this can ignore the second return value.
func Resolve(principal *models.Principal, sm SchemaManager, nsEnabled bool, name string) (class, originalAlias string, err error) {
	if !nsEnabled && principal != nil && principal.Namespace != "" {
		return "", "", fmt.Errorf("%w: principal=%q namespace=%q", ErrNamespaceOnDisabledCluster, principal.Username, principal.Namespace)
	}

	qualified := name
	if nsEnabled {
		qualified = qualify(principal, name)
	}

	// Check if the qualified name is an alias
	if resolvedClass := sm.ResolveAlias(qualified); resolvedClass != "" {
		return resolvedClass, qualified, nil
	}

	// Not an alias, return the qualified name
	return qualified, "", nil
}
