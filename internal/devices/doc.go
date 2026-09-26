// Package devices maintains the fleet's device registry in MongoDB: one devices document per
// device identity, refreshed from accepted registrations and heartbeats, and the staleness
// sweep that marks silent devices offline.
package devices
