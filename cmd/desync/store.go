package main

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/folbricht/desync"
)

// MultiStoreWithCache is used to parse store and cache locations given in the
// command line.
// n - Number of goroutines, applies to some types of stores like SSH
// cacheLocation - Place of the local store used for caching, can be blank
// storeLocation - URLs or paths to remote or local stores that should be queried in order
func MultiStoreWithCache(n int, cacheLocation string, clientCert string, clientKey string, storeLocations ...string) (desync.Store, error) {
	var (
		store  desync.Store
		stores []desync.Store
	)
	for _, location := range storeLocations {
		loc, err := url.Parse(location)
		if err != nil {
			return store, fmt.Errorf("Unable to parse store location %s : %s", location, err)
		}
		var s desync.Store
		switch loc.Scheme {
		case "ssh":
			s, err = desync.NewRemoteSSHStore(loc, n)
			if err != nil {
				return store, err
			}
		case "http", "https":
			h, err := desync.NewRemoteHTTPStore(loc, n, clientCert, clientKey)
			if err != nil {
				return store, err
			}
			h.SetTimeout(cfg.HTTPTimeout)
			h.SetErrorRetry(cfg.HTTPErrorRetry)
			s = h
		case "s3+http", "s3+https":
			accesskey, secretkey := cfg.GetS3CredentialsFor(loc)
			s, err = desync.NewS3Store(location, accesskey, secretkey)
			if err != nil {
				return store, err
			}
		case "":
			s, err = desync.NewLocalStore(loc.Path)
			if err != nil {
				return store, err
			}
		default:
			return store, fmt.Errorf("Unsupported store access scheme %s", loc.Scheme)
		}
		stores = append(stores, s)
	}

	// Combine all stores into one router
	store = desync.NewStoreRouter(stores...)

	// See if we want to use a local store as cache, if so, attach a cache to
	// the router
	if cacheLocation != "" {
		cache, err := desync.NewLocalStore(cacheLocation)
		if err != nil {
			return store, err
		}
		cache.UpdateTimes = true
		store = desync.NewCache(store, cache)
	}
	return store, nil
}

// WritableStore is used to parse a store location from the command line for
// commands that expect to write chunks, such as make or tar. It determines
// which type of writable store is needed, instantiates and returns a
// single desync.WriteStore.
func WritableStore(n int, location string) (desync.WriteStore, error) {
	u, err := url.Parse(location)
	if err != nil {
		return nil, err
	}
	if u.Scheme == "" { // No scheme in the URL? Got to be a local dir
		return desync.NewLocalStore(location)
	}
	if strings.HasPrefix(location, "s3+http") {
		accesskey, secretkey := cfg.GetS3CredentialsFor(u)
		return desync.NewS3Store(location, accesskey, secretkey)
	}
	return nil, fmt.Errorf("store '%s' does not support writing", location)
}
