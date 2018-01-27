package main

import (
	"fmt"
	"net/url"

	"github.com/folbricht/desync"
)

// MultiStoreWithCache is used to parse store and cache locations given in the
// command line.
// n - Number of goroutines, applies to some types of stores like SSH
// cacheLocation - Place of the local store used for caching, can be blank
// storeLocation - URLs or paths to remote or local stores that should be queried in order
func MultiStoreWithCache(n int, cacheLocation string, storeLocations ...string) (desync.Store, error) {
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
			s, err = desync.NewRemoteHTTPStore(loc)
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
