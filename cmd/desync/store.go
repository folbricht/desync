package main

import (
	"fmt"
	"net/url"

	"path"

	"github.com/folbricht/desync"
	"github.com/pkg/errors"
)

// storeOptions are used to pass additional options to store initalization
type storeOptions struct {
	n          int
	clientCert string
	clientKey  string
}

// MultiStoreWithCache is used to parse store and cache locations given in the
// command line.
// cacheLocation - Place of the local store used for caching, can be blank
// storeLocation - URLs or paths to remote or local stores that should be queried in order
func MultiStoreWithCache(opts storeOptions, cacheLocation string, storeLocations ...string) (desync.Store, error) {
	var (
		store  desync.Store
		stores []desync.Store
	)
	for _, location := range storeLocations {
		s, err := storeFromLocation(location, opts)
		if err != nil {
			return store, err
		}
		stores = append(stores, s)
	}

	// Combine all stores into one router
	store = desync.NewStoreRouter(stores...)

	// See if we want to use a writable store as cache, if so, attach a cache to
	// the router
	if cacheLocation != "" {
		cache, err := WritableStore(cacheLocation, opts)
		if err != nil {
			return store, err
		}

		if ls, ok := cache.(desync.LocalStore); ok {
			ls.UpdateTimes = true
		}
		store = desync.NewCache(store, cache)
	}
	return store, nil
}

// multiStoreWithCache is used to parse store locations, and return a store
// router instance containing them all for reading, in the order they're given
func multiStore(opts storeOptions, storeLocations ...string) (desync.Store, error) {
	var stores []desync.Store
	for _, location := range storeLocations {
		s, err := storeFromLocation(location, opts)
		if err != nil {
			return nil, err
		}
		stores = append(stores, s)
	}

	return desync.NewStoreRouter(stores...), nil
}

// WritableStore is used to parse a store location from the command line for
// commands that expect to write chunks, such as make or tar. It determines
// which type of writable store is needed, instantiates and returns a
// single desync.WriteStore.
func WritableStore(location string, opts storeOptions) (desync.WriteStore, error) {
	s, err := storeFromLocation(location, opts)
	if err != nil {
		return nil, err
	}
	store, ok := s.(desync.WriteStore)
	if !ok {
		return nil, fmt.Errorf("store '%s' does not support writing", location)
	}
	return store, nil
}

// Parse a single store URL or path and return an initialized instance of it
func storeFromLocation(location string, opts storeOptions) (desync.Store, error) {
	loc, err := url.Parse(location)
	if err != nil {
		return nil, fmt.Errorf("Unable to parse store location %s : %s", location, err)
	}
	var s desync.Store
	switch loc.Scheme {
	case "ssh":
		s, err = desync.NewRemoteSSHStore(loc, opts.n)
		if err != nil {
			return nil, err
		}
	case "sftp":
		s, err = desync.NewSFTPStore(loc)
		if err != nil {
			return nil, err
		}
	case "http", "https":
		h, err := desync.NewRemoteHTTPStore(loc, opts.n, opts.clientCert, opts.clientKey)
		if err != nil {
			return nil, err
		}
		h.SetTimeout(cfg.HTTPTimeout)
		h.SetErrorRetry(cfg.HTTPErrorRetry)
		s = h
	case "s3+http", "s3+https":
		s3Creds, region := cfg.GetS3CredentialsFor(loc)
		s, err = desync.NewS3Store(loc, s3Creds, region)
		if err != nil {
			return nil, err
		}
	case "":
		s, err = desync.NewLocalStore(loc.Path)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("Unsupported store access scheme %s", loc.Scheme)
	}
	return s, nil
}

func readCaibxFile(location string, opts storeOptions) (c desync.Index, err error) {
	is, indexName, err := indexStoreFromLocation(location, opts)
	if err != nil {
		return c, err
	}
	defer is.Close()

	c, err = is.GetIndex(indexName)
	if err != nil {
		return c, err
	}
	return c, err
}

func storeCaibxFile(idx desync.Index, location string, opts storeOptions) error {
	is, indexName, err := writableIndexStore(location, opts)
	if err != nil {
		return err
	}
	defer is.Close()
	return is.StoreIndex(indexName, idx)
}

// WritableIndexStore is used to parse a store location from the command line for
// commands that expect to write indexes, such as make or tar. It determines
// which type of writable store is needed, instantiates and returns a
// single desync.IndexWriteStore.
func writableIndexStore(location string, opts storeOptions) (desync.IndexWriteStore, string, error) {
	s, indexName, err := indexStoreFromLocation(location, opts)
	if err != nil {
		return nil, indexName, err
	}
	store, ok := s.(desync.IndexWriteStore)
	if !ok {
		return nil, indexName, fmt.Errorf("store '%s' does not support writing", location)
	}
	return store, indexName, nil
}

// Parse a single store URL or path and return an initialized instance of it
func indexStoreFromLocation(location string, opts storeOptions) (desync.IndexStore, string, error) {
	loc, err := url.Parse(location)
	if err != nil {
		return nil, "", fmt.Errorf("Unable to parse store location %s : %s", location, err)
	}

	indexName := path.Base(loc.Path)
	// Remove file name from url path
	p := *loc
	p.Path = path.Dir(p.Path)

	var s desync.IndexStore
	switch loc.Scheme {
	case "ssh":
		return nil, "", errors.New("Index storage is not supported by ssh remote stores")
	case "sftp":
		s, err = desync.NewSFTPIndexStore(loc)
		if err != nil {
			return nil, "", err
		}
	case "http", "https":
		h, err := desync.NewRemoteHTTPIndexStore(&p, opts.n, opts.clientCert, opts.clientKey)
		if err != nil {
			return nil, "", err
		}
		h.SetTimeout(cfg.HTTPTimeout)
		h.SetErrorRetry(cfg.HTTPErrorRetry)
		s = h
	case "s3+http", "s3+https":
		s3Creds, region := cfg.GetS3CredentialsFor(&p)
		s, err = desync.NewS3IndexStore(&p, s3Creds, region)
		if err != nil {
			return nil, "", err
		}
	case "":

		s, err = desync.NewLocaIndexlStore(p.String())
		if err != nil {
			return nil, "", err
		}
	default:
		return nil, "", fmt.Errorf("Unsupported store access scheme %s", loc.Scheme)
	}
	return s, indexName, nil
}
