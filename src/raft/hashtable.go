package raft

import (
	"sync"

)

type HashTable struct {
	table map[string] struct {
		value string
		reqID int 
	}
	lock sync.Mutex
}

func newHashTable() *HashTable {
	return &HashTable{
		table: make(map[string]struct{value string; reqID int}),
	}
}

func (h *HashTable) set(key string, value string, reqID int) int {
	h.lock.Lock()
	defer h.lock.Unlock()

	if item, exists := h.table[key]; !exists || item.reqID < reqID {
		h.table[key] = struct{value string; reqID int}{
			value: value,
			reqID: reqID,
		}
		return 1
	}
	return -1
}

func (h *HashTable) get(key string) interface{} {
	h.lock.Lock()
	defer h.lock.Unlock()

	if item, exists := h.table[key]; exists  {
		return item.value;
	}

	return nil;
}

func (h *HashTable) getReqID(key string) interface{} {
	h.lock.Lock()
	defer h.lock.Unlock()

	if item, exists := h.table[key]; exists  {
		return item.reqID;
	}

	return nil;
}

func (h* HashTable) delete(key string, reqID int) int {
	h.lock.Lock()
	defer h.lock.Unlock()

	if item, exists := h.table[key]; exists && item.reqID <= reqID {
		delete(h.table, key)
		return 1
	}

	return -1
}

