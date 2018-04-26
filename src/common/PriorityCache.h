// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_PRIORITY_CACHE_H
#define CEPH_PRIORITY_CACHE_H

#include <stdint.h>
#include <string>

namespace PriorityCache {
  enum Priority {
    PRI0,  // Reserved for special items
    PRI1,  // High priority cache items
    PRI2,  // Medium priority cache items
    PRI3,  // Low priority cache items
    LAST = PRI3,
  };

  int64_t get_chunk(uint64_t usage);

  struct PriCache {
    virtual ~PriCache();
    virtual int64_t request_cache_bytes(PriorityCache::Priority pri) const = 0;
    virtual int64_t get_cache_bytes(PriorityCache::Priority pri) const = 0;
    virtual int64_t get_cache_bytes() const = 0;
    // set_cache_bytes returns the difference between the currently set bytes and new value
    virtual int64_t set_cache_bytes(PriorityCache::Priority pri, int64_t bytes) = 0;
    virtual int64_t add_cache_bytes(PriorityCache::Priority pri, int64_t bytes) = 0;
    virtual int64_t commit_cache_size() = 0;
    virtual double get_cache_ratio() const = 0;
    virtual int set_cache_ratio(double ratio) = 0;
    virtual int64_t get_cache_min() const = 0;
    virtual int set_cache_min(int64_t min) = 0;
    virtual std::string get_cache_name() const = 0;
  };
}

#endif
