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

#include "PriorityCache.h"

namespace PriorityCache {
  int64_t get_chunk(uint64_t usage, uint64_t total_bytes) {
    // If nothing is requested or nothing available, don't actually reserve
    // anything.  Generally it is better to reserve a byte in PRI0 explicitly
    // to guarantee that a chunk of memory is made available.
    if (usage == 0 || total_bytes == 0) {
      return 0;
    }
    uint64_t chunk = total_bytes;

    // Find the nearest power of 2
    chunk -= 1;
    chunk |= chunk >> 1;
    chunk |= chunk >> 2;
    chunk |= chunk >> 4;
    chunk |= chunk >> 8;
    chunk |= chunk >> 16;
    chunk |= chunk >> 32;
    chunk += 1;

    // shrink it to 1/128 of the rounded up cache size 
    chunk /= 128;
    
    // bound the chunk size to be between 4MB and 128MB
    chunk = (chunk > 4ul*1024*1024) ? chunk : 4ul*1024*1024;
    chunk = (chunk < 128ul*1024*1024) ? chunk : 128ul*1024*1024;
 
    // Add a chunk of headroom and round up to the near chunk
    uint64_t val = usage + chunk;
    uint64_t r = (val) % chunk;
    if (r > 0)
      val = val + chunk - r;
    return val;
  }

  PriCache::~PriCache() {
  }
}
