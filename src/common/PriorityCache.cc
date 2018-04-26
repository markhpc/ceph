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
  int64_t get_chunk(uint64_t usage) {
    // Add a chunk of headroom and round up to the near chunk
    uint64_t chunk = 2<<24;
    uint64_t val = usage + chunk;
    uint64_t r = (val) % chunk;
    if (r > 0)
      val = val + chunk - r;
    return val;
  }

  PriCache::~PriCache() {
  }
}
