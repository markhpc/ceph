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
#include "common/dout.h"
#include "perfglue/heap_profiler.h"

#define dout_context cct
#define dout_subsys ceph_subsys_bluestore

namespace PriorityCache {
  int64_t get_chunk(uint64_t usage, uint64_t total_bytes) {
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

    // shrink it to 1/256 of the rounded up cache size 
    chunk /= 256;

    // bound the chunk size to be between 4MB and 32MB
    chunk = (chunk > 4ul*1024*1024) ? chunk : 4ul*1024*1024;
    chunk = (chunk < 32ul*1024*1024) ? chunk : 32ul*1024*1024;

    // Add 7 chunks of headroom and round up to the near chunk
    uint64_t val = usage + (7 * chunk);
    uint64_t r = (val) % chunk;
    if (r > 0)
      val = val + chunk - r;
    return val;
  }

  Manager::Manager(CephContext *c, 
                   uint64_t min,
                   uint64_t max,
                   uint64_t target) : 
      cct(c),
      caches{},
      min_mem(min),
      max_mem(max),
      target_mem(target),
      tuned_mem(min)
  {
    tune_memory(true);
  }

  void Manager::tune_memory(bool interval_stats)
  {
    size_t heap_size = 0;
    size_t unmapped = 0;
    uint64_t mapped = 0;
  
    ceph_heap_release_free_memory();
    ceph_heap_get_numeric_property("generic.heap_size", &heap_size);
    ceph_heap_get_numeric_property("tcmalloc.pageheap_unmapped_bytes", &unmapped);
    mapped = heap_size - unmapped;
  
    uint64_t new_size = tuned_mem;
    new_size = (new_size < max_mem) ? new_size : max_mem;
    new_size = (new_size > min_mem) ? new_size : min_mem;
  
    // Approach the min/max slowly, but bounce away quickly.
    if ((uint64_t) mapped < target_mem) {
      double ratio = 1 - ((double) mapped / target_mem);
      new_size += ratio * (max_mem - new_size);
    } else { 
      double ratio = 1 - ((double) target_mem / mapped);
      new_size -= ratio * (new_size - min_mem);
    }
  
    if (interval_stats) {
      ldout(cct, 5) << __func__
                    << " target: " << target_mem
                    << " heap: " << heap_size
                    << " unmapped: " << unmapped
                    << " mapped: " << mapped  
                    << " old mem: " << tuned_mem
                    << " new mem: " << new_size << dendl;
    } else {
      ldout(cct, 20) << __func__
                     << " target: " << target_mem
                     << " heap: " << heap_size
                     << " unmapped: " << unmapped
                     << " mapped: " << mapped        
                     << " old mem: " << tuned_mem
                     << " new mem: " << new_size << dendl;
    }
    tuned_mem = new_size;
  }

  void Manager::balance()
  {
    int64_t mem_avail = tuned_mem;
    // Each cache is going to get a little extra from get_chunk, so shrink the
    // available memory here to compensate.
    mem_avail -= get_chunk(1, tuned_mem) * caches.size();
    assert(mem_avail >= 0);

    // Assign memory for each priority level
    for (int i = 0; i < Priority::LAST+1; i++) {
      ldout(cct, 10) << __func__ << " assigning cache bytes for PRI: " << i << dendl;
      balance_priority(&mem_avail, static_cast<Priority>(i));
    }

    // assert if we assigned more memory than is available.
    assert(mem_avail >= 0);

    // Finally commit the new cache sizes and rotate the bins
    for (auto it = caches.begin(); it != caches.end(); it++) {
      it->second->commit_cache_size(tuned_mem);
      it->second->rotate_bins();
    }
  }

  void Manager::balance_priority(int64_t *mem_avail, Priority pri)
  {
    std::unordered_map<std::string, std::shared_ptr<PriCache>> tmp_caches = caches;
    double cur_ratios = 0;
    double new_ratios = 0;
    uint64_t round = 0;
    // First, zero this priority's bytes, sum the initial ratios.
    for (auto it = tmp_caches.begin(); it != tmp_caches.end(); it++) {
      it->second->set_cache_bytes(pri, 0);
      cur_ratios += it->second->get_cache_ratio();
    }

    // For other priorities, loop until caches are satisified or we run out of
    // memory.  Since we can't allocate fractional bytes, stop if we have fewer
    // bytes left than the number of participating caches.
    while (!tmp_caches.empty() && *mem_avail > static_cast<int64_t>(tmp_caches.size())) {
      uint64_t total_assigned = 0;
      for (auto it = tmp_caches.begin(); it != tmp_caches.end();) {
        int64_t cache_wants = it->second->request_cache_bytes(pri, tuned_mem);

        // Usually the ratio should be set to the fraction of the current caches'
        // assigned ratio compared to the total ratio of all caches that still
        // want memory.  There is a special case where the only caches left are
        // all assigned 0% ratios but still want memory.  In that case, give 
        // them an equal shot at the remaining memory for this priority.
        double ratio = 1.0 / tmp_caches.size();
        if (cur_ratios > 0) {
          ratio = it->second->get_cache_ratio() / cur_ratios;
        }
        int64_t fair_share = static_cast<int64_t>(*mem_avail * ratio);

        if (cache_wants > fair_share) {
          // If we want too much, take what we can get but stick around for more
          it->second->add_cache_bytes(pri, fair_share);
          total_assigned += fair_share;
          new_ratios += it->second->get_cache_ratio();
          ldout(cct, 1) << __func__ << " " << it->first
                               << " pri: " << (int) pri
                               << " round: " << round
                               << " wanted: " << cache_wants
                               << " ratio: " << it->second->get_cache_ratio()
                               << " cur_ratios: " << cur_ratios
                               << " fair_share: " << fair_share
                               << " mem_avail: " << *mem_avail
                               << dendl;

          ++it;
        } else {
          // Otherwise assign only what we want
          if (cache_wants > 0) {
            it->second->add_cache_bytes(pri, cache_wants);
            total_assigned += cache_wants;
            ldout(cct, 1) << __func__ << " " << it->first
                                 << " pri: " << (int) pri
                                 << " round: " << round
                                 << " wanted: " << cache_wants
                                 << " ratio: " << it->second->get_cache_ratio()
                                 << " cur_ratios: " << cur_ratios
                                 << " fair_share: " << fair_share
                                 << " mem_avail: " << *mem_avail
                                 << dendl;

          }
          // Eitherer the cache didn't want anything or got what it wanted, so
          // remove it from the tmp list. 
          it = tmp_caches.erase(it);
        }
      }

      // Reset the ratios 
      *mem_avail -= total_assigned;
      cur_ratios = new_ratios;
      new_ratios = 0;

      ++round;
    }

    // Assign any remaining memory via the default ratios to the last priority.
    if (pri == Priority::LAST && *mem_avail > 0) {
      uint64_t total_assigned = 0;
      for (auto it = caches.begin(); it != caches.end(); it++) {
        double ratio = it->second->get_cache_ratio();
        int64_t fair_share = static_cast<int64_t>(*mem_avail * ratio);
        it->second->set_cache_bytes(Priority::LAST, fair_share);
        total_assigned += fair_share;
      }
      *mem_avail -= total_assigned;
    }
  }

  PriCache::~PriCache() {
  }
}
