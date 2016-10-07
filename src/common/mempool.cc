// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Allen Samuels <allen.samuels@sandisk.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include "include/mempool.h"

// **** Set this to false to disable debug mode ***
static const bool debug_mode = false;


std::map<std::string,mempool::pool_t *> *mempool::pool_t::pool_head = nullptr;
std::mutex mempool::pool_t::pool_head_lock;


static mempool::pool_t *pools[mempool::num_pools] = {
#define P(x) nullptr,
   DEFINE_MEMORY_POOLS_HELPER(P)
#undef P
};

mempool::pool_t& mempool::GetPool(mempool::pool_index_t ix) {
   if (pools[ix]) return *pools[ix];
#define P(x) \
   case x: pools[ix] = new mempool::pool_t(#x,debug_mode); break;

   switch (ix) {
      DEFINE_MEMORY_POOLS_HELPER(P);
      default: assert(0);
   }
   return *pools[ix];
#undef P
}

size_t mempool::pool_t::allocated_bytes() const {
  ssize_t result = 0;
  for (size_t i = 0; i < shard_size; ++i) {
    result += shard[i].bytes;
  }
  return (size_t) result;
}
size_t mempool::pool_t::allocated_items() const {
  ssize_t result = 0;
  for (size_t i = 0; i < shard_size; ++i) {
    result += shard[i].items;
  }
  return (size_t) result;
}

//
// Accumulate stats sorted by ...
//
void mempool::pool_t::StatsByItems(
  const std::string& prefix,
  std::multimap<size_t,StatsByItems_t>& byItems,
  size_t trim)  
{
   VisitAllPools(prefix,byItems,trim);
}

void mempool::pool_t::StatsByBytes(
   const std::string& prefix,
   std::multimap<size_t,StatsByBytes_t>& byBytes,
   size_t trim) {
   VisitAllPools(prefix,byBytes,trim);
}

void mempool::pool_t::StatsByTypeID(
   const std::string& prefix,
   std::map<const char *,StatsByTypeID_t>& byTypeID,
   size_t trim) {
   VisitAllPools(prefix,byTypeID,trim);
}

//
// Here's where the work is done
//
void mempool::pool_allocator_base_t::UpdateStats(std::multimap<size_t,StatsByBytes_t>&m) const {
   StatsByBytes_t s;
   s.typeID = typeID;
   s.items = items;
   m.insert(std::make_pair((size_t)bytes,s));  
}

void mempool::pool_allocator_base_t::UpdateStats(std::multimap<size_t,StatsByItems_t>&m) const {
   StatsByItems_t s;
   s.typeID = typeID;
   s.bytes  = bytes;
   m.insert(std::make_pair((size_t)items,s));  
}

void mempool::pool_allocator_base_t::UpdateStats(std::map<const char *,StatsByTypeID_t>&m) const {
   StatsByTypeID_t &s = m[typeID];
   s.items  += items;
   s.bytes  += bytes;
}

static std::string demangle(const char* name);

void mempool::FormatStatsByBytes(const std::multimap<size_t,StatsByBytes_t>&m, ceph::Formatter *f) {
  f->open_array_section("by_bytes");
  for (auto p = m.rbegin(); p != m.rend(); ++p) {
    f->open_object_section("type");
    f->dump_unsigned("bytes",p->first);
    p->second.dump(f);
    f->close_section();
  }
  f->close_section();
}

void mempool::FormatStatsByItems(const std::multimap<size_t,StatsByItems_t>&m, ceph::Formatter *f) {
   f->open_array_section("by_items");
  for (auto p = m.rbegin(); p != m.rend(); ++p) {
     f->open_object_section("type");
      f->dump_unsigned("items",p->first);
      p->second.dump(f);
      f->close_section();
   }
   f->close_section();
}

void mempool::FormatStatsByTypeID(const std::map<const char *,StatsByTypeID_t>&m, ceph::Formatter *f) {
   f->open_array_section("by_type");
  for (auto p = m.rbegin(); p != m.rend(); ++p) {
     f->open_object_section("type");
      f->dump_string("type",demangle(p->first));
      p->second.dump(f);
      f->close_section();
   }
   f->close_section();
}

void mempool::DumpStatsByBytes(const std::string& prefix,ceph::Formatter *f,size_t trim) {
   std::multimap<size_t,mempool::StatsByBytes_t> m;
   pool_t::StatsByBytes(prefix,m,trim);
   FormatStatsByBytes(m,f);
}

void mempool::DumpStatsByItems(const std::string& prefix,ceph::Formatter *f,size_t trim) {
   std::multimap<size_t,StatsByItems_t> m;
   pool_t::StatsByItems(prefix,m,trim);
   FormatStatsByItems(m,f);
}

void mempool::DumpStatsByTypeID(const std::string& prefix,ceph::Formatter *f,size_t trim) {
   std::map<const char *,StatsByTypeID_t> m;
   pool_t::StatsByTypeID(prefix,m,trim);
   FormatStatsByTypeID(m,f);
}

//// Stole this code from http://stackoverflow.com/questions/281818/unmangling-the-result-of-stdtype-infoname
#ifdef __GNUG__
#include <cstdlib>
#include <memory>
#include <cxxabi.h>

static std::string demangle(const char* name) {

    int status = -4; // some arbitrary value to eliminate the compiler warning

    // enable c++11 by passing the flag -std=c++11 to g++
    std::unique_ptr<char, void(*)(void*)> res {
        abi::__cxa_demangle(name, NULL, NULL, &status),
        std::free
    };

    return (status==0) ? res.get() : name ;
}

#else

// does nothing if not g++
static std::string demangle(const char* name) {
    return name;
}

#endif

void mempool::StatsByBytes_t::dump(ceph::Formatter *f) const {
  f->dump_unsigned("items",items);
  f->dump_string("TypeID",demangle(typeID));
}

void mempool::StatsByItems_t::dump(ceph::Formatter *f) const {
   f->dump_unsigned("bytes",bytes);
   f->dump_string("TypeID",demangle(typeID));
}

void mempool::StatsByTypeID_t::dump(ceph::Formatter *f) const {
   f->dump_unsigned("bytes",bytes);
   f->dump_unsigned("items",items);
}


