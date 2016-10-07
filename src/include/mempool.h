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

#ifndef _CEPH_INCLUDE_MEMPOOL_H
#define _CEPH_INCLUDE_MEMPOOL_H
#include <iostream>
#include <fstream>

#include <cstddef>
#include <map>
#include <set>
#include <vector>
#include <assert.h>
#include <list>
#include <mutex>
#include <atomic>
#include <climits>
#include <typeinfo>

#include <common/Formatter.h>

/**********************

Memory Pools

A memory pool isn't a physical entity, no is made to the underlying malloc/free strategy or implementation.
Rather a memory pool is a method for accounting the consumption of memory of a set of containers.

Memory pools are statically declared (see pool_index_t).

Containers (i.e., stl containers) and objects are declared to be part of a particular pool (again, statically).
As those containers grow and shrink as well as are created and destroyed the memory consumption is tracked.

The goal is to be able to inexpensively answer the question: How much memory is pool 'x' using?

However, there is also a "debug" mode which enables substantial additional statistics to be tracked.
It is hoped that the 'debug' mode is inexpensive enough to allow it to be enabled on production systems.

Using memory pools is very easy.

To create a new memory pool, simply add a new name into the list of memory pools that's defined in "DEFINE_MEMORY_POOLS_HELPER".
-- that's it -- :)

For each memory pool that's created a C++ namespace is also automatically created (name is same as in DEFINE_MEMORY_POOLS_HELPER).
In that namespace is automatically declared for you all of the slab containers.
(For details on slab_xxxx containers see include/slab_containers.h)

Thus for mempool "unittest_1" we have automatically available to us:

   unittest_1::map
   unittest_1::multimap
   unittest_1::set
   unittest_1::multiset
   unittest_1::list
   unittest_1::vector

For standalone object, i.e., not in a container we utilize class-based operator new and delete AND a "factory" object
that serves as a container for all of the instances of the standalone object type.

Thus for a particular object of type "T" we have:

   struct T {
      MEMBER_OF_MEMPOOL()
      ...
   };

else where, in some .cc file we define the operator new/delete code (and the factory object) with:

   DEFINE_OBJECT_IN_MEMPOOL(T,unittest_1,stackSize)

<just like all slab_xxx containers, memory is pre-allocated for the first "stackSize" objects, you can set it to 0 if you like :)

----------------
At any time, you can call the function unittest_1::allocated_bytes() which will tell you how many malloc'ed
bytes there currently are in the mempool. No locking required.
----------------

Debug_Mode statistics

Debug_mode is globally statically enabled/disabled through a line in mempool.cc (static definition of debug_mode).

When debug mode is on, the following statistics are available. When debug mode is off, you can make these calls, but the
results will all be empty (since there aren't any stats available). But it won't fault out...

Each memory pool collects 4 statistics for each "container" that's in the memory pool. There are several ways to
interrogate and retrieve these statistics that sort and aggregate them accordingly.

The 4 "stats" for each container are:

slots: Number of elements currently allocated for the container.
   For nodal containers (set,map,list,...) it's the same as .size()
   For vectors, it's the same as capacity()
slabs: Number of slabs of memory currently allocated to this container (either in use or not in use)
bytes: Number of bytes of memory currently allocated to this container (either in use or not in use)
typeID: a string that's the type signature. (a demangled typeid for the container).

The four external interrogation functions are:

void mempool::DumpStatsByBytes(const std::string& prefix,ceph::Formatter *f,size_t trim = 50);
void mempool::DumpStatsBySlots(const std::string& prefix,ceph::Formatter *f,size_t trim = 50);
void mempool::DumpStatsBySlabs(const std::string& prefix,ceph::Formatter *f,size_t trim = 50);
void mempool::DumpStatsByTypeID(const std::string& prefix,ceph::Formatter *f,size_t trim = 50);

Where prefix is used to select pools to be examined. A prefix of "" will collect against all pools.
A prefix of "unit" will collect against all pools that start with "unit".

trim controls the size of the stats, you'll get at most "trim" elements and those will be the "trim" LARGEST elements.

For ...ByBytes, ...BySlots, ...BySlabs The output is containers sorted by those criteria.
For ...ByTypeID The output is summed by typeID (i.e., summed across all containers of the same type)

**********************/

namespace mempool {


//
// Adding to this #define creates a mempool of the same name
//
#define DEFINE_MEMORY_POOLS_HELPER(f) \
  f(unittest_1)			      \
  f(unittest_2)			      \
  f(bluestore)


#define P(x) x,
enum pool_index_t {
  DEFINE_MEMORY_POOLS_HELPER(P)
  num_pools        // Must be last.
};
#undef P

struct pool_allocator_base_t;
class pool_t;

//
// Doubly linked list membership.
//
struct list_member_t {
  list_member_t *next;
  list_member_t *prev;
  list_member_t() : next(this), prev(this) {}
  ~list_member_t() { assert(next == this && prev == this); }
  void insert(list_member_t *i) {
    i->next = next;
    i->prev = this;
    next = i;
  }
  void remove() {
    prev->next = next;
    next->prev = prev;
    next = this;
    prev = this;
  }
};

struct shard_t {
  std::atomic<size_t> bytes, items;
  mutable std::mutex lock;  // Only used for containers list
  list_member_t containers;
  shard_t() : bytes(0), items(0) {}
};

//
// Stats structures
//
struct StatsByBytes_t {
  const char* typeID = nullptr;
  size_t items = 0;
  void dump(ceph::Formatter *f) const;
};
struct StatsByItems_t {
  const char *typeID = nullptr;
  size_t bytes = 0;
  void dump(ceph::Formatter *f) const;
};
struct StatsByTypeID_t {
  size_t items = 0;
  size_t bytes = 0;
  void dump(ceph::Formatter *f) const;
};

void FormatStatsByBytes(const std::multimap<size_t,StatsByBytes_t>&m,
			ceph::Formatter *f);
void FormatStatsByItems(const std::multimap<size_t,StatsByItems_t>&m,
			ceph::Formatter *f);
void FormatStatsByTypeID(const std::map<const char *,StatsByTypeID_t>&m,
			 ceph::Formatter *f);

void DumpStatsByBytes(const std::string& prefix, ceph::Formatter *f,
		      size_t trim = 50);
void DumpStatsByItems(const std::string& prefix, ceph::Formatter *f,
		      size_t trim = 50);
void DumpStatsByTypeID(const std::string& prefix, ceph::Formatter *f,
		       size_t trim = 50);

// Root of all allocators, this enables the container information to
// operation easily These fields are "always" accurate ;-)
struct pool_allocator_base_t {
  list_member_t list_member;
  pool_t *pool = nullptr;
  shard_t *shard = nullptr;
  const char *typeID = nullptr;
  bool force_debug = false;
  std::atomic<size_t> items = {0};
  std::atomic<size_t> bytes = {0};

  //
  // Helper functions for Stats
  //
  void UpdateStats(std::multimap<size_t,StatsByBytes_t>& byBytes) const;
  void UpdateStats(std::multimap<size_t,StatsByItems_t>& bySlots) const;
  void UpdateStats(std::map<const char *,StatsByTypeID_t>& byTypeID) const;
  //
  // Effective constructor
  //
  void AttachPool(pool_index_t index, const char *typeID);
  ~pool_allocator_base_t();
};

enum { shard_size = 64 }; // Sharding of headers

pool_t& GetPool(pool_index_t ix);

class pool_t {
  static std::map<std::string,pool_t *> *pool_head;
  static std::mutex pool_head_lock;
  std::string name;
  shard_t shard[shard_size];
  friend class pool_allocator_base_t;
public:
  bool debug;
  //
  // How much this pool consumes. O(<shard-size>)
  //
  size_t allocated_bytes() const;
  size_t allocated_items() const;
  //
  // Aggregate stats by consumed.
  //
  static void StatsByBytes(const std::string& prefix,
			   std::multimap<size_t,StatsByBytes_t>& bybytes,
			   size_t trim);
   static void StatsByItems(const std::string& prefix,
			    std::multimap<size_t,StatsByItems_t>& bySlots,
			    size_t trim);
   static void StatsByTypeID(const std::string& prefix,
			     std::map<const char *,StatsByTypeID_t>& byTypeID,
			     size_t trim);
  shard_t* pick_a_shard() {
    // Dirt cheap, see:
    //   http://fossies.org/dox/glibc-2.24/pthread__self_8c_source.html
    size_t me = (size_t)pthread_self();
    size_t i = (me >> 3) % shard_size;
    return &shard[i];
  }
public:
  pool_t(const std::string& n, bool _debug) : name(n), debug(_debug) {
    std::unique_lock<std::mutex> lock(pool_head_lock);
    if (pool_head == nullptr) {
      pool_head = new std::map<std::string,pool_t *>;
    }
    assert(pool_head->find(name) == pool_head->end());
    (*pool_head)[name] = this;
  }
  virtual ~pool_t() {
    std::unique_lock<std::mutex> lock(pool_head_lock);
    assert(pool_head->find(name) != pool_head->end());
    pool_head->erase(pool_head->find(name));
    if (pool_head->size() == 0) {
      delete pool_head;
      pool_head = nullptr;
    }
  }
  //
  // Tracking of container ctor/dtor
  //
  void AttachAllocator(pool_allocator_base_t *base);
  void DetachAllocator(pool_allocator_base_t *base);
private:
  //
  // Helpers for per-pool stats
  //
  template<typename maptype> void VisitPool(maptype& map,size_t trim) const {
    for (size_t i = 0; i < shard_size; ++i) {
      std::unique_lock<std::mutex> shard_lock(shard[i].lock);
      for (const list_member_t *p = shard[i].containers.next;
	   p != &shard[i].containers;
	   p = p->next) {
	const pool_allocator_base_t *c =
	  reinterpret_cast<const pool_allocator_base_t *>(p);
	c->UpdateStats(map);
	while (map.size() > trim) {
	  map.erase(map.begin());
	}
      }
    }
  }
  template<typename maptype> static void VisitAllPools(
    const std::string& prefix,
    maptype& map,
    size_t trim) {
    //
    // Scan all of the pools for prefix match
    //
    for (size_t i = 0; i < num_pools; ++i) {
      const pool_t &pool = mempool::GetPool((pool_index_t)i);
      if (prefix == pool.name.substr(0, std::min(prefix.size(),
						 pool.name.size()))) {
	pool.VisitPool(map,trim);
      }
    }
  }
};

inline void pool_allocator_base_t::AttachPool(
  pool_index_t index,
  const char *_typeID)
{
  assert(pool == nullptr);
  pool = &GetPool(index);
  shard = pool->pick_a_shard();
  typeID = _typeID;
  if (pool->debug || force_debug) {
    std::unique_lock<std::mutex> lock(shard->lock);
    shard->containers.insert(&list_member);
  }
}

inline pool_allocator_base_t::~pool_allocator_base_t()
{
  if ((pool && pool->debug) || force_debug) {
    std::unique_lock<std::mutex> lock(shard->lock);
    list_member.remove();
  }
}

//
// The ceph::slab_xxxx containers are made from standard STL containers with a custom allocator.
//
// When you declare a slab container you provide 1 or 2 additional integer template parameters that
// modify the memory allocation pattern. The point is to amortize the memory allocations for Slots
// within the container so that the memory allocation time and space overheads are reduced.
//
//  ceph::slab_map     <key,value,stackSize,heapSize = stackSize,compare = less<key>>
//  ceph::slab_multimap<key,value,stackSize,heapSize = stackSize,compare = less<key>>
//  ceph::slab_set     <value,    stackSize,heapSize = stackSize,compare = less<key>>
//  ceph::slab_multiset<value,    stackSize,heapSize = stackSize,compare = less<key>>
//  ceph::list         <value,    stackSize,heapSize = stackSize>
//
//  stackSize indicates the number of Slots that will be allocated within the container itself.
//      in other words, if the container never has more than stackSize Slots, there will be no additional
//      memory allocation calls.
//
//  heapSize indicates the number of Slots that will be requested when a memory allocation is required.
//      In other words, Slots are allocated in batches of heapSize.
//
//  All of this wizardry comes with a price. There are two basic restrictions:
//  (1) Slots allocated in a batch can only be freed in the same batch amount
//  (2) Slots cannot escape a container, i.e., be transferred to another container
//
//  The first restriction suggests that long-lived containers might not want to use this code. As some allocation/free
//      patterns can result in large amounts of unused, but un-freed memory (worst case 'excess' memory occurs when
//      each batch contains only a single in-use node). Worst-case unused memory consumption is thus equal to:
//         container.size() * (heapSize -1) * sizeof(Node)
//      This computation assumes that the slab_xxxx::reserve function is NOT used. If that function is used then
//      the maximum unused memory consumption is related to its parameters.
//  The second restriction means that some functions like list::splice are now O(N), not O(1)
//      list::swap is supported but is O(2N) not O(1) as before.
//      vector::swap is also supported. It converts any stack elements into heap elements and then does an O(1) swap. So
//          it's worst-case runtime is O(2*stackSize), which is likely to be pretty good :)
//      set::swap, multiset::swap, map::swap and multimap::swap are unavailable, but could be implemented if needed (though EXPENSIVELY).
//

//
// fast slab allocator
//
// This is an STL allocator intended for use with short-lived node-heavy containers, i.e., map, set, etc.
//
// Memory is allocated in slabs. Each slab contains a fixed number of slots for objects.
//
// The first slab is part of the object itself, meaning that no memory allocation is required
// if the container doesn't exceed "stackSize" Slots.
//
// Subsequent slabs are allocated using the normal heap. A slab on the heap, by default, contains 'heapSize' Slots.
// However, a "reserve" function (same functionality as vector::reserve) is provided that ensure a minimum number
// of free Slots is available without further memory allocation. If the slab_xxxx::reserve function needs to allocate
// additional Slots, only a single memory allocation will be done. Meaning that it's possible to have slabs that
// are larger (or smaller) than 'heapSize' Slots.
//

template<typename T>
class pool_allocator : public pool_allocator_base_t {
  pool_allocator *selfPointer;   ///< for selfCheck

public:
  typedef pool_allocator<T> allocator_type;
  typedef T value_type;
  typedef value_type *pointer;
  typedef const value_type * const_pointer;
  typedef value_type& reference;
  typedef const value_type& const_reference;
  typedef std::size_t size_type;
  typedef std::ptrdiff_t difference_type;

  template<typename U> struct rebind {
    typedef pool_allocator<U> other;
  };

  pool_allocator(bool _force_debug=false) {
    force_debug = _force_debug;
    typeID = typeid(*this).name();
    selfPointer = this;
  }
  ~pool_allocator() {
  }

  pointer allocate(size_t n, void *p = nullptr) {
    size_t total = sizeof(T) * n;
    shard->bytes += total;
    shard->items += n;
    bytes += total;
    items += n;
    pointer r = reinterpret_cast<pointer>(new char[total]);
    return r;
  }

  void deallocate(pointer p, size_type n) {
    size_t total = sizeof(T) * n;
    shard->bytes -= total;
    shard->items -= n;
    bytes -= total;
    items -= n;
    delete[] reinterpret_cast<char*>(p);
  }

  void destroy(pointer p) {
    p->~T();
  }

  template<class U>
  void destroy(U *p) {
    p->~U();
  }

  void construct(pointer p, const_reference val) {
    ::new ((void *)p) T(val);
  }

  template<class U, class... Args> void construct(U* p,Args&&... args) {
    ::new((void *)p) U(std::forward<Args>(args)...);
  }

  bool operator==(const pool_allocator&) { return true; }
  bool operator!=(const pool_allocator&) { return false; }

  void selfCheck() const {
    // If you fail here, the horrible get_my_allocator hack is failing.
    assert(this == selfPointer);
  }

private:
  // Can't copy or assign this guy
  pool_allocator(pool_allocator&) = delete;
  pool_allocator(pool_allocator&&) = delete;
  void operator=(const pool_allocator&) = delete;
  void operator=(const pool_allocator&&) = delete;
};

//
// Extended containers
//

// std::map  
template<
  pool_index_t pool_ix,
  typename key,
  typename value,
  typename compare = std::less<key> >
struct map :
    public std::map<key,value,compare,pool_allocator<std::pair<key,value>> > {
  map() {
    get_my_actual_allocator()->AttachPool(pool_ix, typeid(*this).name());
  }

  typedef pool_allocator<std::pair<key,value>> my_alloc_type;
  my_alloc_type * get_my_actual_allocator() {
    my_alloc_type *alloc = reinterpret_cast<my_alloc_type *>(this);
    alloc->selfCheck();
    return alloc;
  }
};

// std::multimap
template<
  pool_index_t pool_ix,
  typename key,
  typename value,
  typename compare = std::less<key> >
struct multimap : public std::multimap<key,value,compare,
				       pool_allocator<std::pair<key,value>> > {
  multimap() {
    get_my_actual_allocator()->AttachPool(pool_ix,typeid(*this).name());
  }

  typedef pool_allocator<std::pair<key,value>> my_alloc_type;
  my_alloc_type * get_my_actual_allocator() {
    my_alloc_type *alloc = reinterpret_cast<my_alloc_type *>(this);
    alloc->selfCheck();
    return alloc;
  }
};

// std::set
template<
  pool_index_t pool_ix,
  typename key,
  typename compare = std::less<key> >
struct set : public std::set<key,compare,pool_allocator<key> > {
  set() {
    get_my_actual_allocator()->AttachPool(pool_ix,typeid(*this).name());
  }

  typedef pool_allocator<key> my_alloc_type;
  my_alloc_type * get_my_actual_allocator() {
    my_alloc_type *alloc = reinterpret_cast<my_alloc_type *>(this);
    alloc->selfCheck();
    return alloc;
  }
};

// std::multiset
template<
  pool_index_t pool_ix,
  typename key,
  typename compare = std::less<key> >
struct multiset : public std::multiset<key,compare,pool_allocator<key> > {
  multiset() {
    get_my_actual_allocator()->AttachPool(pool_ix,typeid(*this).name());
  }

  typedef pool_allocator<key> my_alloc_type;
  my_alloc_type * get_my_actual_allocator() {
    my_alloc_type *alloc = reinterpret_cast<my_alloc_type *>(this);
    alloc->selfCheck();
    return alloc;
  }
};

// std::list
template<
  pool_index_t pool_ix,
  typename node>
struct list : public std::list<node,pool_allocator<node> > {
  list() {
    get_my_actual_allocator()->AttachPool(pool_ix, typeid(*this).name());
  }
  list(const list& o) {
    get_my_actual_allocator()->AttachPool(pool_ix,typeid(*this).name());
    copy(o);
  };
  list& operator=(const list& o) {
    copy(o);
    return *this;
  }

  typedef pool_allocator<node> my_alloc_type;
  my_alloc_type * get_my_actual_allocator() {
    my_alloc_type *alloc = reinterpret_cast<my_alloc_type *>(this);
    alloc->selfCheck();
    return alloc;
  }
};

// std::vector
template<
  pool_index_t pool_ix,
  typename node>
struct vector : public std::vector<node,pool_allocator<node> > {
  vector() {
    get_my_actual_allocator()->AttachPool(pool_ix, typeid(*this).name());
  }
  vector(size_t s) {
    get_my_actual_allocator()->AttachPool(pool_ix, typeid(*this).name());
    this->reserve(s);
  }

  typedef pool_allocator<node> my_alloc_type;
  my_alloc_type * get_my_actual_allocator() {
    my_alloc_type *alloc = reinterpret_cast<my_alloc_type *>(this);
    alloc->selfCheck();
    return alloc;
  }
};

  
// Finally, a way to allocate non-container objects :) This only
// supports standalone objects, not arrays. Arrays could be added if
// they're needed.
template<pool_index_t pool_ix,typename o>
class factory {
  pool_allocator<o> alloc;
public:
  factory()
    : alloc(true) { // force debug
    alloc.AttachPool(pool_ix,typeid(*this).name());
  }
  void *allocate() {
    return (void *)alloc.allocate(1);
  }
  void  free(void *p) {
    alloc.deallocate((o *)p, 1);
  }
  size_t allocated_items() {
    return alloc.items;
  }
  size_t allocated_bytes() {
    return alloc.bytes;
  }
};

};


// Namespace mempool

#define P(x)								\
  namespace x {								\
    template<typename k,typename v, typename cmp = std::less<k> >	\
    using map = mempool::map<mempool::x,k,v,cmp>;			\
    template<typename k,typename v, typename cmp = std::less<k> >	\
    using multimap = mempool::multimap<mempool::x,k,v,cmp>;		\
    template<typename k, typename cmp = std::less<k> >			\
    using set = mempool::set<mempool::x,k,cmp>;				\
    template<typename v>						\
    using list = mempool::list<mempool::x,v>;				\
    template<typename v>						\
    using vector = mempool::vector<mempool::x,v>;			\
    template<typename v>						\
    using factory = mempool::factory<mempool::x,v>;			\
    inline size_t allocated_bytes() {					\
      return mempool::GetPool(mempool::x).allocated_bytes();		\
    }									\
    inline size_t allocated_items() {					\
      return mempool::GetPool(mempool::x).allocated_items();		\
    }									\
  };

DEFINE_MEMORY_POOLS_HELPER(P)

#undef P

//
// Helper macros for proper implementation of object factories.
//
// Use this macro to create type-specific operator new and delete.
// It should be part of the
//
#define MEMBER_OF_MEMPOOL()	    \
   void *operator new(size_t size); \
   void *operator new[](size_t size) { assert(0 == "No array new"); } \
   void  operator delete(void *); \
   void  operator delete[](void *) { assert(0 == "no array delete"); }

// Use this macro in some particular .cc file to match the above macro
// It creates the object factory and creates the relevant operator new
// and delete stuff

#define DEFINE_OBJECT_IN_MEMPOOL(obj,factoryname,pool)			\
  static pool::factory<obj> _factory_##factoryname;			\
  void * obj::operator new(size_t size) {				\
    assert(size == sizeof(obj));					\
    return _factory_##factoryname.allocate();				\
  }									\
  void obj::operator delete(void *p)  {					\
    _factory_##factoryname.free(p);					\
  }


#endif
