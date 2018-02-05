// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013- Sage Weil <sage@inktank.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_OSD_PETSTORE_H
#define CEPH_OSD_PETSTORE_H

#include <mutex>
#include <boost/intrusive_ptr.hpp>

#include "include/unordered_map.h"
#include "include/memory.h"
#include "common/Finisher.h"
#include "common/RefCountedObj.h"
#include "common/RWLock.h"
#include "os/ObjectStore.h"
#include "include/assert.h"
#include "os/fs/FS.h"
#include "PetObject.h"
#include "BufferListObject.h"
#include "VectorObject.h"

//#include "Collection.h"

class PetStore : public ObjectStore {
public:
  typedef PetObject::Ref PetObjectRef;
//  struct Collection;

  struct Collection : public CollectionImpl {
    coll_t cid;
    int bits = 0;
    CephContext *cct;
    ceph::unordered_map<ghobject_t, PetObjectRef> object_hash;  ///< for lookup
    map<ghobject_t, PetObjectRef> object_map;        ///< for iteration
    map<string,bufferptr> xattr;
    RWLock lock;   ///< for object_{map,hash}
    bool exists;

    typedef boost::intrusive_ptr<Collection> Ref;
    friend void intrusive_ptr_add_ref(Collection *c) { c->get(); }
    friend void intrusive_ptr_release(Collection *c) { c->put(); }

    const coll_t &get_cid() override {
      return cid;
    }

    PetObjectRef create_object(const ghobject_t& oid) const;

    // NOTE: The lock only needs to protect the object_map/hash, not the
    // contents of individual objects.  The osd is already sequencing
    // reads and writes, so we will never see them concurrently at this
    // level.

    PetObjectRef get_object(ghobject_t oid) {
      RWLock::RLocker l(lock);
      auto o = object_hash.find(oid);
      if (o == object_hash.end())
	return PetObjectRef();
      return o->second;
    }

    PetObjectRef get_or_create_object(ghobject_t oid) {
      RWLock::WLocker l(lock);
      auto result = object_hash.emplace(oid, PetObjectRef());
      if (result.second)
        object_map[oid] = result.first->second = create_object(oid);
      return result.first->second;
    }

    void encode(bufferlist& bl) const {
      ENCODE_START(1, 1, bl);
      ::encode(xattr, bl);
      uint32_t s = object_map.size();
      ::encode(s, bl);
      for (map<ghobject_t, PetObjectRef>::const_iterator p = object_map.begin();
	   p != object_map.end();
	   ++p) {
	::encode(p->first, bl);
	p->second->encode(bl);
      }
      ENCODE_FINISH(bl);
    }
    void decode(bufferlist::iterator& p) {
      DECODE_START(1, p);
      ::decode(xattr, p);
      uint32_t s;
      ::decode(s, p);
      while (s--) {
	ghobject_t k;
	::decode(k, p);
	auto o = create_object(k);
	o->decode(p);
	object_map.insert(make_pair(k, o));
	object_hash.insert(make_pair(k, o));
      }
      DECODE_FINISH(p);
    }

    uint64_t used_bytes() const {
      uint64_t result = 0;
      for (map<ghobject_t, PetObjectRef>::const_iterator p = object_map.begin();
	   p != object_map.end();
	   ++p) {
        result += p->second->get_size();
      }

      return result;
    }

    explicit Collection(CephContext *cct, coll_t c)
      : cid(c),
	cct(cct),
        lock("PetStore::Collection::lock", true, false),
	exists(true) {}
  };
  typedef Collection::Ref CollectionRef;

private:
  KeyValueDB *db = nullptr;
  FS *fs = nullptr;
  uuid_d fsid;
  int path_fd = -1;
  int fsid_fd = -1;
  int objects_fd = -1;
  int oset_fd = -1;
  bool mounted = false;

  class OmapIteratorImpl;

  ceph::unordered_map<__le32, std::function<int(Transaction::iterator& i,
                                                Transaction::Op *op)>> trans_map;
  ceph::unordered_map<coll_t, Collection::Ref> coll_map;
  RWLock coll_lock = {"PetStore::coll_lock"};    ///< rwlock to protect coll_map

  Collection::Ref get_collection(const coll_t& cid);

  Finisher finisher;

  uint64_t used_bytes;

  // --------------------------------------------------------
  // private methods

  int _open_path();
  void _close_path();
  int _open_fsid(bool create);
  int _lock_fsid();
  int _read_fsid(uuid_d *f);
  int _write_fsid();
  void _close_fsid();
  int _open_objects_dir();
  int _create_objects_dir();
  void _close_objects_dir();
  int _open_db(bool create);
  void _close_db();

  void _do_transaction(Transaction& t);

  // private operation methods
  int _op_touch(Transaction::iterator& i, Transaction::Op *op);
  int _op_write(Transaction::iterator& i, Transaction::Op *op);
  int _op_zero(Transaction::iterator& i, Transaction::Op *op);
  int _op_truncate(Transaction::iterator& i, Transaction::Op *op);
  int _op_remove(Transaction::iterator& i, Transaction::Op *op);
  int _op_setattr(Transaction::iterator& i, Transaction::Op *op);
  int _op_setattrs(Transaction::iterator& i, Transaction::Op *op);
  int _op_rmattr(Transaction::iterator& i, Transaction::Op *op);
  int _op_rmattrs(Transaction::iterator& i, Transaction::Op *op);
  int _op_clone(Transaction::iterator& i, Transaction::Op *op);
  int _op_clonerange(Transaction::iterator& i, Transaction::Op *op);
  int _op_clonerange2(Transaction::iterator& i, Transaction::Op *op);
  int _op_mkcoll(Transaction::iterator& i, Transaction::Op *op);
  int _op_coll_hint(Transaction::iterator& i, Transaction::Op *op);
  int _op_rmcoll(Transaction::iterator& i, Transaction::Op *op);
  int _op_coll_add(Transaction::iterator& i, Transaction::Op *op);
  int _op_coll_remove(Transaction::iterator& i, Transaction::Op *op);
  int _op_coll_move_rename(Transaction::iterator& i, Transaction::Op *op);
  int _op_try_rename(Transaction::iterator& i, Transaction::Op *op);
  int _op_omap_clear(Transaction::iterator& i, Transaction::Op *op);
  int _op_omap_setkeys(Transaction::iterator& i, Transaction::Op *op);
  int _op_omap_rmkeys(Transaction::iterator& i, Transaction::Op *op);
  int _op_omap_rmkeyrange(Transaction::iterator& i, Transaction::Op *op);
  int _op_omap_setheader(Transaction::iterator& i, Transaction::Op *op);
  int _op_split_collection2(Transaction::iterator& i, Transaction::Op *op);
  int _op_set_allochint(Transaction::iterator& i, Transaction::Op *op);


  int _save();
  int _load();

  void dump(Formatter *f);
  void dump_all();

public:
  PetStore(CephContext *cct, const string& path);
  ~PetStore() override {}; 

  string get_type() override {
    return "petstore";
  }

  bool test_mount_in_use();

  int mount() override;
  int umount() override;

  int fsck(bool deep) override {
    return 0;
  }

  int validate_hobject_key(const hobject_t &obj) const override {
    return 0;
  }
  unsigned get_max_attr_name_length() override {
    return 256;  // arbitrary; there is no real limit internally
  }

  int mkfs() override;
  int mkjournal() override {
    return 0;
  }
  bool wants_journal() override {
    return false;
  }
  bool allows_journal() override {
    return false;
  }
  bool needs_journal() override {
    return false;
  }

  int get_devices(set<string> *ls) override {
    // no devices for us!
    return 0;
  }

  int statfs(struct store_statfs_t *buf) override;

  bool exists(const coll_t& cid, const ghobject_t& oid) override;
  bool exists(CollectionHandle &c, const ghobject_t& oid) override;
  int stat(const coll_t& cid, const ghobject_t& oid,
	   struct stat *st, bool allow_eio = false) override;
  int stat(CollectionHandle &c, const ghobject_t& oid,
	   struct stat *st, bool allow_eio = false) override;
  int set_collection_opts(
    const coll_t& cid,
    const pool_opts_t& opts) override;
  int read(
    const coll_t& cid,
    const ghobject_t& oid,
    uint64_t offset,
    size_t len,
    bufferlist& bl,
    uint32_t op_flags = 0) override;
  int read(
    CollectionHandle &c,
    const ghobject_t& oid,
    uint64_t offset,
    size_t len,
    bufferlist& bl,
    uint32_t op_flags = 0) override;
  using ObjectStore::fiemap;
  int fiemap(const coll_t& cid, const ghobject_t& oid, uint64_t offset, size_t len, bufferlist& bl) override;
  int fiemap(const coll_t& cid, const ghobject_t& oid, uint64_t offset, size_t len, map<uint64_t, uint64_t>& destmap) override;
  int getattr(const coll_t& cid, const ghobject_t& oid, const char *name,
	      bufferptr& value) override;
  int getattr(CollectionHandle &c, const ghobject_t& oid, const char *name,
	      bufferptr& value) override;
  int getattrs(const coll_t& cid, const ghobject_t& oid,
	       map<string,bufferptr>& aset) override;
  int getattrs(CollectionHandle &c, const ghobject_t& oid,
	       map<string,bufferptr>& aset) override;

  int list_collections(vector<coll_t>& ls) override;

  CollectionHandle open_collection(const coll_t& c) override {
    return get_collection(c);
  }
  bool collection_exists(const coll_t& c) override;
  int collection_empty(const coll_t& c, bool *empty) override;
  int collection_bits(const coll_t& c) override;
  using ObjectStore::collection_list;
  int collection_list(const coll_t& cid,
		      const ghobject_t& start, const ghobject_t& end, int max,
		      vector<ghobject_t> *ls, ghobject_t *next) override;

  using ObjectStore::omap_get;
  int omap_get(
    const coll_t& cid,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    bufferlist *header,      ///< [out] omap header
    map<string, bufferlist> *out /// < [out] Key to value map
    ) override;

  using ObjectStore::omap_get_header;
  /// Get omap header
  int omap_get_header(
    const coll_t& cid,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    bufferlist *header,      ///< [out] omap header
    bool allow_eio = false ///< [in] don't assert on eio
    ) override;

  using ObjectStore::omap_get_keys;
  /// Get keys defined on oid
  int omap_get_keys(
    const coll_t& cid,              ///< [in] Collection containing oid
    const ghobject_t &oid, ///< [in] Object containing omap
    set<string> *keys      ///< [out] Keys defined on oid
    ) override;

  using ObjectStore::omap_get_values;
  /// Get key values
  int omap_get_values(
    const coll_t& cid,                    ///< [in] Collection containing oid
    const ghobject_t &oid,       ///< [in] Object containing omap
    const set<string> &keys,     ///< [in] Keys to get
    map<string, bufferlist> *out ///< [out] Returned keys and values
    ) override;

  using ObjectStore::omap_check_keys;
  /// Filters keys into out which are defined on oid
  int omap_check_keys(
    const coll_t& cid,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    const set<string> &keys, ///< [in] Keys to check
    set<string> *out         ///< [out] Subset of keys defined on oid
    ) override;

  using ObjectStore::get_omap_iterator;
  ObjectMap::ObjectMapIterator get_omap_iterator(
    const coll_t& cid,              ///< [in] collection
    const ghobject_t &oid  ///< [in] object
    ) override;

  void set_fsid(uuid_d u) override;
  uuid_d get_fsid() override;

  uint64_t estimate_objects_overhead(uint64_t num_objects) override {
    return 0; //do not care
  }

  objectstore_perf_stat_t get_cur_stats() override;

  const PerfCounters* get_perf_counters() const override {
    return nullptr;
  }


  int queue_transactions(
    Sequencer *osr, vector<Transaction>& tls,
    TrackedOpRef op = TrackedOpRef(),
    ThreadPool::TPHandle *handle = NULL) override;
};
/*
struct Object : public RefCountedObject {
  std::mutex xattr_mutex;
  std::mutex omap_mutex;
  map<string,bufferptr> xattr;
  bufferlist omap_header;
  map<string,bufferlist> omap;

  typedef boost::intrusive_ptr<Object> Ref;
  friend void intrusive_ptr_add_ref(Object *o) { o->get(); }
  friend void intrusive_ptr_release(Object *o) { o->put(); }

  Object() : RefCountedObject(nullptr, 0) {}
  // interface for object data
  virtual size_t get_size() const = 0;
  virtual int read(uint64_t offset, uint64_t len, bufferlist &bl) = 0;
  virtual int write(uint64_t offset, const bufferlist &bl) = 0;
  virtual int clone(Object *src, uint64_t srcoff, uint64_t len,
                      uint64_t dstoff) = 0;
  virtual int truncate(uint64_t offset) = 0;
  virtual void encode(bufferlist& bl) const = 0;
  virtual void decode(bufferlist::iterator& p) = 0;

  void encode_base(bufferlist& bl) const {
    ::encode(xattr, bl);
    ::encode(omap_header, bl);
    ::encode(omap, bl);
  }
  void decode_base(bufferlist::iterator& p) {
    ::decode(xattr, p);
    ::decode(omap_header, p);
    ::decode(omap, p);
  }

  void dump(Formatter *f) const {
    f->dump_int("data_len", get_size());
    f->dump_int("omap_header_len", omap_header.length());

    f->open_array_section("xattrs");
    for (map<string,bufferptr>::const_iterator p = xattr.begin();
         p != xattr.end();
         ++p) {
      f->open_object_section("xattr");
      f->dump_string("name", p->first);
      f->dump_int("length", p->second.length());
      f->close_section();
    }
    f->close_section();

    f->open_array_section("omap");
    for (map<string,bufferlist>::const_iterator p = omap.begin();
         p != omap.end();
         ++p) {
      f->open_object_section("pair");
      f->dump_string("key", p->first);
      f->dump_int("length", p->second.length());
      f->close_section();
    }
    f->close_section();
  }
};
*/
struct Collection : public ObjectStore::CollectionImpl {
  coll_t cid;
  int bits = 0;
  CephContext *cct;
  ceph::unordered_map<ghobject_t, PetObject::Ref> object_hash;  ///< for lookup
  map<ghobject_t, PetObject::Ref> object_map;        ///< for iteration
  map<string,bufferptr> xattr;
  RWLock lock;   ///< for object_{map,hash}
  bool exists;

  typedef boost::intrusive_ptr<Collection> Ref;
  friend void intrusive_ptr_add_ref(Collection *c) { c->get(); }
  friend void intrusive_ptr_release(Collection *c) { c->put(); }

  const coll_t &get_cid() override {
    return cid;
  }

  PetObject::Ref create_object(const ghobject_t& oid) const;

// NOTE: The lock only needs to protect the object_map/hash, not the
// contents of individual objects.  The osd is already sequencing
// reads and writes, so we will never see them concurrently at this
// level.

  PetObject::Ref get_object(ghobject_t oid) {
    RWLock::RLocker l(lock);
    auto o = object_hash.find(oid);
    if (o == object_hash.end())
      return PetObject::Ref();
    return o->second;
  }

  PetObject::Ref get_or_create_object(ghobject_t oid) {
    RWLock::WLocker l(lock);
    auto result = object_hash.emplace(oid, PetObject::Ref());
    if (result.second)
      object_map[oid] = result.first->second = create_object(oid);
    return result.first->second;
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(xattr, bl);
    uint32_t s = object_map.size();
    ::encode(s, bl);
    for (map<ghobject_t, PetObject::Ref>::const_iterator p = object_map.begin();
         p != object_map.end();
         ++p) {
      ::encode(p->first, bl);
      p->second->encode(bl);
    }
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& p) {
    DECODE_START(1, p);
    ::decode(xattr, p);
    uint32_t s;
    ::decode(s, p);
    while (s--) {
      ghobject_t k;
      ::decode(k, p);
      auto o = create_object(k);
      o->decode(p);
      object_map.insert(make_pair(k, o));
      object_hash.insert(make_pair(k, o));
    }
    DECODE_FINISH(p);
  }

  uint64_t used_bytes() const {
    uint64_t result = 0;
    for (map<ghobject_t, PetObject::Ref>::const_iterator p = object_map.begin();
         p != object_map.end();
         ++p) {
      result += p->second->get_size();
    }

    return result;
  }

  explicit Collection(CephContext *cct, coll_t c)
    : cid(c),
      cct(cct),
      lock("PetStore::Collection::lock", true, false),
      exists(true) {}
};

#endif
