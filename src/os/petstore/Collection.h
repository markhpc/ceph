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
#ifndef CEPH_OSD_PETSTORE_COLLECTION_H
#define CEPH_OSD_PETSTORE_COLLECTION_H

#include "Object.h"
#include "os/ObjectStore.h"

struct Collection : public CollectionImpl {
  coll_t cid;
  int bits = 0;
  CephContext *cct;
  ceph::unordered_map<ghobject_t, ObjectRef> object_hash;  ///< for lookup
  map<ghobject_t, ObjectRef> object_map;        ///< for iteration
  map<string,bufferptr> xattr;
  RWLock lock;   ///< for object_{map,hash}
  bool exists;

  typedef boost::intrusive_ptr<Collection> Ref;
  friend void intrusive_ptr_add_ref(Collection *c) { c->get(); }
  friend void intrusive_ptr_release(Collection *c) { c->put(); }

  const coll_t &get_cid() override {
    return cid;
  }

  ObjectRef create_object() const;

// NOTE: The lock only needs to protect the object_map/hash, not the
// contents of individual objects.  The osd is already sequencing
// reads and writes, so we will never see them concurrently at this
// level.

  ObjectRef get_object(ghobject_t oid) {
    RWLock::RLocker l(lock);
    auto o = object_hash.find(oid);
    if (o == object_hash.end())
      return ObjectRef();
    return o->second;
  }

  ObjectRef get_or_create_object(ghobject_t oid) {
    RWLock::WLocker l(lock);
    auto result = object_hash.emplace(oid, ObjectRef());
    if (result.second)
      object_map[oid] = result.first->second = create_object();
    return result.first->second;
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(xattr, bl);
    uint32_t s = object_map.size();
    ::encode(s, bl);
    for (map<ghobject_t, ObjectRef>::const_iterator p = object_map.begin();
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
      auto o = create_object();
      o->decode(p);
      object_map.insert(make_pair(k, o));
      object_hash.insert(make_pair(k, o));
    }
    DECODE_FINISH(p);
  }

  uint64_t used_bytes() const {
    uint64_t result = 0;
    for (map<ghobject_t, ObjectRef>::const_iterator p = object_map.begin();
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

#endif
