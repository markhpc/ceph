/// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#include "acconfig.h"

#ifdef HAVE_SYS_MOUNT_H
#include <sys/mount.h>
#endif

#ifdef HAVE_SYS_PARAM_H
#include <sys/param.h>
#endif

#include "include/types.h"
#include "include/stringify.h"
#include "include/unordered_map.h"
#include "include/memory.h"
#include "common/errno.h"
#include "PetStore.h"
#include "include/compat.h"
#include "common/safe_io.h"

#define dout_context cct
#define dout_subsys ceph_subsys_petstore
#undef dout_prefix
#define dout_prefix *_dout << "petstore(" << path << ") "

PetStore::PetStore(CephContext *cct, const string& path)
    : ObjectStore(cct, path),
      coll_lock("PetStore::coll_lock"),
      finisher(cct),
      used_bytes(0)
{
}


// for comparing collections for lock ordering
bool operator>(const PetStore::CollectionRef& l,
	       const PetStore::CollectionRef& r)
{
  return (unsigned long)l.get() > (unsigned long)r.get();
}

int PetStore::mount()
{
  int r = _load();
  if (r < 0)
    return r;
  finisher.start();
  return 0;
}


/*
int PetStore::mount()
{
  dout(1) << __func__ << " path " << path << dendl;

  int r = _open_path();
  if (r < 0)
    return r;
  r = _open_fsid(false);
  if (r < 0)
    goto out_path;

  r = _read_fsid(&fsid);
  if (r < 0)
    goto out_fsid;

  r = _lock_fsid();
  if (r < 0)
    goto out_fsid;

  r = _open_objects_dir();
  if (r < 0)
    goto out_fsid;

  // FIXME: superblock, features

  r = _open_db(false);
  if (r < 0)
    goto out_objects_dir;

  r = _recover_next_fid();
  if (r < 0)
    goto out_db;

  r = _recover_next_nid();
  if (r < 0)
    goto out_db;

  r = _open_collections();
  if (r < 0)
    goto out_db;

  finisher.start();

  mounted = true;
  return 0;

 out_db:
  _close_db();
 out_objects_dir:
  _close_objects_dir();
 out_fsid:
//  _close_fsid();
 out_path:
  _close_path();
  return r;
}
*/

int PetStore::umount()
{
  finisher.wait_for_empty();
  finisher.stop();
  return _save();
}

/*
int PetStore::umount()
{
  assert(mounted);
  dout(1) << __func__ << dendl;

  _sync();
  _reap_collections();

  dout(20) << __func__ << " draining finisher" << dendl;
  finisher.wait_for_empty();
  dout(20) << __func__ << " stopping finisher" << dendl;
  finisher.stop();
  dout(20) << __func__ << " closing" << dendl;

  mounted = false;
  if (oset_fd >= 0)
    VOID_TEMP_FAILURE_RETRY(::close(oset_fd));
  _close_db();
  _close_objects_dir();
//  _close_fsid();
  _close_path();

  return _save();
}
*/

int PetStore::_save()
{
  dout(10) << __func__ << dendl;
  dump_all();
  set<coll_t> collections;
  for (ceph::unordered_map<coll_t,CollectionRef>::iterator p = coll_map.begin();
       p != coll_map.end();
       ++p) {
    dout(20) << __func__ << " coll " << p->first << " " << p->second << dendl;
    collections.insert(p->first);
    bufferlist bl;
    assert(p->second);
    p->second->encode(bl);
    string fn = path + "/" + stringify(p->first);
    int r = bl.write_file(fn.c_str());
    if (r < 0)
      return r;
  }

  string fn = path + "/collections";
  bufferlist bl;
  ::encode(collections, bl);
  int r = bl.write_file(fn.c_str());
  if (r < 0)
    return r;

  return 0;
}

void PetStore::dump_all()
{
  Formatter *f = Formatter::create("json-pretty");
  f->open_object_section("store");
  dump(f);
  f->close_section();
  dout(0) << "dump:";
  f->flush(*_dout);
  *_dout << dendl;
  delete f;
}

void PetStore::dump(Formatter *f)
{
  f->open_array_section("collections");
  for (ceph::unordered_map<coll_t,CollectionRef>::iterator p = coll_map.begin();
       p != coll_map.end();
       ++p) {
    f->open_object_section("collection");
    f->dump_string("name", stringify(p->first));

    f->open_array_section("xattrs");
    for (map<string,bufferptr>::iterator q = p->second->xattr.begin();
	 q != p->second->xattr.end();
	 ++q) {
      f->open_object_section("xattr");
      f->dump_string("name", q->first);
      f->dump_int("length", q->second.length());
      f->close_section();
    }
    f->close_section();

    f->open_array_section("objects");
    for (map<ghobject_t,PetObjectRef>::iterator q = p->second->object_map.begin();
	 q != p->second->object_map.end();
	 ++q) {
      f->open_object_section("object");
      f->dump_string("name", stringify(q->first));
      if (q->second)
	q->second->dump(f);
      f->close_section();
    }
    f->close_section();

    f->close_section();
  }
  f->close_section();
}

int PetStore::_load()
{
  dout(10) << __func__ << dendl;
  bufferlist bl;
  string fn = path + "/collections";
  string err;
  int r = bl.read_file(fn.c_str(), &err);
  if (r < 0)
    return r;

  set<coll_t> collections;
  bufferlist::iterator p = bl.begin();
  ::decode(collections, p);

  for (set<coll_t>::iterator q = collections.begin();
       q != collections.end();
       ++q) {
    string fn = path + "/" + stringify(*q);
    bufferlist cbl;
    int r = cbl.read_file(fn.c_str(), &err);
    if (r < 0)
      return r;
    CollectionRef c(new Collection(cct, *q));
    bufferlist::iterator p = cbl.begin();
    c->decode(p);
    coll_map[*q] = c;
    used_bytes += c->used_bytes();
  }

  dump_all();

  return 0;
}

void PetStore::set_fsid(uuid_d u)
{
  int r = write_meta("fsid", stringify(u));
  assert(r >= 0);
}

uuid_d PetStore::get_fsid()
{
  string fsid_str;
  int r = read_meta("fsid", &fsid_str);
  assert(r >= 0);
  uuid_d uuid;
  bool b = uuid.parse(fsid_str.c_str());
  assert(b);
  return uuid;
}

int PetStore::mkfs()
{
  string fsid_str;
  int r = read_meta("fsid", &fsid_str);
  if (r == -ENOENT) {
    uuid_d fsid;
    fsid.generate_random();
    fsid_str = stringify(fsid);
    r = write_meta("fsid", fsid_str);
    if (r < 0)
      return r;
    dout(1) << __func__ << " new fsid " << fsid_str << dendl;
  } else if (r < 0) {
    return r;
  } else {
    dout(1) << __func__ << " had fsid " << fsid_str << dendl;
  }

  string fn = path + "/collections";
  derr << path << dendl;
  bufferlist bl;
  set<coll_t> collections;
  ::encode(collections, bl);
  r = bl.write_file(fn.c_str());
  if (r < 0)
    return r;

  r = write_meta("type", "petstore");
  if (r < 0)
    return r;

  return 0;
}


/*
int PetStore::mkfs()
{
  dout(1) << __func__ << " path " << path << dendl;
  int r;
  uuid_d old_fsid;

// --------------------------
// memstore


  string fsid_str;
  r = read_meta("fsid", &fsid_str);
  if (r == -ENOENT) {
    uuid_d fsid;
    fsid.generate_random();
    fsid_str = stringify(fsid);
    r = write_meta("fsid", fsid_str);
    if (r < 0)
      return r;
    dout(1) << __func__ << " new fsid " << fsid_str << dendl;
  } else if (r < 0) {
    return r;
  } else {  
    dout(1) << __func__ << " had fsid " << fsid_str << dendl;
  }

  string fn = path + "/collections";
  derr << path << dendl;
  bufferlist bl;
  set<coll_t> collections;
  ::encode(collections, bl);
  r = bl.write_file(fn.c_str());
  if (r < 0)
    return r;

  r = write_meta("type", "petstore");

  if (r < 0)
       return r;

// --------------------------

  r = _open_path();
  if (r < 0)
    return r;

  r = _open_fsid(true);
  if (r < 0)
    goto out_path_fd;

  r = _lock_fsid();
  if (r < 0)
    goto out_close_fsid;

  r = _read_fsid(&old_fsid);
  if (r < 0 && old_fsid.is_zero()) {
    if (fsid.is_zero()) {
      fsid.generate_random();
      dout(1) << __func__ << " generated fsid " << fsid << dendl;
    } else {
      dout(1) << __func__ << " using provided fsid " << fsid << dendl;
    }
    r = _write_fsid();
    if (r < 0)
      goto out_close_fsid;
  } else {
    if (!fsid.is_zero() && fsid != old_fsid) {
      derr << __func__ << " on-disk fsid " << old_fsid
           << " != provided " << fsid << dendl;
      r = -EINVAL;
      goto out_close_fsid;
    }
    fsid = old_fsid;
    dout(1) << __func__ << " fsid is already set to " << fsid << dendl;
  }

  r = _create_objects_dir();
  if (r < 0)
    goto out_close_fsid;

  r = _open_db(true);
  if (r < 0)
    goto out_close_objects_dir;

  // FIXME: superblock
  dout(10) << __func__ << " success" << dendl;
  r = 0;
  _close_db();

 out_close_objects_dir:
  _close_objects_dir();
 out_close_fsid:
//  _close_fsid();
 out_path_fd:
  _close_path();
  return r;
}
*/

int PetStore::statfs(struct store_statfs_t *st)
{
   dout(10) << __func__ << dendl;
  st->reset();
  st->total = cct->_conf->get_val<uint64_t>("petstore_device_bytes");
  st->available = MAX(int64_t(st->total) - int64_t(used_bytes), 0ll);
  dout(10) << __func__ << ": used_bytes: " << used_bytes
	   << "/" << cct->_conf->get_val<uint64_t>("petstore_device_bytes") << dendl;
  return 0;
}

objectstore_perf_stat_t PetStore::get_cur_stats()
{
  // fixme
  return objectstore_perf_stat_t();
}

PetStore::CollectionRef PetStore::get_collection(const coll_t& cid)
{
  RWLock::RLocker l(coll_lock);
  ceph::unordered_map<coll_t,CollectionRef>::iterator cp = coll_map.find(cid);
  if (cp == coll_map.end())
    return CollectionRef();
  return cp->second;
}


// ---------------
// read operations

bool PetStore::exists(const coll_t& cid, const ghobject_t& oid)
{
  CollectionHandle c = get_collection(cid);
  if (!c)
    return false;
  return exists(c, oid);
}

bool PetStore::exists(CollectionHandle &c_, const ghobject_t& oid)
{
  Collection *c = static_cast<Collection*>(c_.get());
  dout(10) << __func__ << " " << c->get_cid() << " " << oid << dendl;
  if (!c->exists)
    return false;

  // Perform equivalent of c->get_object_(oid) != NULL. In C++11 the
  // shared_ptr needs to be compared to nullptr.
  return (bool)c->get_object(oid);
}

int PetStore::stat(
    const coll_t& cid,
    const ghobject_t& oid,
    struct stat *st,
    bool allow_eio)
{
  CollectionHandle c = get_collection(cid);
  if (!c)
    return -ENOENT;
  return stat(c, oid, st, allow_eio);
}

int PetStore::stat(
  CollectionHandle &c_,
  const ghobject_t& oid,
  struct stat *st,
  bool allow_eio)
{
  Collection *c = static_cast<Collection*>(c_.get());
  dout(10) << __func__ << " " << c->cid << " " << oid << dendl;
  if (!c->exists)
    return -ENOENT;
  PetObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  st->st_size = o->get_size();
  st->st_blksize = 4096;
  st->st_blocks = (st->st_size + st->st_blksize - 1) / st->st_blksize;
  st->st_nlink = 1;
  return 0;
}

int PetStore::set_collection_opts(
  const coll_t& cid,
  const pool_opts_t& opts)
{
  return -EOPNOTSUPP;
}

int PetStore::read(
    const coll_t& cid,
    const ghobject_t& oid,
    uint64_t offset,
    size_t len,
    bufferlist& bl,
    uint32_t op_flags)
{
  CollectionHandle c = get_collection(cid);
  if (!c)
    return -ENOENT;
  return read(c, oid, offset, len, bl, op_flags);
}

int PetStore::read(
  CollectionHandle &c_,
  const ghobject_t& oid,
  uint64_t offset,
  size_t len,
  bufferlist& bl,
  uint32_t op_flags)
{
  Collection *c = static_cast<Collection*>(c_.get());
  dout(10) << __func__ << " " << c->cid << " " << oid << " "
	   << offset << "~" << len << dendl;
  if (!c->exists)
    return -ENOENT;
  PetObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  if (offset >= o->get_size())
    return 0;
  size_t l = len;
  if (l == 0 && offset == 0)  // note: len == 0 means read the entire object
    l = o->get_size();
  else if (offset + l > o->get_size())
    l = o->get_size() - offset;
  bl.clear();
  return o->read(offset, l, bl);
}

int PetStore::fiemap(const coll_t& cid, const ghobject_t& oid,
		     uint64_t offset, size_t len, bufferlist& bl)
{
  map<uint64_t, uint64_t> destmap;
  int r = fiemap(cid, oid, offset, len, destmap);
  if (r >= 0)
    ::encode(destmap, bl);
  return r;
}

int PetStore::fiemap(const coll_t& cid, const ghobject_t& oid,
		     uint64_t offset, size_t len, map<uint64_t, uint64_t>& destmap)
{
  dout(10) << __func__ << " " << cid << " " << oid << " " << offset << "~"
	   << len << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;

  PetObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  size_t l = len;
  if (offset + l > o->get_size())
    l = o->get_size() - offset;
  if (offset >= o->get_size())
    goto out;
  destmap[offset] = l;
 out:
  return 0;
}

int PetStore::getattr(const coll_t& cid, const ghobject_t& oid,
		      const char *name, bufferptr& value)
{
  CollectionHandle c = get_collection(cid);
  if (!c)
    return -ENOENT;
  return getattr(c, oid, name, value);
}

int PetStore::getattr(CollectionHandle &c_, const ghobject_t& oid,
		      const char *name, bufferptr& value)
{
  Collection *c = static_cast<Collection*>(c_.get());
  dout(10) << __func__ << " " << c->cid << " " << oid << " " << name << dendl;
  if (!c->exists)
    return -ENOENT;
  PetObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  string k(name);
  std::lock_guard<std::mutex> lock(o->xattr_mutex);
  if (!o->xattr.count(k)) {
    return -ENODATA;
  }
  value = o->xattr[k];
  return 0;
}

int PetStore::getattrs(const coll_t& cid, const ghobject_t& oid,
		       map<string,bufferptr>& aset)
{
  CollectionHandle c = get_collection(cid);
  if (!c)
    return -ENOENT;
  return getattrs(c, oid, aset);
}

int PetStore::getattrs(CollectionHandle &c_, const ghobject_t& oid,
		       map<string,bufferptr>& aset)
{
  Collection *c = static_cast<Collection*>(c_.get());
  dout(10) << __func__ << " " << c->cid << " " << oid << dendl;
  if (!c->exists)
    return -ENOENT;

  PetObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  std::lock_guard<std::mutex> lock(o->xattr_mutex);
  aset = o->xattr;
  return 0;
}

int PetStore::list_collections(vector<coll_t>& ls)
{
  dout(10) << __func__ << dendl;
  RWLock::RLocker l(coll_lock);
  for (ceph::unordered_map<coll_t,CollectionRef>::iterator p = coll_map.begin();
       p != coll_map.end();
       ++p) {
    ls.push_back(p->first);
  }
  return 0;
}

bool PetStore::collection_exists(const coll_t& cid)
{
  dout(10) << __func__ << " " << cid << dendl;
  RWLock::RLocker l(coll_lock);
  return coll_map.count(cid);
}

int PetStore::collection_empty(const coll_t& cid, bool *empty)
{
  dout(10) << __func__ << " " << cid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::RLocker l(c->lock);
  *empty = c->object_map.empty();
  return 0;
}

int PetStore::collection_bits(const coll_t& cid)
{
  dout(10) << __func__ << " " << cid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::RLocker l(c->lock);
  return c->bits;
}

int PetStore::collection_list(const coll_t& cid,
			      const ghobject_t& start,
			      const ghobject_t& end,
			      int max,
			      vector<ghobject_t> *ls, ghobject_t *next)
{
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::RLocker l(c->lock);

  dout(10) << __func__ << " cid " << cid << " start " << start
	   << " end " << end << dendl;
  map<ghobject_t,PetObjectRef>::iterator p = c->object_map.lower_bound(start);
  while (p != c->object_map.end() &&
	 ls->size() < (unsigned)max &&
	 p->first < end) {
    ls->push_back(p->first);
    ++p;
  }
  if (next != NULL) {
    if (p == c->object_map.end())
      *next = ghobject_t::get_max();
    else
      *next = p->first;
  }
  dout(10) << __func__ << " cid " << cid << " got " << ls->size() << dendl;
  return 0;
}

int PetStore::omap_get(
    const coll_t& cid,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    bufferlist *header,      ///< [out] omap header
    map<string, bufferlist> *out /// < [out] Key to value map
    )
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;

  PetObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  std::lock_guard<std::mutex> lock(o->omap_mutex);
  *header = o->omap_header;
  *out = o->omap;
  return 0;
}

int PetStore::omap_get_header(
    const coll_t& cid,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    bufferlist *header,      ///< [out] omap header
    bool allow_eio ///< [in] don't assert on eio
    )
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;

  PetObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  std::lock_guard<std::mutex> lock(o->omap_mutex);
  *header = o->omap_header;
  return 0;
}

int PetStore::omap_get_keys(
    const coll_t& cid,              ///< [in] Collection containing oid
    const ghobject_t &oid, ///< [in] Object containing omap
    set<string> *keys      ///< [out] Keys defined on oid
    )
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;

  PetObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  std::lock_guard<std::mutex> lock(o->omap_mutex);
  for (map<string,bufferlist>::iterator p = o->omap.begin();
       p != o->omap.end();
       ++p)
    keys->insert(p->first);
  return 0;
}

int PetStore::omap_get_values(
    const coll_t& cid,                    ///< [in] Collection containing oid
    const ghobject_t &oid,       ///< [in] Object containing omap
    const set<string> &keys,     ///< [in] Keys to get
    map<string, bufferlist> *out ///< [out] Returned keys and values
    )
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;

  PetObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  std::lock_guard<std::mutex> lock(o->omap_mutex);
  for (set<string>::const_iterator p = keys.begin();
       p != keys.end();
       ++p) {
    map<string,bufferlist>::iterator q = o->omap.find(*p);
    if (q != o->omap.end())
      out->insert(*q);
  }
  return 0;
}

int PetStore::omap_check_keys(
    const coll_t& cid,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    const set<string> &keys, ///< [in] Keys to check
    set<string> *out         ///< [out] Subset of keys defined on oid
    )
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;

  PetObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  std::lock_guard<std::mutex> lock(o->omap_mutex);
  for (set<string>::const_iterator p = keys.begin();
       p != keys.end();
       ++p) {
    map<string,bufferlist>::iterator q = o->omap.find(*p);
    if (q != o->omap.end())
      out->insert(*p);
  }
  return 0;
}

class PetStore::OmapIteratorImpl : public ObjectMap::ObjectMapIteratorImpl {
  CollectionRef c;
  PetObjectRef o;
  map<string,bufferlist>::iterator it;
public:
  OmapIteratorImpl(CollectionRef c, PetObjectRef o)
    : c(c), o(o), it(o->omap.begin()) {}

  int seek_to_first() override {
    std::lock_guard<std::mutex>(o->omap_mutex);
    it = o->omap.begin();
    return 0;
  }
  int upper_bound(const string &after) override {
    std::lock_guard<std::mutex>(o->omap_mutex);
    it = o->omap.upper_bound(after);
    return 0;
  }
  int lower_bound(const string &to) override {
    std::lock_guard<std::mutex>(o->omap_mutex);
    it = o->omap.lower_bound(to);
    return 0;
  }
  bool valid() override {
    std::lock_guard<std::mutex>(o->omap_mutex);
    return it != o->omap.end();
  }
  int next(bool validate=true) override {
    std::lock_guard<std::mutex>(o->omap_mutex);
    ++it;
    return 0;
  }
  string key() override {
    std::lock_guard<std::mutex>(o->omap_mutex);
    return it->first;
  }
  bufferlist value() override {
    std::lock_guard<std::mutex>(o->omap_mutex);
    return it->second;
  }
  int status() override {
    return 0;
  }
};

ObjectMap::ObjectMapIterator PetStore::get_omap_iterator(const coll_t& cid,
							 const ghobject_t& oid)
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return ObjectMap::ObjectMapIterator();

  PetObjectRef o = c->get_object(oid);
  if (!o)
    return ObjectMap::ObjectMapIterator();
  return ObjectMap::ObjectMapIterator(new OmapIteratorImpl(c, o));
}


// ---------------
// write operations

int PetStore::queue_transactions(Sequencer *osr,
				 vector<Transaction>& tls,
				 TrackedOpRef op,
				 ThreadPool::TPHandle *handle)
{
  // because petstore operations are synchronous, we can implement the
  // Sequencer with a mutex. this guarantees ordering on a given sequencer,
  // while allowing operations on different sequencers to happen in parallel
  struct OpSequencer : public Sequencer_impl {
    OpSequencer(CephContext* cct) :
      Sequencer_impl(cct) {}
    std::mutex mutex;
    void flush() override {}
    bool flush_commit(Context*) override { return true; }
  };

  std::unique_lock<std::mutex> lock;
  if (osr) {
    if (!osr->p) {
      osr->p = new OpSequencer(cct);
    }
    auto seq = static_cast<OpSequencer*>(osr->p.get());
    lock = std::unique_lock<std::mutex>(seq->mutex);
  }

  for (vector<Transaction>::iterator p = tls.begin(); p != tls.end(); ++p) {
    // poke the TPHandle heartbeat just to exercise that code path
    if (handle)
      handle->reset_tp_timeout();

    _do_transaction(*p);
  }

  Context *on_apply = NULL, *on_apply_sync = NULL, *on_commit = NULL;
  ObjectStore::Transaction::collect_contexts(tls, &on_apply, &on_commit,
					     &on_apply_sync);
  if (on_apply_sync)
    on_apply_sync->complete(0);
  if (on_apply)
    finisher.queue(on_apply);
  if (on_commit)
    finisher.queue(on_commit);
  return 0;
}

// ---------------
// private methods

int PetStore::_open_path()
{
  assert(path_fd < 0);
  path_fd = ::open(path.c_str(), O_DIRECTORY);
  if (path_fd < 0) {
    int r = -errno;
    derr << __func__ << " unable to open " << path << ": " << cpp_strerror(r)
         << dendl;
    return r;
  }
  assert(fs == NULL);
  fs = FS::create(path_fd);
  dout(1) << __func__ << " using fs driver '" << fs->get_name() << "'" << dendl;
  return 0;
}

void PetStore::_close_path()
{
  VOID_TEMP_FAILURE_RETRY(::close(path_fd));
  path_fd = -1;
  delete fs;
  fs = NULL;
}

int PetStore::_open_objects_dir()
{
  assert(objects_fd < 0);
  objects_fd = ::openat(path_fd, "objects", O_DIRECTORY);
  if (objects_fd < 0) {
    int r = -errno;
    derr << __func__ << " cannot open " << path << "/objects: "
         << cpp_strerror(r) << dendl;
    return r;
  }
  return 0;
}

int PetStore::_create_objects_dir()
{
  assert(objects_fd < 0);
  objects_fd = ::openat(path_fd, "objects", O_DIRECTORY);
  if (objects_fd < 0 && errno == ENOENT) {
    int r = ::mkdirat(path_fd, "objects", 0755);
    if (r < 0) {
      r = -errno;
      derr << __func__ << " cannot create " << path << "/objects: "
           << cpp_strerror(r) << dendl;
      return r;
    }
    objects_fd = ::openat(path_fd, "objects", O_DIRECTORY);
  }
  if (objects_fd < 0) {
    int r = -errno;
    derr << __func__ << " cannot open created " << path << "/objects: "
         << cpp_strerror(r) << dendl;
    return r;
  }
  return 0;
}

void PetStore::_close_objects_dir()
{
  if (oset_fd >= 0) {
    VOID_TEMP_FAILURE_RETRY(::close(oset_fd));
    oset_fd = -1;
  }
  VOID_TEMP_FAILURE_RETRY(::close(objects_fd));
  objects_fd = -1;
}


int PetStore::_open_fsid(bool create)
{
  assert(fsid_fd < 0);
  int flags = O_RDWR;
  if (create)
    flags |= O_CREAT;
  fsid_fd = ::openat(path_fd, "fsid", flags, 0644);
  if (fsid_fd < 0) {
    int err = -errno;
    derr << __func__ << " " << cpp_strerror(err) << dendl;
    return err;
  }
  return 0;
}

int PetStore::_read_fsid(uuid_d *uuid)
{
  char fsid_str[40];
  int ret = safe_read(fsid_fd, fsid_str, sizeof(fsid_str));
  if (ret < 0)
    return ret;
  if (ret > 36)
    fsid_str[36] = 0;
  if (!uuid->parse(fsid_str))
    return -EINVAL;
  return 0;
}

int PetStore::_write_fsid()
{
  int r = ::ftruncate(fsid_fd, 0);
  if (r < 0) {
    r = -errno;
    derr << __func__ << " fsid truncate failed: " << cpp_strerror(r) << dendl;
    return r;
  }
  string str = stringify(fsid) + "\n";
  r = safe_write(fsid_fd, str.c_str(), str.length());
  if (r < 0) {
    derr << __func__ << " fsid write failed: " << cpp_strerror(r) << dendl;
    return r;
  }
  r = ::fsync(fsid_fd);
  if (r < 0) {
    derr << __func__ << " fsid fsync failed: " << cpp_strerror(r) << dendl;
    return r;
  }
  return 0;
}

void PetStore::_close_fsid()
{
//  VOID_TEMP_FAILURE_RETRY(::close(fsid_fd));
  fsid_fd = -1;
}

int PetStore::_lock_fsid()
{
  struct flock l;
  memset(&l, 0, sizeof(l));
  l.l_type = F_WRLCK;
  l.l_whence = SEEK_SET;
  l.l_start = 0;
  l.l_len = 0;
  int r = ::fcntl(fsid_fd, F_SETLK, &l);
  if (r < 0) {
    int err = errno;
    derr << __func__ << " failed to lock " << path << "/fsid"
         << " (is another ceph-osd still running?)"
         << cpp_strerror(err) << dendl;
    return -err;
  }
  return 0;
}

bool PetStore::test_mount_in_use()
{
  // most error conditions mean the mount is not in use (e.g., because
  // it doesn't exist).  only if we fail to lock do we conclude it is
  // in use.
  bool ret = false;
  int r = _open_path();
  if (r < 0)
    return false;
/*
  r = _open_fsid(false);
  if (r < 0)
    goto out_path;
  r = _lock_fsid();
  if (r < 0)
    ret = true; // if we can't lock, it is in used
  _close_fsid();
*/
 out_path:
  _close_path();
  return ret;
}

int PetStore::_open_db(bool create)
{
  assert(!db);
  char fn[PATH_MAX];
  snprintf(fn, sizeof(fn), "%s/db", path.c_str());
  db = KeyValueDB::create(g_ceph_context,
                          cct->_conf->get_val<std::string>("newstore_backend"),
                          fn);
  if (!db) {
    derr << __func__ << " error creating db" << dendl;
    delete db;
    db = NULL;
    return -EIO;
  }
  string options;
  if (cct->_conf->get_val<std::string>("newstore_backend") == "rocksdb")
    options = cct->_conf->get_val<std::string>("newstore_rocksdb_options");
  db->init(options);
  stringstream err;
  int r;
  if (create)
    r = db->create_and_open(err);
  else
    r = db->open(err);
  if (r) {
    derr << __func__ << " erroring opening db: " << err.str() << dendl;
    delete db;
    db = NULL;
    return -EIO;
  }
  dout(1) << __func__ << " opened " << cct->_conf->get_val<std::string>("newstore_backend")
          << " path " << path << " options " << options << dendl;
  return 0;
}

void PetStore::_close_db()
{
  assert(db);
  delete db;
  db = NULL;
}

void PetStore::_do_transaction(Transaction& t)
{
  typedef Transaction T;

  T::iterator i = t.begin();
  int pos = 0;

  while (i.have_op()) {
    Transaction::Op *op = i.decode_op();
    dout(10) << __func__ << " op_type: " << op->op << dendl;

    int r = 0;
    switch (op->op) {
    case T::OP_NOP: break; // Do Nothing
    case T::OP_TOUCH: r = _op_touch(i, op); break;
    case T::OP_WRITE: r = _op_write(i, op); break;
    case T::OP_ZERO: r = _op_zero(i, op); break;
    case T::OP_TRIMCACHE: break; // Unimplemented
    case T::OP_TRUNCATE: r = _op_truncate(i, op); break;
    case T::OP_REMOVE: r = _op_remove(i, op); break;
    case T::OP_SETATTR: r = _op_setattr(i, op); break;
    case T::OP_SETATTRS: r = _op_setattrs(i, op); break;
    case T::OP_RMATTRS: r = _op_rmattrs(i, op); break;
    case T::OP_CLONE: r = _op_clone(i, op); break;
    case T::OP_CLONERANGE: r = _op_clonerange(i, op); break;
    case T::OP_CLONERANGE2: r = _op_clonerange2(i, op); break;
    case T::OP_MKCOLL: r = _op_mkcoll(i, op); break;
    case T::OP_COLL_HINT: r = _op_coll_hint(i, op); break;
    case T::OP_RMCOLL: r = _op_rmcoll(i, op); break;
    case T::OP_COLL_ADD: r = _op_coll_add(i ,op); break;
    case T::OP_COLL_REMOVE: r = _op_coll_remove(i, op); break;
    case T::OP_COLL_MOVE: assert(0 == "deprecated"); break;
    case T::OP_COLL_MOVE_RENAME: r = _op_coll_move_rename(i ,op); break;
    case T::OP_TRY_RENAME: r = _op_try_rename(i, op); break;
    case T::OP_COLL_SETATTR: assert(0 == "not implemented"); break;
    case T::OP_COLL_RMATTR: assert(0 == "not implemented"); break;
    case T::OP_COLL_RENAME: assert(0 == "not implemented"); break;
    case T::OP_OMAP_CLEAR: r = _op_omap_clear(i, op); break;
    case T::OP_OMAP_SETKEYS: r = _op_omap_setkeys(i, op); break;
    case T::OP_OMAP_RMKEYS: r = _op_omap_rmkeys(i, op); break;
    case T::OP_OMAP_RMKEYRANGE: r = _op_omap_rmkeyrange(i, op); break;
    case T::OP_OMAP_SETHEADER: r = _op_omap_setheader(i, op); break;
    case T::OP_SPLIT_COLLECTION: assert(0 == "deprecated"); break;
    case T::OP_SPLIT_COLLECTION2: r = _op_split_collection2(i, op); break;
    case T::OP_SETALLOCHINT: break; // Unimplemented
    default:
      derr << "bad op " << op->op << dendl;
      ceph_abort();
    }

    if (r < 0) {
      bool ok = false;

      if (r == -ENOENT && !(op->op == T::OP_CLONERANGE ||
			    op->op == T::OP_CLONE ||
			    op->op == T::OP_CLONERANGE2 ||
			    op->op == T::OP_COLL_ADD))
	// -ENOENT is usually okay
	ok = true;
      if (r == -ENODATA)
	ok = true;

      if (!ok) {
	const char *msg = "unexpected error code";

	if (r == -ENOENT && (op->op == T::OP_CLONERANGE ||
			     op->op == T::OP_CLONE ||
			     op->op == T::OP_CLONERANGE2))
	  msg = "ENOENT on clone suggests osd bug";

	if (r == -ENOSPC)
	  // For now, if we hit _any_ ENOSPC, crash, before we do any damage
	  // by partially applying transactions.
	  msg = "ENOSPC from PetStore, misconfigured cluster or insufficient memory";

	if (r == -ENOTEMPTY) {
	  msg = "ENOTEMPTY suggests garbage data in osd data dir";
	  dump_all();
	}

	derr    << " error " << cpp_strerror(r) << " not handled on operation " << op->op
		<< " (op " << pos << ", counting from 0)" << dendl;
	dout(0) << msg << dendl;
	dout(0) << " transaction dump:\n";
	JSONFormatter f(true);
	f.open_object_section("transaction");
	t.dump(&f);
	f.close_section();
	f.flush(*_dout);
	*_dout << dendl;
	assert(0 == "unexpected error");
      }
    }

    ++pos;
  }
}

int PetStore::_op_touch(Transaction::iterator& i, Transaction::Op *op)
{
  CollectionRef c = get_collection(i.get_cid(op->cid));
  if (!c) return -ENOENT;
  c->get_or_create_object(i.get_oid(op->oid));
  return 0;
}

int PetStore::_op_write(Transaction::iterator& i, Transaction::Op *op)
{ 
  CollectionRef c = get_collection(i.get_cid(op->cid));
  if (!c) return -ENOENT;
  PetObjectRef o = c->get_or_create_object(i.get_oid(op->oid));

  bufferlist bl;
  i.decode_bl(bl);

  dout(10) << "OP_WRITE" << " " << c->get_cid() << " " << o->get_oid() << " "
             << op->off << "~" << op->len << " " << bl.length() << dendl;
  dout(30) << "dump:";
  bl.hexdump(*_dout);
  *_dout << dendl;

  assert(op->len == bl.length());

  if (op->len > 0) {
    const ssize_t old_size = o->get_size();
    o->write(op->off, bl);
    used_bytes += (o->get_size() - old_size);
  }
  return 0;
}

int PetStore::_op_zero(Transaction::iterator& i, Transaction::Op *op)
{ 
  CollectionRef c = get_collection(i.get_cid(op->cid));
  if (!c) return -ENOENT;
  PetObjectRef o = c->get_or_create_object(i.get_oid(op->oid));

  bufferlist bl;
  bl.append_zero(op->len);
  i.decode_bl(bl);
  assert(op->len == bl.length());

  if (op->len > 0) {
    const ssize_t old_size = o->get_size();
    o->write(op->off, bl);
    used_bytes += (o->get_size() - old_size);
  }

  return 0;
}

int PetStore::_op_truncate(Transaction::iterator& i, Transaction::Op *op)
{ 
  CollectionRef c = get_collection(i.get_cid(op->cid));
  if (!c) return -ENOENT;
  PetObjectRef o = c->get_object(i.get_oid(op->oid));
  if (!o) return -ENOENT;

  const ssize_t old_size = o->get_size();
  int r = o->truncate(op->off);
  used_bytes += (o->get_size() - old_size);
  return r;
}

int PetStore::_op_remove(Transaction::iterator& i, Transaction::Op *op)
{
  CollectionRef c = get_collection(i.get_cid(op->cid));
  if (!c) return -ENOENT;

  RWLock::WLocker l(c->lock);

  auto o = c->object_hash.find(i.get_oid(op->oid));
  if (o == c->object_hash.end())
    return -ENOENT;

  used_bytes -= o->second->get_size();
  c->object_hash.erase(o);
  c->object_map.erase(i.get_oid(op->oid));
  return 0;
}

int PetStore::_op_setattr(Transaction::iterator& i, Transaction::Op *op)
{
  CollectionRef c = get_collection(i.get_cid(op->cid));
  if (!c) return -ENOENT;
  PetObjectRef o = c->get_object(i.get_oid(op->oid));
  if (!o) return -ENOENT;

  bufferlist bl;
  i.decode_bl(bl);
  map<string, bufferptr> aset;
  aset[i.decode_string()] = bufferptr(bl.c_str(), bl.length());
  return o->setattrs(aset);
}

int PetStore::_op_setattrs(Transaction::iterator& i, Transaction::Op *op)
{
  CollectionRef c = get_collection(i.get_cid(op->cid));
  if (!c) return -ENOENT;
  PetObjectRef o = c->get_object(i.get_oid(op->oid));
  if (!o) return -ENOENT;

  map<string, bufferptr> aset;
  i.decode_attrset(aset);
  return o->setattrs(aset);
}

int PetStore::_op_rmattr(Transaction::iterator& i, Transaction::Op *op)
{
  CollectionRef c = get_collection(i.get_cid(op->cid));
  if (!c) return -ENOENT;
  PetObjectRef o = c->get_object(i.get_oid(op->oid));
  if (!o) return -ENOENT;

  return o->rmattr(i.decode_string().c_str());
}

int PetStore::_op_rmattrs(Transaction::iterator& i, Transaction::Op *op)
{
  CollectionRef c = get_collection(i.get_cid(op->cid));
  if (!c) return -ENOENT;
  PetObjectRef o = c->get_object(i.get_oid(op->oid));
  if (!o) return -ENOENT;

  return o->rmattrs();
}

//TODO: Nicer locking?
int PetStore::_op_clone(Transaction::iterator& i, Transaction::Op *op)
{
  CollectionRef c = get_collection(i.get_cid(op->cid));
  if (!c) return -ENOENT;
  PetObjectRef oo = c->get_object(i.get_oid(op->oid));
  if (!oo) return -ENOENT;
  PetObjectRef no = c->get_or_create_object(i.get_oid(op->dest_oid));

  used_bytes += oo->get_size() - no->get_size();
  no->clone(oo.get(), 0, oo->get_size(), 0);

  // take xattr and omap locks with std::lock()
  std::unique_lock<std::mutex>
      ox_lock(oo->xattr_mutex, std::defer_lock),
      nx_lock(no->xattr_mutex, std::defer_lock),
      oo_lock(oo->omap_mutex, std::defer_lock),
      no_lock(no->omap_mutex, std::defer_lock);
  std::lock(ox_lock, nx_lock, oo_lock, no_lock);

  no->omap_header = oo->omap_header;
  no->omap = oo->omap;
  no->xattr = oo->xattr;
  return 0;
}

int PetStore::_op_clonerange(Transaction::iterator& i, Transaction::Op *op)
{
  CollectionRef c = get_collection(i.get_cid(op->cid));
  if (!c) return -ENOENT;
  PetObjectRef oo = c->get_object(i.get_oid(op->oid));
  if (!oo) return -ENOENT;
  PetObjectRef no = c->get_or_create_object(i.get_oid(op->dest_oid));

  uint64_t len = op->len;
  if(op->off >= oo->get_size())
    return 0;
  if(op->off + len >= oo->get_size())
    len = oo->get_size() - op->off;

  const ssize_t old_size = no->get_size();
  no->clone(oo.get(), op->off, len, op->off);
  used_bytes += (no->get_size() - old_size);

  return 0;
}

int PetStore::_op_clonerange2(Transaction::iterator& i, Transaction::Op *op)
{
  CollectionRef c = get_collection(i.get_cid(op->cid));
  if (!c) return -ENOENT;
  PetObjectRef oo = c->get_object(i.get_oid(op->oid));
  if (!oo) return -ENOENT;
  PetObjectRef no = c->get_or_create_object(i.get_oid(op->dest_oid));

  uint64_t len = op->len;
  if(op->off >= oo->get_size())
    return 0;
  if(op->off + len >= oo->get_size())
    len = oo->get_size() - op->off;

  const ssize_t old_size = no->get_size();
  no->clone(oo.get(), op->off, len, op->dest_off);
  used_bytes += (no->get_size() - old_size);

  return 0;
}

int PetStore::_op_mkcoll(Transaction::iterator& i, Transaction::Op *op)
{
  coll_t cid = i.get_cid(op->cid);

  RWLock::WLocker l(coll_lock);
  auto result = coll_map.insert(std::make_pair(cid, CollectionRef()));
  if (!result.second)
    return -EEXIST;
  result.first->second.reset(new Collection(cct, cid));
  result.first->second->bits = op->split_bits;
  return 0;
}

int PetStore::_op_rmcoll(Transaction::iterator& i, Transaction::Op *op)
{
  coll_t cid = i.get_cid(op->cid);
  dout(10) << __func__ << " " << cid << dendl;

  RWLock::WLocker l(coll_lock);
  ceph::unordered_map<coll_t,CollectionRef>::iterator cp = coll_map.find(cid);
  if (cp == coll_map.end()) return -ENOENT;

  {
    RWLock::RLocker l2(cp->second->lock);
    if (!cp->second->object_map.empty()) return -ENOTEMPTY;
    cp->second->exists = false;
  }

  used_bytes -= cp->second->used_bytes();
  coll_map.erase(cp);
  return 0;

}

int PetStore::_op_coll_hint(Transaction::iterator& i, Transaction::Op *op)
{
  coll_t cid = i.get_cid(op->cid);

  bufferlist hint;
  i.decode_bl(hint);
  bufferlist::iterator hiter = hint.begin();

  if (op->hint_type == Transaction::COLL_HINT_EXPECTED_NUM_OBJECTS) {
    uint32_t pg_num;
    uint64_t num_objs;
    ::decode(pg_num, hiter);
    ::decode(num_objs, hiter);

    // Not actually doing anything yet.
    return 0;
  }
  // Ignore the hint
  dout(10) << "Unrecognized collection hint type: " << op->hint_type << dendl;
  return 0;
}

int PetStore::_op_coll_add(Transaction::iterator& i, Transaction::Op *op)
{
  CollectionRef c = get_collection(i.get_cid(op->cid));
  if (!c) return -ENOENT;
  CollectionRef oc = get_collection(i.get_cid(op->dest_cid));
  if (!oc) return -ENOENT;

  ghobject_t oid = i.get_oid(op->oid);

  RWLock::WLocker l1(std::min(&(*c), &(*oc))->lock);
  RWLock::WLocker l2(MAX(&(*c), &(*oc))->lock);

  if (c->object_hash.count(oid)) return -EEXIST;
  if (oc->object_hash.count(oid) == 0) return -ENOENT;

  PetObjectRef o = oc->object_hash[oid];
  c->object_map[oid] = o;
  c->object_hash[oid] = o;

  return 0;
}

int PetStore::_op_coll_remove(Transaction::iterator& i, Transaction::Op *op)
{
  CollectionRef c = get_collection(i.get_cid(op->cid));
  if (!c) return -ENOENT;

  ghobject_t oid = i.get_oid(op->oid);

  RWLock::WLocker l(c->lock);

  auto o = c->object_hash.find(oid);
  if (o == c->object_hash.end()) return -ENOENT;

  used_bytes -= o->second->get_size();
  c->object_hash.erase(o);
  c->object_map.erase(oid);

  return 0;
}

int PetStore::_op_coll_move_rename(Transaction::iterator& i, Transaction::Op *op)
{
  CollectionRef c = get_collection(i.get_cid(op->cid));
  if (!c) return -ENOENT;
  CollectionRef oc = get_collection(i.get_cid(op->dest_cid));
  if (!oc) return -ENOENT;

  // note: c and oc may be the same
  assert(&(*c) == &(*oc));

  c->lock.get_write();

  const ghobject_t& oid = i.get_oid(op->oid);
  if (oc->object_hash.count(oid) == 0) {
    c->lock.put_write();
    return -ENOENT;
  }

  const ghobject_t dest_oid = i.get_oid(op->dest_oid);
  if (c->object_hash.count(dest_oid)) {
    c->lock.put_write();
    return -EEXIST;
  }

  {
    PetObjectRef o = oc->object_hash[oid];
    c->object_map[dest_oid] = o;
    c->object_hash[dest_oid] = o;
    oc->object_map.erase(oid);
    oc->object_hash.erase(oid);
  }
  c->lock.put_write();
  return 0;
}

int PetStore::_op_try_rename(Transaction::iterator& i, Transaction::Op *op)
{ 
  CollectionRef c = get_collection(i.get_cid(op->cid));
  if (!c) return -ENOENT;
  CollectionRef oc = get_collection(i.get_cid(op->cid));
  if (!oc) return -ENOENT;

  // note: c and oc may be the same
  assert(&(*c) == &(*oc));

  c->lock.get_write();

  const ghobject_t& oid = i.get_oid(op->oid);
  if (oc->object_hash.count(oid) == 0) {
    c->lock.put_write();
    return -ENOENT;
  }

  const ghobject_t dest_oid = i.get_oid(op->dest_oid);
  if (c->object_hash.count(dest_oid)) {
    c->lock.put_write();
    return -EEXIST;
  }

  {
    PetObjectRef o = oc->object_hash[oid];
    c->object_map[dest_oid] = o;
    c->object_hash[dest_oid] = o;
    oc->object_map.erase(oid);
    oc->object_hash.erase(oid);
  }
  c->lock.put_write();
  return 0;
}

int PetStore::_op_omap_clear(Transaction::iterator& i, Transaction::Op *op)
{ 
  CollectionRef c = get_collection(i.get_cid(op->cid));
  if (!c) return -ENOENT;
  PetObjectRef o = c->get_object(i.get_oid(op->oid));
  if (!o) return -ENOENT;

  return o->omap_clear();
}

int PetStore::_op_omap_setkeys(Transaction::iterator& i, Transaction::Op *op)
{ 
  CollectionRef c = get_collection(i.get_cid(op->cid));
  if (!c) return -ENOENT;
  PetObjectRef o = c->get_object(i.get_oid(op->oid));
  if (!o) return -ENOENT;

  bufferlist bl;
  i.decode_attrset_bl(&bl);
  return o->omap_setkeys(bl);
}

int PetStore::_op_omap_rmkeys(Transaction::iterator& i, Transaction::Op *op)
{ 
  CollectionRef c = get_collection(i.get_cid(op->cid));
  if (!c) return -ENOENT;
  PetObjectRef o = c->get_object(i.get_oid(op->oid));
  if (!o) return -ENOENT;

  bufferlist bl;
  i.decode_keyset_bl(&bl);
  return o->omap_rmkeys(bl);
}

int PetStore::_op_omap_rmkeyrange(Transaction::iterator& i, Transaction::Op *op)
{
  CollectionRef c = get_collection(i.get_cid(op->cid));
  if (!c) return -ENOENT;
  PetObjectRef o = c->get_object(i.get_oid(op->oid));
  if (!o) return -ENOENT;

  string first, last;
  first = i.decode_string();
  last = i.decode_string();
  return o->omap_rmkeyrange(first, last);
}

int PetStore::_op_omap_setheader(Transaction::iterator& i, Transaction::Op *op)
{
  CollectionRef c = get_collection(i.get_cid(op->cid));
  if (!c) return -ENOENT;
  PetObjectRef o = c->get_object(i.get_oid(op->oid));
  if (!o) return -ENOENT;

  bufferlist bl;
  i.decode_bl(bl);
  return o->omap_setheader(bl);
}

int PetStore::_op_split_collection2(Transaction::iterator& i, Transaction::Op *op)
{
  CollectionRef c = get_collection(i.get_cid(op->cid));
  if (!c) return -ENOENT;
  CollectionRef oc = get_collection(i.get_cid(op->dest_cid));
  if (!oc) return -ENOENT;

  RWLock::WLocker l1(std::min(&(*c), &(*oc))->lock);
  RWLock::WLocker l2(MAX(&(*c), &(*oc))->lock);

  map<ghobject_t,PetObjectRef>::iterator p = c->object_map.begin();
  while (p != c->object_map.end()) {
    if (p->first.match(op->split_bits, op->split_rem)) {
      dout(20) << " moving " << p->first << dendl;
      oc->object_map.insert(make_pair(p->first, p->second));
      oc->object_hash.insert(make_pair(p->first, p->second));
      c->object_hash.erase(p->first);
      c->object_map.erase(p++);
    } else {
      ++p;
    }
  }

  c->bits = op->split_bits;
  assert(oc->bits == (int)op->split_bits);

  return 0;
}

PetObject::Ref PetStore::Collection::create_object(const ghobject_t& oid) const {
  return new VectorObject(cct, oid);
}
