// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "NewStore.h"
#include "include/compat.h"
#include "include/stringify.h"
#include "common/errno.h"
#include "common/safe_io.h"

#define dout_context cct 
#define dout_subsys ceph_subsys_newstore

/*

  TODO:

  * alloc hint to set frag size
  * collection_list must flush pending db work
  * rocksdb: use db_paths (db/ and db.bulk/ ?)
  * rocksdb: auto-detect use_fsync option when not xfs or btrfs
  * avoid mtime updates when doing open-by-handle
  * fid xattr backpointer
  * inline first fsync_item in TransContext to void allocation?
  * refcounted fragments (for efficient clone)

 */

/*
 * Some invariants:
 *
 * - The fragment extent referenced by the fragment_t is always
 *   defined.  It may be zeros (a hole in the underlying fs).
 *
 * - The fragment file may be larger than the fragment_t indicates.
 *   If so, the trailing cruft should be ignored and can be safely
 *   discarded (see _clean_fid_tail).
 *
 * - The fragment file may be smaller than the fragment_t indicates.
 *   The content is defined to be zeros in this case.
 *
 * - All fragments start on a frag_size boundary, and are
 *   at most frag_size bytes.
 *
 * - A fragment *may* be shorter than frag_size, even if
 *   it isn't at the end of a file.
 *
 * - The fragment map never extends beyond the size of the object.
 *
 * - The fragment_t::offset is (currently) always 0.
 *
 */

const string PREFIX_SUPER = "S"; // field -> value
const string PREFIX_COLL = "C"; // collection name -> (nothing)
const string PREFIX_OBJ = "O";  // object name -> onode
const string PREFIX_OVERLAY = "V"; // u64 + offset -> value
const string PREFIX_OMAP = "M"; // u64 + keyname -> value
const string PREFIX_WAL = "L";  // write ahead log

/*
 * object name key structure
 *
 * 2 chars: shard (-- for none, or hex digit, so that we sort properly)
 * encoded u64: poolid + 2^63 (so that it sorts properly)
 * encoded u32: hash (bit reversed)
 *
 * 1 char: '.'
 *
 * escaped string: namespace
 *
 * 1 char: '<', '=', or '>'.  if =, then object key == object name, and
 *         we are followed just by the key.  otherwise, we are followed by
 *         the key and then the object name.
 * escaped string: key
 * escaped string: object name (unless '=' above)
 *
 * encoded u64: snap
 * encoded u64: generation
 */

/*
 * string encoding in the key
 *
 * The key string needs to lexicographically sort the same way that
 * ghobject_t does.  We do this by escaping anything <= to '%' with %
 * plus a 2 digit hex string, and anything >= '~' with ~ plus the two
 * hex digits.
 *
 * We use ! as a terminator for strings; this works because it is < %
 * and will get escaped if it is present in the string.
 *
 */

static void append_escaped(const string &in, string *out)
{
  char hexbyte[8];
  for (string::const_iterator i = in.begin(); i != in.end(); ++i) {
    if (*i <= '#') {
      snprintf(hexbyte, sizeof(hexbyte), "#%02x", (unsigned)*i);
      out->append(hexbyte);
    } else if (*i >= '~') {
      snprintf(hexbyte, sizeof(hexbyte), "~%02x", (unsigned)*i);
      out->append(hexbyte);
    } else {
      out->push_back(*i);
    }
  }
  out->push_back('!');
}

static int decode_escaped(const char *p, string *out)
{
  const char *orig_p = p;
  while (*p && *p != '!') {
    if (*p == '#' || *p == '~') {
      unsigned hex;
      int r = sscanf(++p, "%2x", &hex);
      if (r < 1)
	return -EINVAL;
      out->push_back((char)hex);
      p += 2;
    } else {
      out->push_back(*p++);
    }
  }
  return p - orig_p;
}

// some things we encode in binary (as le32 or le64); print the
// resulting key strings nicely
string pretty_binary_string(const string& in)
{
  char buf[10];
  string out;
  out.reserve(in.length() * 3);
  enum { NONE, HEX, STRING } mode = NONE;
  unsigned from = 0, i;
  for (i=0; i < in.length(); ++i) {
    if ((in[i] < 32 || (unsigned char)in[i] > 126) ||
	(mode == HEX && in.length() - i >= 4 &&
	 ((in[i] < 32 || (unsigned char)in[i] > 126) ||
	  (in[i+1] < 32 || (unsigned char)in[i+1] > 126) ||
	  (in[i+2] < 32 || (unsigned char)in[i+2] > 126) ||
	  (in[i+3] < 32 || (unsigned char)in[i+3] > 126)))) {
      if (mode == STRING) {
	out.append(in.substr(from, i - from));
	out.push_back('\'');
      }
      if (mode != HEX) {
	out.append("0x");
	mode = HEX;
      }
      if (in.length() - i >= 4) {
	// print a whole u32 at once
	snprintf(buf, sizeof(buf), "%08x",
		 (uint32_t)(((unsigned char)in[i] << 24) |
			    ((unsigned char)in[i+1] << 16) |
			    ((unsigned char)in[i+2] << 8) |
			    ((unsigned char)in[i+3] << 0)));
	i += 3;
      } else {
	snprintf(buf, sizeof(buf), "%02x", (int)(unsigned char)in[i]);
      }
      out.append(buf);
    } else {
      if (mode != STRING) {
	out.push_back('\'');
	mode = STRING;
	from = i;
      }
    }
  }
  if (mode == STRING) {
    out.append(in.substr(from, i - from));
    out.push_back('\'');
  }
  return out;
}

static void _key_encode_shard(shard_id_t shard, string *key)
{
  // make field ordering match with ghobject_t compare operations
  if (shard == shard_id_t::NO_SHARD) {
    // otherwise ff will sort *after* 0, not before.
    key->append("--");
  } else {
    char buf[32];
    snprintf(buf, sizeof(buf), "%02x", (int)shard);
    key->append(buf);
  }
}
static const char *_key_decode_shard(const char *key, shard_id_t *pshard)
{
  if (key[0] == '-') {
    *pshard = shard_id_t::NO_SHARD;
  } else {
    unsigned shard;
    int r = sscanf(key, "%x", &shard);
    if (r < 1)
      return NULL;
    *pshard = shard_id_t(shard);
  }
  return key + 2;
}

static void _key_encode_u32(uint32_t u, string *key)
{
  uint32_t bu;
#ifdef CEPH_BIG_ENDIAN
  bu = u;
#elif defined(CEPH_LITTLE_ENDIAN)
  bu = swab(u);
#else
# error wtf
#endif
  key->append((char*)&bu, 4);
}

static const char *_key_decode_u32(const char *key, uint32_t *pu)
{
  uint32_t bu;
  memcpy(&bu, key, 4);
#ifdef CEPH_BIG_ENDIAN
  *pu = bu;
#elif defined(CEPH_LITTLE_ENDIAN)
  *pu = swab(bu);
#else
# error wtf
#endif
  return key + 4;
}

static void _key_encode_u64(uint64_t u, string *key)
{
  uint64_t bu;
#ifdef CEPH_BIG_ENDIAN
  bu = u;
#elif defined(CEPH_LITTLE_ENDIAN)
  bu = swab(u);
#else
# error wtf
#endif
  key->append((char*)&bu, 8);
}

static const char *_key_decode_u64(const char *key, uint64_t *pu)
{
  uint64_t bu;
  memcpy(&bu, key, 8);
#ifdef CEPH_BIG_ENDIAN
  *pu = bu;
#elif defined(CEPH_LITTLE_ENDIAN)
  *pu = swab(bu);
#else
# error wtf
#endif
  return key + 8;
}

static void get_coll_key_range(const coll_t& cid, int bits,
			       string *temp_start, string *temp_end,
			       string *start, string *end)
{
  temp_start->clear();
  temp_end->clear();
  start->clear();
  end->clear();

  spg_t pgid;
  if (cid.is_pg(&pgid)) {
    _key_encode_shard(pgid.shard, start);
    *end = *start;
    *temp_start = *start;
    *temp_end = *start;

    _key_encode_u64(pgid.pool() + 0x8000000000000000ull, start);
    _key_encode_u64((-2ll - pgid.pool()) + 0x8000000000000000ull, temp_start);
    _key_encode_u32(hobject_t::_reverse_bits(pgid.ps()), start);
    _key_encode_u32(hobject_t::_reverse_bits(pgid.ps()), temp_start);
    start->append(".");
    temp_start->append(".");

    _key_encode_u64(pgid.pool() + 0x8000000000000000ull, end);
    _key_encode_u64((-2ll - pgid.pool()) + 0x8000000000000000ull, temp_end);

    uint64_t end_hash =
      hobject_t::_reverse_bits(pgid.ps()) + (1ull << (32-bits));
    if (end_hash <= 0xffffffffull) {
      _key_encode_u32(end_hash, end);
      _key_encode_u32(end_hash, temp_end);
      end->append(".");
      temp_end->append(".");
    } else {
      _key_encode_u32(0xffffffff, end);
      _key_encode_u32(0xffffffff, temp_end);
      end->append(":");
      temp_end->append(":");
    }
  } else {
    _key_encode_shard(shard_id_t::NO_SHARD, start);
    _key_encode_u64(-1ull + 0x8000000000000000ull, start);
    *end = *start;
    _key_encode_u32(0, start);
    start->append(".");
    _key_encode_u32(0xffffffff, end);
    end->append(":");

    // no separate temp section
    *temp_start = *end;
    *temp_end = *end;
  }
}

static int get_key_object(const string& key, ghobject_t *oid);

static void get_object_key(CephContext* cct, const ghobject_t& oid, string *key)
{
  key->clear();

  _key_encode_shard(oid.shard_id, key);
  _key_encode_u64(oid.hobj.pool + 0x8000000000000000ull, key);
  _key_encode_u32(oid.hobj.get_bitwise_key_u32(), key);
  key->append(".");

  append_escaped(oid.hobj.nspace, key);

  if (oid.hobj.get_key().length()) {
    // is a key... could be < = or >.
    // (ASCII chars < = and > sort in that order, yay)
    if (oid.hobj.get_key() < oid.hobj.oid.name) {
      key->append("<");
      append_escaped(oid.hobj.get_key(), key);
      append_escaped(oid.hobj.oid.name, key);
    } else if (oid.hobj.get_key() > oid.hobj.oid.name) {
      key->append(">");
      append_escaped(oid.hobj.get_key(), key);
      append_escaped(oid.hobj.oid.name, key);
    } else {
      // same as no key
      key->append("=");
      append_escaped(oid.hobj.oid.name, key);
    }
  } else {
    // no key
    key->append("=");
    append_escaped(oid.hobj.oid.name, key);
  }

  _key_encode_u64(oid.hobj.snap, key);
  _key_encode_u64(oid.generation, key);

  // sanity check
  if (true) {
    ghobject_t t;
    int r = get_key_object(*key, &t);
    if (r || t != oid) {
      derr << "  r " << r << dendl;
      derr << "key " << pretty_binary_string(*key) << dendl;
      derr << "oid " << oid << dendl;
      derr << "  t " << t << dendl;
      assert(t == oid);
    }
  }
}

static int get_key_object(const string& key, ghobject_t *oid)
{
  int r;
  const char *p = key.c_str();

  p = _key_decode_shard(p, &oid->shard_id);
  if (!p)
    return -2;

  uint64_t pool;
  p = _key_decode_u64(p, &pool);
  if (!p)
    return -3;
  oid->hobj.pool = pool - 0x8000000000000000;

  unsigned hash;
  p = _key_decode_u32(p, &hash);
  if (!p)
    return -4;
  oid->hobj.set_bitwise_key_u32(hash);
  if (*p != '.')
    return -5;
  ++p;

  r = decode_escaped(p, &oid->hobj.nspace);
  if (r < 0)
    return -6;
  p += r + 1;

  if (*p == '=') {
    // no key
    ++p;
    r = decode_escaped(p, &oid->hobj.oid.name);
    if (r < 0)
      return -8;
    p += r + 1;
  } else if (*p == '<' || *p == '>') {
    // key + name
    ++p;
    string okey;
    r = decode_escaped(p, &okey);
    if (r < 0)
      return -8;
    p += r + 1;
    r = decode_escaped(p, &oid->hobj.oid.name);
    if (r < 0)
      return -9;
    p += r + 1;
  } else {
    // malformed
    return -7;
  }

  p = _key_decode_u64(p, &oid->hobj.snap.val);
  if (!p)
    return -10;
  p = _key_decode_u64(p, &oid->generation);
  if (!p)
    return -11;
  return 0;
}


void get_overlay_key(uint64_t nid, uint64_t offset, string *out)
{
  _key_encode_u64(nid, out);
  _key_encode_u64(offset, out);
}

// '-' < '.' < '~'
void get_omap_header(uint64_t id, string *out)
{
  _key_encode_u64(id, out);
  out->push_back('-');
}

// hmm, I don't think there's any need to escape the user key since we
// have a clean prefix.
void get_omap_key(uint64_t id, const string& key, string *out)
{
  _key_encode_u64(id, out);
  out->push_back('.');
  out->append(key);
}

void rewrite_omap_key(uint64_t id, string old, string *out)
{
  _key_encode_u64(id, out);
  out->append(old.substr(out->length()));
}

void decode_omap_key(const string& key, string *user_key)
{
  *user_key = key.substr(sizeof(uint64_t) + 1);
}

void get_omap_tail(uint64_t id, string *out)
{
  _key_encode_u64(id, out);
  out->push_back('~');
}

void get_wal_key(uint64_t seq, string *out)
{
  _key_encode_u64(seq, out);
}


// Onode

NewStore::Onode::Onode(const ghobject_t& o, const string& k)
  : nref(0),
    oid(o),
    key(k),
    dirty(false),
    exists(true),
    flush_lock("NewStore::Onode::flush_lock") {
}

// OnodeHashLRU

#undef dout_prefix
#define dout_prefix *_dout << "newstore.lru(" << this << ") "

void NewStore::OnodeHashLRU::_touch(OnodeRef o)
{
  lru_list_t::iterator p = lru.iterator_to(*o);
  lru.erase(p);
  lru.push_front(*o);
}

void NewStore::OnodeHashLRU::add(const ghobject_t& oid, OnodeRef o)
{
  Mutex::Locker l(lock);
  dout(30) << __func__ << " " << oid << " " << o << dendl;
  assert(onode_map.count(oid) == 0);
  onode_map[oid] = o;
  lru.push_back(*o);
}

NewStore::OnodeRef NewStore::OnodeHashLRU::lookup(const ghobject_t& oid)
{
  Mutex::Locker l(lock);
  dout(30) << __func__ << dendl;
  ceph::unordered_map<ghobject_t,OnodeRef>::iterator p = onode_map.find(oid);
  if (p == onode_map.end()) {
    dout(30) << __func__ << " " << oid << " miss" << dendl;
    return OnodeRef();
  }
  dout(30) << __func__ << " " << oid << " hit " << p->second << dendl;
  _touch(p->second);
  return p->second;
}

void NewStore::OnodeHashLRU::clear()
{
  Mutex::Locker l(lock);
  dout(10) << __func__ << dendl;
  lru.clear();
  onode_map.clear();
}

void NewStore::OnodeHashLRU::remove(const ghobject_t& oid)
{
  Mutex::Locker l(lock);
  ceph::unordered_map<ghobject_t,OnodeRef>::iterator p = onode_map.find(oid);
  if (p == onode_map.end()) {
    dout(30) << __func__ << " " << oid << " miss" << dendl;
    return;
  }
  dout(30) << __func__ << " " << oid << " hit " << p->second << dendl;
  lru_list_t::iterator pi = lru.iterator_to(*p->second);
  lru.erase(pi);
  onode_map.erase(p);
}

void NewStore::OnodeHashLRU::rename(const ghobject_t& old_oid,
				    const ghobject_t& new_oid)
{
  Mutex::Locker l(lock);
  dout(30) << __func__ << " " << old_oid << " -> " << new_oid << dendl;
  ceph::unordered_map<ghobject_t,OnodeRef>::iterator po, pn;
  po = onode_map.find(old_oid);
  pn = onode_map.find(new_oid);

  assert(po != onode_map.end());
  if (pn != onode_map.end()) {
    lru_list_t::iterator p = lru.iterator_to(*pn->second);
    lru.erase(p);
    onode_map.erase(pn);
  }
  onode_map.insert(make_pair(new_oid, po->second));
  _touch(po->second);
  onode_map.erase(po);
}

bool NewStore::OnodeHashLRU::get_next(
  const ghobject_t& after,
  pair<ghobject_t,OnodeRef> *next)
{
  Mutex::Locker l(lock);
  dout(20) << __func__ << " after " << after << dendl;

  if (after == ghobject_t()) {
    if (lru.empty()) {
      return false;
    }
    ceph::unordered_map<ghobject_t,OnodeRef>::iterator p = onode_map.begin();
    assert(p != onode_map.end());
    next->first = p->first;
    next->second = p->second;
    return true;
  }

  ceph::unordered_map<ghobject_t,OnodeRef>::iterator p = onode_map.find(after);
  assert(p != onode_map.end()); // for now
  lru_list_t::iterator pi = lru.iterator_to(*p->second);
  ++pi;
  if (pi == lru.end()) {
    return false;
  }
  next->first = pi->oid;
  next->second = onode_map[pi->oid];
  return true;
}

int NewStore::OnodeHashLRU::trim(int max)
{
  Mutex::Locker l(lock);
  dout(20) << __func__ << " max " << max
	   << " size " << onode_map.size() << dendl;
  int trimmed = 0;
  int num = onode_map.size() - max;
  lru_list_t::iterator p = lru.end();
  if (num)
    --p;
  while (num > 0) {
    Onode *o = &*p;
    int refs = o->nref;
    if (refs > 1) {
      dout(20) << __func__ << "  " << o->oid << " has " << refs
	       << " refs; stopping with " << num << " left to trim" << dendl;
      break;
    }
    dout(30) << __func__ << "  trim " << o->oid << dendl;
    if (p != lru.begin()) {
      lru.erase(p--);
    } else {
      lru.erase(p);
      assert(num == 1);
    }
    o->get();  // paranoia
    onode_map.erase(o->oid);
    o->put();
    --num;
    ++trimmed;
  }
  return trimmed;
}

// =======================================================

// Collection

#undef dout_prefix
#define dout_prefix *_dout << "newstore(" << store->path << ").collection(" << cid << ") "

NewStore::Collection::Collection(CephContext* cct, NewStore *ns, coll_t c)
  : store(ns),
    cid(c),
    lock("NewStore::Collection::lock"),
    onode_map(cct)
{
}

NewStore::OnodeRef NewStore::Collection::get_onode(
  
  const ghobject_t& oid,
  bool create)
{
  assert(create ? lock.is_wlocked() : lock.is_locked());

  spg_t pgid;
  if (cid.is_pg(&pgid)) {
    if (!oid.match(cnode.bits, pgid.ps())) {
      lderr(store->cct) << __func__ << " oid " << oid << " not part of " << pgid
	   << " bits " << cnode.bits << dendl;
      assert(0);
    }
  }

  OnodeRef o = onode_map.lookup(oid);
  if (o)
    return o;

  string key;
  get_object_key(store->cct, oid, &key);

  ldout(store->cct, 20) << __func__ << " oid " << oid << " key "
	   << pretty_binary_string(key) << dendl;

  bufferlist v;
  int r = store->db->get(PREFIX_OBJ, key, &v);
  ldout(store->cct, 20) << " r " << r << " v.len " << v.length() << dendl;
  Onode *on;
  if (v.length() == 0) {
    assert(r == -ENOENT);
    if (!create)
      return OnodeRef();

    // new
    on = new Onode(oid, key);
    on->dirty = true;
  } else {
    // loaded
    assert(r >=0);
    on = new Onode(oid, key);
    bufferlist::iterator p = v.begin();
    ::decode(on->onode, p);
  }
  o.reset(on);
  onode_map.add(oid, o);
  return o;
}



// =======================================================

#undef dout_prefix
#define dout_prefix *_dout << "newstore(" << path << ") "


NewStore::NewStore(CephContext *cct, const string& path)
  : ObjectStore(cct, path),
    db(NULL),
    fs(NULL),
    path_fd(-1),
    fsid_fd(-1),
    frag_fd(-1),
    fset_fd(-1),
    mounted(false),
    coll_lock("NewStore::coll_lock"),
    fid_lock("NewStore::fid_lock"),
    nid_lock("NewStore::nid_lock"),
    nid_max(0),
    throttle_ops(cct, "newstore_max_ops", cct->_conf->get_val<uint64_t>("newstore_max_ops")),
    throttle_bytes(cct, "newstore_max_bytes", cct->_conf->get_val<uint64_t>("newstore_max_bytes")),
    throttle_wal_ops(cct, "newstore_wal_max_ops",
		     cct->_conf->get_val<uint64_t>("newstore_max_ops") +
		     cct->_conf->get_val<uint64_t>("newstore_wal_max_ops")),
    throttle_wal_bytes(cct, "newstore_wal_max_bytes",
		       cct->_conf->get_val<uint64_t>("newstore_max_bytes") +
		       cct->_conf->get_val<uint64_t>("newstore_wal_max_bytes")),
    wal_lock("NewStore::wal_lock"),
    wal_seq(0),
    wal_tp(cct,
	   "NewStore::wal_tp",
           "tp_wal_tp",
	   cct->_conf->get_val<uint64_t>("newstore_wal_threads"),
	   "newstore_wal_threads"),
    wal_wq(this,
	     cct->_conf->get_val<uint64_t>("newstore_wal_thread_timeout"),
	     cct->_conf->get_val<uint64_t>("newstore_wal_thread_suicide_timeout"),
	     &wal_tp),
    finisher(cct),
    fsync_tp(cct,
	     "NewStore::fsync_tp",
             "tp_fsync_tp",
	     cct->_conf->get_val<uint64_t>("newstore_fsync_threads"),
	     "newstore_fsync_threads"),
    fsync_wq(this,
	     cct->_conf->get_val<uint64_t>("newstore_fsync_thread_timeout"),
	     cct->_conf->get_val<uint64_t>("newstore_fsync_thread_suicide_timeout"),
	     &fsync_tp),
    aio_thread(this),
    aio_stop(false),
    aio_queue(cct->_conf->get_val<uint64_t>("newstore_aio_max_queue_depth")),
    kv_sync_thread(this),
    kv_lock("NewStore::kv_lock"),
    kv_stop(false),
    reap_lock("NewStore::reap_lock")
{
  _init_logger();
//  cct->_conf->add_observer(this);
}

NewStore::~NewStore()
{
//  cct->_conf->remove_observer(this);
  _shutdown_logger();
  assert(!mounted);
  assert(db == NULL);
  assert(fsid_fd < 0);
  assert(frag_fd < 0);
}

void NewStore::_init_logger()
{
  PerfCountersBuilder b(cct, "newstore",
                        l_newstore_first, l_newstore_last);
  b.add_time_avg(l_newstore_kv_flush_lat, "kv_flush_lat",
                 "Average kv_thread flush latency",
                 "fl_l", PerfCountersBuilder::PRIO_INTERESTING);
  logger = b.create_perf_counters();
  cct->get_perfcounters_collection()->add(logger);
}

void NewStore::_shutdown_logger()
{
  cct->get_perfcounters_collection()->remove(logger);
  delete logger;
}

int NewStore::peek_journal_fsid(uuid_d *fsid)
{
  return 0;
}

int NewStore::_open_path()
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

void NewStore::_close_path()
{
  VOID_TEMP_FAILURE_RETRY(::close(path_fd));
  path_fd = -1;
  delete fs;
  fs = NULL;
}

int NewStore::_open_frag()
{
  assert(frag_fd < 0);
  frag_fd = ::openat(path_fd, "fragments", O_DIRECTORY);
  if (frag_fd < 0) {
    int r = -errno;
    derr << __func__ << " cannot open " << path << "/fragments: "
	 << cpp_strerror(r) << dendl;
    return r;
  }
  return 0;
}

int NewStore::_create_frag()
{
  assert(frag_fd < 0);
  frag_fd = ::openat(path_fd, "fragments", O_DIRECTORY);
  if (frag_fd < 0 && errno == ENOENT) {
    int r = ::mkdirat(path_fd, "fragments", 0755);
    if (r < 0) {
      r = -errno;
      derr << __func__ << " cannot create " << path << "/fragments: "
	   << cpp_strerror(r) << dendl;
      return r;
    }
    frag_fd = ::openat(path_fd, "fragments", O_DIRECTORY);
  }
  if (frag_fd < 0) {
    int r = -errno;
    derr << __func__ << " cannot open created " << path << "/fragments: "
	 << cpp_strerror(r) << dendl;
    return r;
  }
  return 0;
}

void NewStore::_close_frag()
{
  if (fset_fd >= 0) {
    VOID_TEMP_FAILURE_RETRY(::close(fset_fd));
    fset_fd = -1;
  }
  VOID_TEMP_FAILURE_RETRY(::close(frag_fd));
  frag_fd = -1;
}

int NewStore::_open_fsid(bool create)
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

int NewStore::_read_fsid(uuid_d *uuid)
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

int NewStore::_write_fsid()
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

void NewStore::_close_fsid()
{
  VOID_TEMP_FAILURE_RETRY(::close(fsid_fd));
  fsid_fd = -1;
}

int NewStore::_lock_fsid()
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

bool NewStore::test_mount_in_use()
{
  // most error conditions mean the mount is not in use (e.g., because
  // it doesn't exist).  only if we fail to lock do we conclude it is
  // in use.
  bool ret = false;
  int r = _open_path();
  if (r < 0)
    return false;
  r = _open_fsid(false);
  if (r < 0)
    goto out_path;
  r = _lock_fsid();
  if (r < 0)
    ret = true; // if we can't lock, it is in used
  _close_fsid();
 out_path:
  _close_path();
  return ret;
}

int NewStore::_open_db(bool create)
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

void NewStore::_close_db()
{
  assert(db);
  delete db;
  db = NULL;
}

int NewStore::_aio_start()
{
  if (cct->_conf->get_val<bool>("newstore_aio")) {
    dout(10) << __func__ << dendl;
    int r = aio_queue.init();
    if (r < 0)
      return r;
    aio_thread.create("newstore_aio");
  }
  return 0;
}

void NewStore::_aio_stop()
{
  if (cct->_conf->get_val<bool>("newstore_aio")) {
    dout(10) << __func__ << dendl;
    aio_stop = true;
    aio_thread.join();
    aio_stop = false;
    aio_queue.shutdown();
  }
}

int NewStore::_open_collections()
{
  KeyValueDB::Iterator it = db->get_iterator(PREFIX_COLL);
  for (it->upper_bound(string());
       it->valid();
       it->next()) {
    coll_t cid;
    if (cid.parse(it->key())) {
      CollectionRef c(new Collection(cct, this, cid));
      bufferlist bl;
      db->get(PREFIX_COLL, it->key(), &bl);
      bufferlist::iterator p = bl.begin();
      ::decode(c->cnode, p);
      dout(20) << __func__ << " opened " << cid << dendl;
      coll_map[cid] = c;
    } else {
      dout(20) << __func__ << " unrecognized collection " << it->key() << dendl;
    }
  }
  return 0;
}

int NewStore::mkfs()
{
  dout(1) << __func__ << " path " << path << dendl;
  int r;
  uuid_d old_fsid;

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

  r = _create_frag();
  if (r < 0)
    goto out_close_fsid;

  r = _open_db(true);
  if (r < 0)
    goto out_close_frag;

  // FIXME: superblock

  dout(10) << __func__ << " success" << dendl;
  r = 0;
  _close_db();

 out_close_frag:
  _close_frag();
 out_close_fsid:
  _close_fsid();
 out_path_fd:
  _close_path();
  return r;
}

int NewStore::mount()
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

  r = _open_frag();
  if (r < 0)
    goto out_fsid;

  // FIXME: superblock, features

  r = _open_db(false);
  if (r < 0)
    goto out_frag;

  r = _recover_next_fid();
  if (r < 0)
    goto out_db;

  r = _recover_next_nid();
  if (r < 0)
    goto out_db;

  r = _open_collections();
  if (r < 0)
    goto out_db;

  r = _aio_start();
  if (r < 0)
    goto out_db;

  r = _wal_replay();
  if (r < 0)
    goto out_aio;

  finisher.start();
  fsync_tp.start();
  wal_tp.start();
  kv_sync_thread.create("kv_sync_thread");

  mounted = true;
  return 0;

 out_aio:
  _aio_stop();
 out_db:
  _close_db();
 out_frag:
  _close_frag();
 out_fsid:
  _close_fsid();
 out_path:
  _close_path();
  return r;
}

int NewStore::umount()
{
  assert(mounted);
  dout(1) << __func__ << dendl;

  _sync();
  _reap_collections();

  dout(20) << __func__ << " stopping fsync_wq" << dendl;
  fsync_tp.stop();
  dout(20) << __func__ << " stopping aio" << dendl;
  _aio_stop();
  dout(20) << __func__ << " stopping kv thread" << dendl;
  _kv_stop();
  dout(20) << __func__ << " draining wal_wq" << dendl;
  wal_wq.drain();
  dout(20) << __func__ << " stopping wal_tp" << dendl;
  wal_tp.stop();
  dout(20) << __func__ << " draining finisher" << dendl;
  finisher.wait_for_empty();
  dout(20) << __func__ << " stopping finisher" << dendl;
  finisher.stop();
  dout(20) << __func__ << " closing" << dendl;

  mounted = false;
  if (fset_fd >= 0)
    VOID_TEMP_FAILURE_RETRY(::close(fset_fd));
  _close_db();
  _close_frag();
  _close_fsid();
  _close_path();
  return 0;
}

void NewStore::_sync()
{
  dout(10) << __func__ << dendl;

  dout(20) << " flushing fsync wq" << dendl;
  fsync_wq.flush();

  kv_lock.Lock();
  while (!kv_committing.empty() ||
	 !kv_queue.empty()) {
    dout(20) << " waiting for kv to commit" << dendl;
    kv_sync_cond.Wait(kv_lock);
  }
  kv_lock.Unlock();

  dout(10) << __func__ << " done" << dendl;
}

int NewStore::statfs(struct store_statfs_t *buf)
{
  struct statfs _buf;
  buf->reset();
  if (::statfs(path.c_str(), &_buf) < 0) {
    int r = -errno;
    assert(!cct->_conf->get_val<bool>("newstore_fail_eio") || r != -EIO);
    return r;
  }
  buf->total = _buf.f_blocks * _buf.f_bsize;
  buf->available = _buf.f_bavail * _buf.f_bsize;
  return 0;
}

// ---------------
// cache

NewStore::CollectionRef NewStore::_get_collection(const coll_t& cid)
{
  RWLock::RLocker l(coll_lock);
  ceph::unordered_map<coll_t,CollectionRef>::iterator cp = coll_map.find(cid);
  if (cp == coll_map.end())
    return CollectionRef();
  return cp->second;
}

void NewStore::_queue_reap_collection(CollectionRef& c)
{
  dout(10) << __func__ << " " << c->cid << dendl;
  Mutex::Locker l(reap_lock);
  removed_collections.push_back(c);
}

void NewStore::_reap_collections()
{
  reap_lock.Lock();

  list<CollectionRef> removed_colls;
  removed_colls.swap(removed_collections);
  reap_lock.Unlock();

  for (list<CollectionRef>::iterator p = removed_colls.begin();
       p != removed_colls.end();
       ++p) {
    CollectionRef c = *p;
    dout(10) << __func__ << " " << c->cid << dendl;
    {
      pair<ghobject_t,OnodeRef> next;
      while (c->onode_map.get_next(next.first, &next)) {
	assert(!next.second->exists);
	if (!next.second->flush_txns.empty()) {
	  dout(10) << __func__ << " " << c->cid << " " << next.second->oid
		   << " flush_txns " << next.second->flush_txns << dendl;
	  return;
	}
      }
    }
    c->onode_map.clear();
    dout(10) << __func__ << " " << c->cid << " done" << dendl;
  }

  dout(10) << __func__ << " all reaped" << dendl;
  reap_cond.Signal();
}

// ---------------
// read operations

bool NewStore::exists(const coll_t& cid, const ghobject_t& oid)
{
  CollectionHandle c = _get_collection(cid);
  if (!c)
    return false;
  return exists(c, oid);
}

bool NewStore::exists(CollectionHandle &c_, const ghobject_t& oid)
{
  Collection *c = static_cast<Collection *>(c_.get());
  dout(10) << __func__ << " " << c->cid << " " << oid << dendl;
  if (!c->exists)
    return false;

  bool r = true;

  {
    RWLock::RLocker l(c->lock);
    OnodeRef o = c->get_onode(oid, false);
    if (!o || !o->exists)
      r = false;
  }

  return r;
}

/*
int NewStore::stat(
    coll_t cid,
    const ghobject_t& oid,
    struct stat *st,
    bool allow_eio)
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = _get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::RLocker l(c->lock);
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists)
    return -ENOENT;
  st->st_size = o->onode.size;
  st->st_blksize = 4096;
  st->st_blocks = (st->st_size + st->st_blksize - 1) / st->st_blksize;
  st->st_nlink = 1;
  return 0;
}
*/

int NewStore::set_collection_opts(
  const coll_t& cid,
  const pool_opts_t& opts)
{
  CollectionHandle ch = _get_collection(cid);
  if (!ch)
    return -ENOENT;
  Collection *c = static_cast<Collection *>(ch.get());
  dout(15) << __func__ << " " << cid << " options " << opts << dendl;
  if (!c->exists)
    return -ENOENT;
  RWLock::WLocker l(c->lock);
  c->pool_opts = opts;
  return 0;
}

int NewStore::stat(
    const coll_t& cid,
    const ghobject_t& oid,
    struct stat *st,
    bool allow_eio)
{
  CollectionHandle c = _get_collection(cid);
  if (!c)
    return -ENOENT;
  return stat(c, oid, st, allow_eio);
}

int NewStore::stat(
  CollectionHandle &c_,
  const ghobject_t& oid,
  struct stat *st,
  bool allow_eio)
{
  Collection *c = static_cast<Collection *>(c_.get());
  if (!c->exists)
    return -ENOENT;
  dout(10) << __func__ << " " << c->get_cid() << " " << oid << dendl;

  {
    RWLock::RLocker l(c->lock);
    OnodeRef o = c->get_onode(oid, false);
    if (!o || !o->exists)
      return -ENOENT;
    st->st_size = o->onode.size;
    st->st_blksize = 4096;
    st->st_blocks = (st->st_size + st->st_blksize - 1) / st->st_blksize;
    st->st_nlink = 1;
  }

  return 0;
}

/*
int NewStore::read(
  coll_t cid,
  const ghobject_t& oid,
  uint64_t offset,
  size_t length,
  bufferlist& bl,
  uint32_t op_flags,
  bool allow_eio)
{
  dout(15) << __func__ << " " << cid << " " << oid
	   << " " << offset << "~" << length
	   << dendl;
  bl.clear();
  CollectionRef c = _get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::RLocker l(c->lock);

  int r;

  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }

  if (offset == length && offset == 0)
    length = o->onode.size;

  r = _do_read(o, offset, length, bl, op_flags);

 out:
  dout(10) << __func__ << " " << cid << " " << oid
	   << " " << offset << "~" << length
	   << " = " << r << dendl;
  return r;
}
*/

int NewStore::read(
  const coll_t& cid,
  const ghobject_t& oid,
  uint64_t offset,
  size_t length,
  bufferlist& bl,
  uint32_t op_flags)
{
  dout(15) << __func__ << " " << cid << " " << oid
           << " " << offset << "~" << length
           << dendl;
  bl.clear();
  CollectionRef c = _get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::RLocker l(c->lock);

  int r;

  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }

  if (offset == length && offset == 0)
    length = o->onode.size;

  r = _do_read(o, offset, length, bl, op_flags);

 out:
  dout(10) << __func__ << " " << cid << " " << oid
           << " " << offset << "~" << length
           << " = " << r << dendl;
  return r;
}


int NewStore::_do_read(
    OnodeRef o,
    uint64_t offset,
    size_t length,
    bufferlist& bl,
    uint32_t op_flags)
{
  map<uint64_t,fragment_t>::iterator fp, fend;
  map<uint64_t,overlay_t>::iterator op, oend;
  int r;
  int fd = -1;
  fid_t cur_fid;

  dout(20) << __func__ << " " << offset << "~" << length << " size "
	   << o->onode.size << dendl;

  if (offset > o->onode.size) {
    r = 0;
    goto out;
  }

  if (offset + length > o->onode.size) {
    length = o->onode.size - offset;
  }

  o->flush();

  r = 0;

  // loop over overlays and data fragments.  overlays take precedence.
  fend = o->onode.data_map.end();
  fp = o->onode.data_map.lower_bound(offset);
  if (fp != o->onode.data_map.begin()) {
    --fp;
  }
  oend = o->onode.overlay_map.end();
  op = o->onode.overlay_map.lower_bound(offset);
  if (op != o->onode.overlay_map.begin()) {
    --op;
  }
  while (length > 0) {
    if (op != oend && op->first + op->second.length < offset) {
      dout(20) << __func__ << " skip overlay " << op->first << " " << op->second
	       << dendl;
      ++op;
      continue;
    }
    if (fp != fend && fp->first + fp->second.length <= offset) {
      dout(30) << __func__ << " skip frag " << fp->first << "~" << fp->second
	       << dendl;
      ++fp;
      continue;
    }

    // overlay?
    if (op != oend && op->first <= offset) {
      uint64_t x_off = offset - op->first + op->second.value_offset;
      uint64_t x_len = MIN(op->first + op->second.length - offset, length);
      dout(20) << __func__ << "  overlay " << op->first << " " << op->second
	       << " use " << x_off << "~" << x_len << dendl;
      bufferlist v;
      string key;
      get_overlay_key(o->onode.nid, op->second.key, &key);
      db->get(PREFIX_OVERLAY, key, &v);
      bufferlist frag;
      frag.substr_of(v, x_off, x_len);
      bl.claim_append(frag);
      ++op;
      length -= x_len;
      offset += x_len;
      continue;
    }

    unsigned x_len = length;
    if (op != oend &&
	op->first > offset &&
	op->first - offset < x_len) {
      x_len = op->first - offset;
    }

    // frag?
    if (fp != fend && fp->first <= offset) {
      if (fp->second.fid != cur_fid) {
	cur_fid = fp->second.fid;
	if (fd >= 0) {
	  VOID_TEMP_FAILURE_RETRY(::close(fd));
	}
	fd = _open_fid(cur_fid, O_RDONLY);
	if (fd < 0) {
	  r = fd;
	  goto out;
	}
      }
      uint64_t x_off = offset - fp->first - fp->second.offset;
      x_len = MIN(x_len, fp->second.length - x_off);
      dout(30) << __func__ << " data " << fp->first << " " << fp->second
	       << " use " << x_off << "~" << x_len
	       << " fid " << cur_fid << " offset " << x_off + fp->second.offset
	       << dendl;
      r = ::lseek64(fd, x_off, SEEK_SET);
      if (r < 0) {
	r = -errno;
	goto out;
      }
      bufferlist t;
      r = t.read_fd(fd, x_len);
      if (r < 0) {
	goto out;
      }
      bl.claim_append(t);
      if ((unsigned)r < x_len) {
	dout(10) << __func__ << "   short read " << r << " < " << x_len
		 << " from " << cur_fid << dendl;
	bufferptr z(x_len - r);
	z.zero();
	bl.append(z);
      }
      offset += x_len;
      length -= x_len;
      if (x_off + x_len == fp->second.length) {
	++fp;
      }
      continue;
    }

    // zero.
    dout(30) << __func__ << " zero " << offset << "~" << x_len << dendl;
    bufferptr bp(x_len);
    bp.zero();
    bl.push_back(bp);
    offset += x_len;
    length -= x_len;
    continue;
  }
  r = bl.length();

 out:
  if (fd >= 0) {
    VOID_TEMP_FAILURE_RETRY(::close(fd));
  }
  return r;
}

int NewStore::fiemap(
  const coll_t& cid,
  const ghobject_t& oid,
  uint64_t offset,
  size_t len,
  bufferlist& bl)
{

  map<uint64_t, uint64_t> destmap;
  int r = fiemap(cid, oid, offset, len, destmap);
  if (r >= 0) {
    ::encode(destmap, bl);
  }
  return r;
}


int NewStore::fiemap(
  const coll_t& cid,
  const ghobject_t& oid,
  uint64_t offset,
  size_t len,
  map<uint64_t, uint64_t>& destmap)
{

  CollectionRef c = _get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::RLocker l(c->lock);

  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    return -ENOENT;
  }

  if (offset == len && offset == 0)
    len = o->onode.size;

  if (offset > o->onode.size)
    return 0;

  if (offset + len > o->onode.size) {
    len = o->onode.size - offset;
  }

  dout(20) << __func__ << " " << offset << "~" << len << " size "
	   << o->onode.size << dendl;

  map<uint64_t,fragment_t>::iterator fp, fend;
  map<uint64_t,overlay_t>::iterator op, oend;

  // loop over overlays and data fragments.  overlays take precedence.
  fend = o->onode.data_map.end();
  fp = o->onode.data_map.lower_bound(offset);
  if (fp != o->onode.data_map.begin()) {
    --fp;
  }
  oend = o->onode.overlay_map.end();
  op = o->onode.overlay_map.lower_bound(offset);
  if (op != o->onode.overlay_map.begin()) {
    --op;
  }
  uint64_t start = offset;
  while (len > 0) {
    if (op != oend && op->first + op->second.length < offset) {
      ++op;
      continue;
    }
    if (fp != fend && fp->first + fp->second.length <= offset) {
      ++fp;
      continue;
    }

    // overlay?
    if (op != oend && op->first <= offset) {
      uint64_t x_len = MIN(op->first + op->second.length - offset, len);
      //destmap[offset] = x_len;
      dout(30) << __func__ << " get overlay, off =  " << offset << " len=" << x_len << dendl;
      len -= x_len;
      offset += x_len;
      ++op;
      continue;
    }

    unsigned x_len = len;
    if (op != oend &&
	op->first > offset &&
	op->first - offset < x_len) {
      x_len = op->first - offset;
    }

    // frag?
    if (fp != fend && fp->first <= offset) {
      uint64_t x_off = offset - fp->first - fp->second.offset;
      x_len = MIN(x_len, fp->second.length - x_off);
      //destmap[offset] = x_len;
      dout(30) << __func__ << " get frag, off =  " << offset << " len=" << x_len << dendl;
      len -= x_len;
      offset += x_len;
      if (x_off + x_len == fp->second.length)
	++fp;
      continue;
    }
    // we are seeing a hole, time to add an entry to fiemap.
    destmap[start] = offset - start;
    dout(20) << __func__ << " get fiemap entry, off =  " << start << " len=" << destmap[start] << dendl;
    offset += x_len;
    start = offset;
    len -= x_len;
    continue;
  }
  //add tailing
  if (offset - start != 0) {
    destmap[start] = offset - start;
    dout(20) << __func__ << " get fiemap entry, off =  " << start << " len=" << destmap[start] << dendl;
  }
  dout(20) << __func__ << " " << offset << "~" << len << " size = 0 (" << destmap << ")" << dendl;
  return 0;
}

int NewStore::getattr(
  const coll_t& cid,
  const ghobject_t& oid,
  const char *name,
  bufferptr& value)
{
  dout(15) << __func__ << " " << cid << " " << oid << " " << name << dendl;
  CollectionRef c = _get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::RLocker l(c->lock);
  int r;
  string k(name);

  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }

  if (!o->onode.attrs.count(k)) {
    r = -ENODATA;
    goto out;
  }
  value = o->onode.attrs[k];
  r = 0;
 out:
  dout(10) << __func__ << " " << cid << " " << oid << " " << name
	   << " = " << r << dendl;
  return r;
}

int NewStore::getattrs(
  const coll_t& cid,
  const ghobject_t& oid,
  map<string,bufferptr>& aset)
{
  dout(15) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = _get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::RLocker l(c->lock);
  int r;

  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }
  aset = o->onode.attrs;
  r = 0;
 out:
  dout(10) << __func__ << " " << cid << " " << oid
	   << " = " << r << dendl;
  return r;
}

int NewStore::list_collections(vector<coll_t>& ls)
{
  RWLock::RLocker l(coll_lock);
  for (ceph::unordered_map<coll_t, CollectionRef>::iterator p = coll_map.begin();
       p != coll_map.end();
       ++p)
    ls.push_back(p->first);
  return 0;
}

bool NewStore::collection_exists(const coll_t& cid)
{
  RWLock::RLocker l(coll_lock);
  return coll_map.count(cid);
}

int NewStore::collection_empty(const coll_t& cid, bool *empty)
{
  dout(15) << __func__ << " " << cid << dendl;
  vector<ghobject_t> ls;
  ghobject_t next;
  int r = collection_list(cid, ghobject_t(), ghobject_t::get_max(), 5,
			  &ls, &next);
  if (r < 0)
    return false;  // fixme?
  *empty = ls.empty();
  dout(10) << __func__ << " " << cid << " = " << (int)(*empty) << dendl;
  return 0; 
}

int NewStore::collection_bits(const coll_t& cid)
{
  dout(15) << __func__ << " " << cid << dendl;
  CollectionRef c = _get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::RLocker l(c->lock);
  dout(10) << __func__ << " " << cid << " = " << c->cnode.bits << dendl;
  return c->cnode.bits;
}

int NewStore::collection_list(
  const coll_t& cid, const ghobject_t& start, const ghobject_t& end, int max,
  vector<ghobject_t> *ls, ghobject_t *pnext)
{
  dout(15) << __func__ << " " << cid
	   << " start " << start << " end " << end << " max " << max << dendl;
  CollectionRef c = _get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::RLocker l(c->lock);
  int r = 0;
  KeyValueDB::Iterator it;
  string temp_start_key, temp_end_key;
  string start_key, end_key;
  bool set_next = false;
  string pend;
  bool temp;

  ghobject_t static_next;
  if (!pnext)
    pnext = &static_next;

  if (start == ghobject_t::get_max())
    goto out;
  get_coll_key_range(cid, c->cnode.bits, &temp_start_key, &temp_end_key,
		     &start_key, &end_key);
  dout(20) << __func__
	   << " range " << pretty_binary_string(temp_start_key)
	   << " to " << pretty_binary_string(temp_end_key)
	   << " and " << pretty_binary_string(start_key)
	   << " to " << pretty_binary_string(end_key)
	   << " start " << start << dendl;
  it = db->get_iterator(PREFIX_OBJ);
  if (start == ghobject_t() || start == cid.get_min_hobj()) {
    it->upper_bound(temp_start_key);
    temp = true;
  } else {
    string k;
    get_object_key(cct, start, &k);
    if (start.hobj.is_temp()) {
      temp = true;
      assert(k >= temp_start_key && k < temp_end_key);
    } else {
      temp = false;
      assert(k >= start_key && k < end_key);
    }
    dout(20) << " start from " << pretty_binary_string(k)
	     << " temp=" << (int)temp << dendl;
    it->lower_bound(k);
  }
  if (end.hobj.is_max()) {
    pend = temp ? temp_end_key : end_key;
  } else {
    get_object_key(cct, end, &end_key);
    if (end.hobj.is_temp()) {
      if (temp)
	pend = end_key;
      else
	goto out;
    } else {
      pend = temp ? temp_end_key : end_key;
    }
  }
  dout(20) << __func__ << " pend " << pretty_binary_string(pend) << dendl;
  while (true) {
    if (!it->valid() || it->key() > pend) {
      if (!it->valid())
	dout(20) << __func__ << " iterator not valid (end of db?)" << dendl;
      else
	dout(20) << __func__ << " key " << pretty_binary_string(it->key())
		 << " > " << end << dendl;
      if (temp) {
	if (end.hobj.is_temp()) {
	  break;
	}
	dout(30) << __func__ << " switch to non-temp namespace" << dendl;
	temp = false;
	it->upper_bound(start_key);
	pend = end_key;
	dout(30) << __func__ << " pend " << pretty_binary_string(pend) << dendl;
	continue;
      }
      break;
    }
    dout(20) << __func__ << " key " << pretty_binary_string(it->key()) << dendl;
    ghobject_t oid;
    int r = get_key_object(it->key(), &oid);
    assert(r == 0);
    if (ls->size() >= (unsigned)max) {
      dout(20) << __func__ << " reached max " << max << dendl;
      *pnext = oid;
      set_next = true;
      break;
    }
    ls->push_back(oid);
    it->next();
  }
  if (!set_next) {
    *pnext = ghobject_t::get_max();
  }
 out:
  dout(10) << __func__ << " " << cid
	   << " start " << start << " end " << end << " max " << max
	   << " = " << r << ", ls.size() = " << ls->size()
	   << ", next = " << *pnext << dendl;
  return r;
}

// omap reads

NewStore::OmapIteratorImpl::OmapIteratorImpl(CollectionRef c, OnodeRef o, KeyValueDB::Iterator it)
  : c(c), o(o), it(it)
{
  RWLock::RLocker l(c->lock);
  if (o->onode.omap_head) {
    get_omap_header(o->onode.omap_head, &head);
    get_omap_tail(o->onode.omap_head, &tail);
    it->lower_bound(head);
  }
}

int NewStore::OmapIteratorImpl::seek_to_first()
{
  RWLock::RLocker l(c->lock);
  if (o->onode.omap_head) {
    it->lower_bound(head);
  } else {
    it = KeyValueDB::Iterator();
  }
  return 0;
}

int NewStore::OmapIteratorImpl::upper_bound(const string& after)
{
  RWLock::RLocker l(c->lock);
  if (o->onode.omap_head) {
    string key;
    get_omap_key(o->onode.omap_head, after, &key);
    it->upper_bound(key);
  } else {
    it = KeyValueDB::Iterator();
  }
  return 0;
}

int NewStore::OmapIteratorImpl::lower_bound(const string& to)
{
  RWLock::RLocker l(c->lock);
  if (o->onode.omap_head) {
    string key;
    get_omap_key(o->onode.omap_head, to, &key);
    it->lower_bound(key);
  } else {
    it = KeyValueDB::Iterator();
  }
  return 0;
}

bool NewStore::OmapIteratorImpl::valid()
{
  RWLock::RLocker l(c->lock);
  if (o->onode.omap_head && it->valid() && it->raw_key().second <= tail) {
    return true;
  } else {
    return false;
  }
}

int NewStore::OmapIteratorImpl::next(bool validate)
{
  RWLock::RLocker l(c->lock);
  if (o->onode.omap_head) {
    it->next();
    return 0;
  } else {
    return -1;
  }
}

string NewStore::OmapIteratorImpl::key()
{
  RWLock::RLocker l(c->lock);
  assert(it->valid());
  string db_key = it->raw_key().second;
  string user_key;
  decode_omap_key(db_key, &user_key);
  return user_key;
}

bufferlist NewStore::OmapIteratorImpl::value()
{
  RWLock::RLocker l(c->lock);
  assert(it->valid());
  return it->value();
}

int NewStore::omap_get(
  const coll_t& cid,           ///< [in] Collection containing oid
  const ghobject_t &oid,       ///< [in] Object containing omap
  bufferlist *header,          ///< [out] omap header
  map<string, bufferlist> *out /// < [out] Key to value map
  )
{
  dout(15) << __func__ << " " << cid << " oid " << oid << dendl;
  CollectionRef c = _get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::RLocker l(c->lock);
  int r = 0;
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }
  if (!o->onode.omap_head)
    goto out;
  o->flush();
  {
    KeyValueDB::Iterator it = db->get_iterator(PREFIX_OMAP);
    string head, tail;
    get_omap_header(o->onode.omap_head, &head);
    get_omap_tail(o->onode.omap_head, &tail);
    it->lower_bound(head);
    while (it->valid()) {
      if (it->key() == head) {
	dout(30) << __func__ << "  got header" << dendl;
	*header = it->value();
      } else if (it->key() >= tail) {
	dout(30) << __func__ << "  reached tail" << dendl;
	break;
      } else {
	string user_key;
	decode_omap_key(it->key(), &user_key);
	dout(30) << __func__ << "  got " << pretty_binary_string(it->key())
		 << " -> " << user_key << dendl;
	assert(it->key() < tail);
	(*out)[user_key] = it->value();
      }
      it->next();
    }
  }
 out:
  dout(10) << __func__ << " " << cid << " oid " << oid << " = " << r << dendl;
  return r;
}

int NewStore::omap_get_header(
  const coll_t& cid,       ///< [in] Collection containing oid
  const ghobject_t &oid,   ///< [in] Object containing omap
  bufferlist *header,      ///< [out] omap header
  bool allow_eio           ///< [in] don't assert on eio
  )
{
  dout(15) << __func__ << " " << cid << " oid " << oid << dendl;
  CollectionRef c = _get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::RLocker l(c->lock);
  int r = 0;
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }
  if (!o->onode.omap_head)
    goto out;
  o->flush();
  {
    string head;
    get_omap_header(o->onode.omap_head, &head);
    if (db->get(PREFIX_OMAP, head, header) >= 0) {
      dout(30) << __func__ << "  got header" << dendl;
    } else {
      dout(30) << __func__ << "  no header" << dendl;
    }
  }
 out:
  dout(10) << __func__ << " " << cid << " oid " << oid << " = " << r << dendl;
  return r;
}

int NewStore::omap_get_keys(
  const coll_t& cid,     ///< [in] Collection containing oid
  const ghobject_t &oid, ///< [in] Object containing omap
  set<string> *keys      ///< [out] Keys defined on oid
  )
{
  dout(15) << __func__ << " " << cid << " oid " << oid << dendl;
  CollectionRef c = _get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::RLocker l(c->lock);
  int r = 0;
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }
  if (!o->onode.omap_head)
    goto out;
  o->flush();
  {
    KeyValueDB::Iterator it = db->get_iterator(PREFIX_OMAP);
    string head, tail;
    get_omap_header(o->onode.omap_head, &head);
    get_omap_tail(o->onode.omap_head, &tail);
    it->lower_bound(head);
    while (it->valid()) {
      if (it->key() == head) {
	dout(30) << __func__ << "  skipping head" << dendl;
	it->next();
	continue;
      }
      if (it->key() >= tail) {
	dout(30) << __func__ << "  reached tail" << dendl;
	break;
      }
      string user_key;
      decode_omap_key(it->key(), &user_key);
      dout(30) << __func__ << "  got " << pretty_binary_string(it->key())
	       << " -> " << user_key << dendl;
      assert(it->key() < tail);
      keys->insert(user_key);
      it->next();
    }
  }
 out:
  dout(10) << __func__ << " " << cid << " oid " << oid << " = " << r << dendl;
  return r;
}

int NewStore::omap_get_values(
  const coll_t& cid,           ///< [in] Collection containing oid
  const ghobject_t &oid,       ///< [in] Object containing omap
  const set<string> &keys,     ///< [in] Keys to get
  map<string, bufferlist> *out ///< [out] Returned keys and values
  )
{
  dout(15) << __func__ << " " << cid << " oid " << oid << dendl;
  CollectionRef c = _get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::RLocker l(c->lock);
  int r = 0;
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }
  if (!o->onode.omap_head)
    goto out;
  o->flush();
  for (set<string>::const_iterator p = keys.begin(); p != keys.end(); ++p) {
    string key;
    get_omap_key(o->onode.omap_head, *p, &key);
    bufferlist val;
    if (db->get(PREFIX_OMAP, key, &val) >= 0) {
      dout(30) << __func__ << "  got " << pretty_binary_string(key)
	       << " -> " << *p << dendl;
      out->insert(make_pair(*p, val));
    }
  }
 out:
  dout(10) << __func__ << " " << cid << " oid " << oid << " = " << r << dendl;
  return r;
}

int NewStore::omap_check_keys(
  const coll_t& cid,       ///< [in] Collection containing oid
  const ghobject_t &oid,   ///< [in] Object containing omap
  const set<string> &keys, ///< [in] Keys to check
  set<string> *out         ///< [out] Subset of keys defined on oid
  )
{
  dout(15) << __func__ << " " << cid << " oid " << oid << dendl;
  CollectionRef c = _get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::RLocker l(c->lock);
  int r = 0;
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }
  if (!o->onode.omap_head)
    goto out;
  o->flush();
  for (set<string>::const_iterator p = keys.begin(); p != keys.end(); ++p) {
    string key;
    get_omap_key(o->onode.omap_head, *p, &key);
    bufferlist val;
    if (db->get(PREFIX_OMAP, key, &val) >= 0) {
      dout(30) << __func__ << "  have " << pretty_binary_string(key)
	       << " -> " << *p << dendl;
      out->insert(*p);
    } else {
      dout(30) << __func__ << "  miss " << pretty_binary_string(key)
	       << " -> " << *p << dendl;
    }
  }
 out:
  dout(10) << __func__ << " " << cid << " oid " << oid << " = " << r << dendl;
  return r;
}

ObjectMap::ObjectMapIterator NewStore::get_omap_iterator(
  const coll_t& cid,       ///< [in] collection
  const ghobject_t &oid    ///< [in] object
  )
{

  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = _get_collection(cid);
  if (!c) {
    dout(10) << __func__ << " " << cid << "doesn't exist" <<dendl;
    return ObjectMap::ObjectMapIterator();
  }
  RWLock::RLocker l(c->lock);
  OnodeRef o = c->get_onode(oid, false);
  if (!o) {
    dout(10) << __func__ << " " << oid << "doesn't exist" <<dendl;
    return ObjectMap::ObjectMapIterator();
  }
  o->flush();
  dout(10) << __func__ << " header = " << o->onode.omap_head <<dendl;
  KeyValueDB::Iterator it = db->get_iterator(PREFIX_OMAP);
  return ObjectMap::ObjectMapIterator(new OmapIteratorImpl(c, o, it));
}


// -----------------
// write helpers

int NewStore::_recover_next_nid()
{
  nid_max = 0;
  bufferlist bl;
  db->get(PREFIX_SUPER, "nid_max", &bl);
  try {
    ::decode(nid_max, bl);
  } catch (buffer::error& e) {
  }
  dout(1) << __func__ << " old nid_max " << nid_max << dendl;
  nid_last = nid_max;
  return 0;
}

void NewStore::_assign_nid(TransContext *txc, OnodeRef o)
{
  if (o->onode.nid)
    return;
  Mutex::Locker l(nid_lock);
  o->onode.nid = ++nid_last;
  dout(20) << __func__ << " " << o->onode.nid << dendl;
  if (nid_last > nid_max) {
#warning fixme this could race?
    nid_max += cct->_conf->get_val<uint64_t>("newstore_nid_prealloc");
    bufferlist bl;
    ::encode(nid_max, bl);
    txc->t->set(PREFIX_SUPER, "nid_max", bl);
    dout(10) << __func__ << " nid_max now " << nid_max << dendl;
  }
}

int NewStore::_recover_next_fid()
{
  bufferlist bl;
  db->get(PREFIX_SUPER, "fid_max", &bl);
  try {
    ::decode(fid_max, bl);
  } catch (buffer::error& e) {
  }
  dout(1) << __func__ << " old fid_max " << fid_max << dendl;
  fid_last = fid_max;

  if (fid_last.fset > 0) {
    char s[32];
    snprintf(s, sizeof(s), "%u", fid_last.fset);
    assert(fset_fd < 0);
    fset_fd = ::openat(frag_fd, s, O_DIRECTORY, 0644);
    if (fset_fd < 0) {
      int r = -errno;
      derr << __func__ << " cannot open created " << path << "/fragments/"
	 << s << ": " << cpp_strerror(r) << dendl;
      return r;
    }
  }

  return 0;
}

int NewStore::_open_fid(fid_t fid, unsigned flags)
{
  if (fid.handle.length() && cct->_conf->get_val<bool>("newstore_open_by_handle")) {
    int fd = fs->open_handle(path_fd, fid.handle, flags);
    if (fd >= 0) {
      dout(30) << __func__ << " " << fid << " = " << fd
	       << " (open by handle)" << dendl;
      return fd;
    }
    int err = -errno;
    dout(30) << __func__ << " " << fid << " = " << cpp_strerror(err)
	     << " (with open by handle, falling back to file name)" << dendl;
  }

  char fn[32];
  snprintf(fn, sizeof(fn), "%u/%u", fid.fset, fid.fno);
  int fd = ::openat(frag_fd, fn, flags);
  if (fd < 0) {
    int r = -errno;
    derr << __func__ << " on " << fid << ": " << cpp_strerror(r) << dendl;
    return r;
  }
  dout(30) << __func__ << " " << fid << " = " << fd << dendl;
  return fd;
}

int NewStore::_create_fid(TransContext *txc, OnodeRef o,
			  fid_t *fid, unsigned flags)
{
  {
    Mutex::Locker l(fid_lock);
    if (fid_last.fset > 0 &&
	fid_last.fno > 0 &&
	fid_last.fset == fid_max.fset &&
	fid_last.fno < cct->_conf->get_val<uint64_t>("newstore_max_dir_size")) {
      ++fid_last.fno;
      if (fid_last.fno >= fid_max.fno) {
	// raise fid_max, same fset, capping to max_dir_size
	fid_max.fno = min(fid_max.fno + cct->_conf->get_val<uint64_t>("newstore_fid_prealloc"),
			  cct->_conf->get_val<uint64_t>("newstore_max_dir_size"));
	assert(fid_max.fno >= fid_last.fno);
	bufferlist bl;
	::encode(fid_max, bl);
	txc->t->set(PREFIX_SUPER, "fid_max", bl);
	dout(10) << __func__ << " fid_max now " << fid_max << dendl;
      }
    } else {
      // new fset
      ++fid_last.fset;
      fid_last.fno = 1;
      dout(10) << __func__ << " creating " << fid_last.fset << dendl;
      char s[32];
      snprintf(s, sizeof(s), "%u", fid_last.fset);
      int r = ::mkdirat(frag_fd, s, 0755);
      if (r < 0) {
	r = -errno;
	derr << __func__ << " cannot create " << path << "/fragments/"
	     << s << ": " << cpp_strerror(r) << dendl;
	return r;
      }
      if (fset_fd >= 0)
	VOID_TEMP_FAILURE_RETRY(::close(fset_fd));
      fset_fd = ::openat(frag_fd, s, O_DIRECTORY, 0644);
      if (fset_fd < 0) {
	r = -errno;
	derr << __func__ << " cannot open created " << path << "/fragments/"
	     << s << ": " << cpp_strerror(r) << dendl;
      }

      fid_max = fid_last;
      fid_max.fno = cct->_conf->get_val<uint64_t>("newstore_fid_prealloc");
      bufferlist bl;
      ::encode(fid_max, bl);
      txc->t->set(PREFIX_SUPER, "fid_max", bl);
      dout(10) << __func__ << " fid_max now " << fid_max << dendl;
    }
    *fid = fid_last;
  }

  dout(10) << __func__ << " " << fid_last << dendl;
  char s[32];
  snprintf(s, sizeof(s), "%u", fid->fno);
  int fd = ::openat(fset_fd, s, flags | O_CREAT, 0644);
  if (fd < 0) {
    int r = -errno;
    derr << __func__ << " cannot create " << path << "/fragments/"
	 << *fid << ": " << cpp_strerror(r) << dendl;
    return r;
  }

  if (o->onode.expected_object_size) {
    unsigned hint = MIN(o->onode.expected_object_size,
			o->onode.frag_size);
    dout(20) << __func__ << " set alloc hint to " << hint << dendl;
    fs->set_alloc_hint(fd, hint);
  }

  if (cct->_conf->get_val<bool>("newstore_open_by_handle")) {
    int r = fs->get_handle(fd, &fid->handle);
    if (r < 0) {
      dout(30) << __func__ << " get_handle got " << cpp_strerror(r) << dendl;
    } else {
      dout(30) << __func__ << " got handle: ";
      bufferlist bl;
      bl.append(fid->handle);
      bl.hexdump(*_dout);
      *_dout << dendl;
    }
  }

  dout(30) << __func__ << " " << *fid << " = " << fd << dendl;
  return fd;
}

int NewStore::_remove_fid(fid_t fid)
{
  char fn[32];
  snprintf(fn, sizeof(fn), "%u/%u", fid.fset, fid.fno);
  int r = ::unlinkat(frag_fd, fn, 0);
  if (r < 0)
    return -errno;
  return 0;
}

NewStore::TransContext *NewStore::_txc_create(OpSequencer *osr)
{
  TransContext *txc = new TransContext(osr);
  txc->t = db->get_transaction();
  osr->queue_new(txc);
  dout(20) << __func__ << " osr " << osr << " = " << txc << dendl;
  return txc;
}

void NewStore::_txc_state_proc(TransContext *txc)
{
  while (true) {
    dout(10) << __func__ << " txc " << txc
	     << " " << txc->get_state_name() << dendl;
    switch (txc->state) {
    case TransContext::STATE_PREPARE:
      if (!txc->pending_aios.empty()) {
	txc->state = TransContext::STATE_AIO_WAIT;
	_txc_aio_submit(txc);
	return;
      }
      // ** fall-thru **

    case TransContext::STATE_AIO_WAIT:
      if (!txc->sync_items.empty()) {
	txc->state = TransContext::STATE_FSYNC_WAIT;
	if (!cct->_conf->get_val<bool>("newstore_sync_io")) {
	  _txc_queue_fsync(txc);
	  return;
	}
	_txc_do_sync_fsync(txc);
      }
      _txc_finish_io(txc);  // may trigger blocked txc's too
      return;

    case TransContext::STATE_IO_DONE:
      assert(txc->osr->qlock.is_locked());  // see _txc_finish_io
      txc->state = TransContext::STATE_KV_QUEUED;
      if (!cct->_conf->get_val<bool>("newstore_sync_transaction")) {
	Mutex::Locker l(kv_lock);
	if (cct->_conf->get_val<bool>("newstore_sync_submit_transaction")) {
	  db->submit_transaction(txc->t);
	}
	kv_queue.push_back(txc);
	kv_cond.SignalOne();
	return;
      }
      db->submit_transaction_sync(txc->t);
      break;

    case TransContext::STATE_KV_QUEUED:
      txc->state = TransContext::STATE_KV_DONE;
      _txc_finish_kv(txc);
      // ** fall-thru **

    case TransContext::STATE_KV_DONE:
      if (txc->wal_txn) {
	txc->state = TransContext::STATE_WAL_QUEUED;
	if (cct->_conf->get_val<bool>("newstore_sync_wal_apply")) {
	  _wal_apply(txc);
	} else {
	  wal_wq.queue(txc);
	}
	return;
      }
      txc->state = TransContext::STATE_FINISHING;
      break;

    case TransContext::STATE_WAL_APPLYING:
      if (!txc->pending_aios.empty()) {
	txc->state = TransContext::STATE_WAL_AIO_WAIT;
	_txc_aio_submit(txc);
	return;
      }
      // ** fall-thru **

    case TransContext::STATE_WAL_AIO_WAIT:
      _wal_finish(txc);
      return;

    case TransContext::STATE_WAL_CLEANUP:
      txc->state = TransContext::STATE_FINISHING;
      // ** fall-thru **

    case TransContext::TransContext::STATE_FINISHING:
      _txc_finish(txc);
      return;

    default:
      derr << __func__ << " unexpected txc " << txc
	   << " state " << txc->get_state_name() << dendl;
      assert(0 == "unexpected txc state");
      return;
    }
  }
}

void NewStore::_txc_process_fsync(fsync_item *i)
{
  dout(20) << __func__ << " txc " << i->txc << dendl;
  int r = ::fdatasync(i->fd);
  if (r < 0) {
    r = -errno;
    derr << __func__ << " error from fdatasync on " << i->fd
	 << " txc " << i->txc
	 << ": " << cpp_strerror(r) << dendl;
    assert(0 == "error from fdatasync");
  }
  VOID_TEMP_FAILURE_RETRY(::close(i->fd));
  if (i->txc->finish_fsync()) {
    _txc_finish_io(i->txc);
  }
  dout(20) << __func__ << " txc " << i->txc << " done" << dendl;
}

void NewStore::_txc_finish_io(TransContext *txc)
{
  dout(20) << __func__ << " " << txc << dendl;

  /*
   * we need to preserve the order of kv transactions,
   * even though fsyncs will complete in any order.
   */

  OpSequencer *osr = txc->osr.get();
  Mutex::Locker l(osr->qlock);
  txc->state = TransContext::STATE_IO_DONE;

  OpSequencer::q_list_t::iterator p = osr->q.iterator_to(*txc);
  while (p != osr->q.begin()) {
    --p;
    if (p->state < TransContext::STATE_IO_DONE) {
      dout(20) << __func__ << " " << txc << " blocked by " << &*p << " "
	       << p->get_state_name() << dendl;
      return;
    }
    if (p->state > TransContext::STATE_IO_DONE) {
      ++p;
      break;
    }
  }
  do {
    _txc_state_proc(&*p++);
  } while (p != osr->q.end() &&
	   p->state == TransContext::STATE_IO_DONE);
}

int NewStore::_txc_finalize(OpSequencer *osr, TransContext *txc)
{
  dout(20) << __func__ << " osr " << osr << " txc " << txc
	   << " onodes " << txc->onodes << dendl;

  // finalize onodes
  for (set<OnodeRef>::iterator p = txc->onodes.begin();
       p != txc->onodes.end();
       ++p) {
    bufferlist bl;
    ::encode((*p)->onode, bl);
    dout(20) << " onode size is " << bl.length() << dendl;
    txc->t->set(PREFIX_OBJ, (*p)->key, bl);

    Mutex::Locker l((*p)->flush_lock);
    (*p)->flush_txns.insert(txc);
  }

  // journal wal items
  if (txc->wal_txn) {
    txc->wal_txn->seq = wal_seq++;
    bufferlist bl;
    ::encode(*txc->wal_txn, bl);
    string key;
    get_wal_key(txc->wal_txn->seq, &key);
    txc->t->set(PREFIX_WAL, key, bl);
  }

  return 0;
}

void NewStore::_txc_queue_fsync(TransContext *txc)
{
  dout(20) << __func__ << " txc " << txc << dendl;
  fsync_wq.lock();
  for (list<fsync_item>::iterator p = txc->sync_items.begin();
       p != txc->sync_items.end();
       ++p) {
    fsync_wq._enqueue(&*p);
    fsync_wq._wake();
  }
  fsync_wq.unlock();
}

void NewStore::_txc_do_sync_fsync(TransContext *txc)
{
  dout(20) << __func__ << " txc " << txc << dendl;
  for (list<fsync_item>::iterator p = txc->sync_items.begin();
       p != txc->sync_items.end(); ++p) {
    dout(30) << __func__ << " fsync " << p->fd << dendl;
    int r = ::fdatasync(p->fd);
    if (r < 0) {
      r = -errno;
      derr << __func__ << " fsync: " << cpp_strerror(r) << dendl;
      assert(0 == "fsync error");
    }
    VOID_TEMP_FAILURE_RETRY(::close(p->fd));
  }
}

void NewStore::_txc_finish_kv(TransContext *txc)
{
  dout(20) << __func__ << " txc " << txc << dendl;

  // warning: we're calling onreadable_sync inside the sequencer lock
  if (txc->onreadable_sync) {
    txc->onreadable_sync->complete(0);
    txc->onreadable_sync = NULL;
  }
  if (txc->onreadable) {
    finisher.queue(txc->onreadable);
    txc->onreadable = NULL;
  }
  if (txc->oncommit) {
    finisher.queue(txc->oncommit);
    txc->oncommit = NULL;
  }
  while (!txc->oncommits.empty()) {
    finisher.queue(txc->oncommits.front());
    txc->oncommits.pop_front();
  }

  throttle_ops.put(txc->ops);
  throttle_bytes.put(txc->bytes);
}

void NewStore::_txc_finish(TransContext *txc)
{
  dout(20) << __func__ << " " << txc << " onodes " << txc->onodes << dendl;
  assert(txc->state == TransContext::STATE_FINISHING);

  for (set<OnodeRef>::iterator p = txc->onodes.begin();
       p != txc->onodes.end();
       ++p) {
    Mutex::Locker l((*p)->flush_lock);
    dout(20) << __func__ << " onode " << *p << " had " << (*p)->flush_txns
	     << dendl;
    assert((*p)->flush_txns.count(txc));
    (*p)->flush_txns.erase(txc);
    if ((*p)->flush_txns.empty())
      (*p)->flush_cond.Signal();
  }

  // clear out refs
  txc->onodes.clear();

  while (!txc->removed_collections.empty()) {
    _queue_reap_collection(txc->removed_collections.front());
    txc->removed_collections.pop_front();
  }

  throttle_wal_ops.put(txc->ops);
  throttle_wal_bytes.put(txc->bytes);

  OpSequencerRef osr = txc->osr;
  osr->qlock.Lock();
  txc->state = TransContext::STATE_DONE;
  osr->qlock.Unlock();

  _osr_reap_done(osr.get());
}

void NewStore::_osr_reap_done(OpSequencer *osr)
{
  Mutex::Locker l(osr->qlock);
  dout(20) << __func__ << " osr " << osr << dendl;
  while (!osr->q.empty()) {
    TransContext *txc = &osr->q.front();
    dout(20) << __func__ << "  txc " << txc << " " << txc->get_state_name()
	     << dendl;
    if (txc->state != TransContext::STATE_DONE) {
      break;
    }

    if (txc->first_collection) {
      txc->first_collection->onode_map.trim(cct->_conf->get_val<uint64_t>("newstore_onode_map_size"));
    }

    osr->q.pop_front();
    delete txc;
    osr->qcond.Signal();
  }
}

void NewStore::_aio_thread()
{
  dout(10) << __func__ << " start" << dendl;
  while (!aio_stop) {
    dout(40) << __func__ << " polling" << dendl;
    int max = 16;
    aio_t *aio[max];
    int r = aio_queue.get_next_completed(cct->_conf->get_val<uint64_t>("newstore_aio_poll_ms"),
					 aio, max);
    if (r < 0) {
      derr << __func__ << " got " << cpp_strerror(r) << dendl;
    }
    if (r > 0) {
      dout(30) << __func__ << " got " << r << " completed aios" << dendl;
      for (int i = 0; i < r; ++i) {
	TransContext *txc = static_cast<TransContext*>(aio[i]->priv);
	int left = txc->num_aio--;
	dout(10) << __func__ << " finished aio " << aio[i] << " txc " << txc
		 << " state " << txc->get_state_name() << ", "
		 << left << " aios left" << dendl;
	VOID_TEMP_FAILURE_RETRY(::close(aio[i]->fd));
	if (left == 0) {
	  _txc_state_proc(txc);
	}
      }
    }
  }
  dout(10) << __func__ << " end" << dendl;
}

void NewStore::_kv_sync_thread()
{
  dout(10) << __func__ << " start" << dendl;
  kv_lock.Lock();
  while (true) {
    assert(kv_committing.empty());
    assert(wal_cleaning.empty());
    if (kv_queue.empty() && wal_cleanup_queue.empty()) {
      if (kv_stop)
	break;
      dout(20) << __func__ << " sleep" << dendl;
      kv_sync_cond.Signal();
      kv_cond.Wait(kv_lock);
      dout(20) << __func__ << " wake" << dendl;
    } else {
      dout(20) << __func__ << " committing " << kv_queue.size()
	       << " cleaning " << wal_cleanup_queue.size() << dendl;
      kv_committing.swap(kv_queue);
      wal_cleaning.swap(wal_cleanup_queue);
      utime_t start = ceph_clock_now();
      kv_lock.Unlock();

      if (!cct->_conf->get_val<bool>("newstore_sync_submit_transaction")) {
	for (std::deque<TransContext *>::iterator it = kv_committing.begin();
	     it != kv_committing.end();
	     ++it) {
	  db->submit_transaction((*it)->t);
	}
      }

      // one transaction to force a sync.  clean up wal keys while we
      // are at it.
      KeyValueDB::Transaction txc_cleanup_sync = db->get_transaction();
      for (std::deque<TransContext *>::iterator it = wal_cleaning.begin();
	    it != wal_cleaning.end();
	    ++it) {
	wal_transaction_t& wt =*(*it)->wal_txn;
	// cleanup the data in overlays
	for (list<wal_op_t>::iterator p = wt.ops.begin(); p != wt.ops.end(); ++p) {
	  for (vector<overlay_t>::iterator q = p->overlays.begin();
               q != p->overlays.end(); ++q) {
            string key;
            get_overlay_key(p->nid, q->key, &key);
	    txc_cleanup_sync->rmkey(PREFIX_OVERLAY, key);
	  }
	}
	// cleanup the shared overlays. this may double delete something we
	// did above, but that's less work than doing careful ref counting
	// of the overlay key/value pairs.
	for (vector<string>::iterator p = wt.shared_overlay_keys.begin();
             p != wt.shared_overlay_keys.end(); ++p) {
	  txc_cleanup_sync->rmkey(PREFIX_OVERLAY, *p);
	}
	// cleanup the wal
	string key;
	get_wal_key(wt.seq, &key);
	txc_cleanup_sync->rmkey(PREFIX_WAL, key);
      }
      db->submit_transaction_sync(txc_cleanup_sync);
      utime_t finish = ceph_clock_now();
      utime_t dur = finish - start;
      dout(20) << __func__ << " committed " << kv_committing.size()
	       << " cleaned " << wal_cleaning.size()
	       << " in " << dur << dendl;
      while (!kv_committing.empty()) {
	TransContext *txc = kv_committing.front();
	_txc_state_proc(txc);
	kv_committing.pop_front();
      }
      while (!wal_cleaning.empty()) {
	TransContext *txc = wal_cleaning.front();
	_txc_state_proc(txc);
	wal_cleaning.pop_front();
      }

      // this is as good a place as any ...
      _reap_collections();

      kv_lock.Lock();
    }
  }
  kv_lock.Unlock();
  dout(10) << __func__ << " finish" << dendl;
}

wal_op_t *NewStore::_get_wal_op(TransContext *txc)
{
  if (!txc->wal_txn) {
    txc->wal_txn = new wal_transaction_t;
  }
  txc->wal_txn->ops.push_back(wal_op_t());
  return &txc->wal_txn->ops.back();
}

int NewStore::_wal_apply(TransContext *txc)
{
  wal_transaction_t& wt = *txc->wal_txn;
  dout(20) << __func__ << " txc " << txc << " seq " << wt.seq << dendl;
  txc->state = TransContext::STATE_WAL_APPLYING;

  assert(txc->pending_aios.empty());
  int r = _do_wal_transaction(wt, txc);
  assert(r == 0);

  _txc_state_proc(txc);
  return 0;
}

int NewStore::_wal_finish(TransContext *txc)
{
  wal_transaction_t& wt = *txc->wal_txn;
  dout(20) << __func__ << " txc " << " seq " << wt.seq << txc << dendl;

  Mutex::Locker l(kv_lock);
  txc->state = TransContext::STATE_WAL_CLEANUP;
  wal_cleanup_queue.push_back(txc);
  kv_cond.SignalOne();
  return 0;
}

int NewStore::_do_wal_transaction(wal_transaction_t& wt,
				  TransContext *txc)
{
  vector<int> sync_fds;
  sync_fds.reserve(wt.ops.size());

  // read all the overlay data first for apply
  _do_read_all_overlays(wt);

  for (list<wal_op_t>::iterator p = wt.ops.begin(); p != wt.ops.end(); ++p) {
    switch (p->op) {
    case wal_op_t::OP_WRITE:
      {
	dout(20) << __func__ << " write " << p->fid << " "
		 << p->offset << "~" << p->length << dendl;
	unsigned flags = O_RDWR;
	if (cct->_conf->get_val<bool>("newstore_o_direct") &&
	    (p->offset & ~CEPH_PAGE_MASK) == 0 &&
	    (p->length & ~CEPH_PAGE_MASK) == 0) {
	  dout(20) << __func__ << " page-aligned io, using O_DIRECT, "
		   << p->data.buffers().size() << " buffers" << dendl;
	  flags |= O_DIRECT | O_DSYNC;
	  if (!p->data.is_page_aligned()) {
	    dout(20) << __func__ << " rebuilding buffer to be page-aligned"
		     << dendl;
	    p->data.rebuild();
	  }
	}
	int fd = _open_fid(p->fid, flags);
	if (fd < 0)
	  return fd;
#ifdef HAVE_LIBAIO
	if (cct->_conf->get_val<bool>("newstore_aio") && txc && (flags & O_DIRECT)) {
	  txc->pending_aios.push_back(aio_t(txc, fd));
	  aio_t& aio = txc->pending_aios.back();
	  p->data.prepare_iov(&aio.iov);
	  aio.pwritev(p->offset, p->length);
	  dout(2) << __func__ << " prepared aio " << &aio << dendl;
	} else
#endif
	{
	  int r = ::lseek64(fd, p->offset, SEEK_SET);
	  if (r < 0) {
	    r = -errno;
	    derr << __func__ << " lseek64 on " << fd << " got: "
		 << cpp_strerror(r) << dendl;
	    return r;
	  }
	  r = p->data.write_fd(fd);
	  if (r < 0) {
	    derr << __func__ << " write_fd on " << fd << " got: "
		 << cpp_strerror(r) << dendl;
	    return r;
	  }
	  if (!(flags & O_DIRECT))
	    sync_fds.push_back(fd);
	  else
	    VOID_TEMP_FAILURE_RETRY(::close(fd));
	}
      }
      break;
    case wal_op_t::OP_ZERO:
      {
	dout(20) << __func__ << " zero " << p->fid << " "
		 << p->offset << "~" << p->length << dendl;
	int fd = _open_fid(p->fid, O_RDWR);
	if (fd < 0)
	  return fd;
	int r = fs->zero(fd, p->offset, p->length);
	if (r < 0) {
	  derr << __func__ << " zero on " << fd << " got: "
	       << cpp_strerror(r) << dendl;
	  return r;
	}
	// FIXME: do aio fdatasync?
	sync_fds.push_back(fd);
      }
      break;
    case wal_op_t::OP_TRUNCATE:
      {
	dout(20) << __func__ << " truncate " << p->fid << " "
		 << p->offset << dendl;
	int fd = _open_fid(p->fid, O_RDWR);
	if (fd < 0)
	  return fd;
	int r = ::ftruncate(fd, p->offset);
	if (r < 0) {
	  r = -errno;
	  derr << __func__ << " truncate on " << fd << " got: "
	       << cpp_strerror(r) << dendl;
	  return r;
	}
	// note: we are not syncing this truncate.  instead, we are
	// careful about only reading as much of the fragment as we
	// know is valid, and truncating to expected size before
	// extending the file.
      }
      break;

    case wal_op_t::OP_REMOVE:
      dout(20) << __func__ << " remove " << p->fid << dendl;
      _remove_fid(p->fid);
      // note: we do not fsync the directory.  instead, we tolerate
      // leaked fragments in a crash.  in practice, this will be
      // exceedingly rare.
      break;

    default:
      assert(0 == "unrecognized wal op");
    }
  }

  for (vector<int>::iterator p = sync_fds.begin();
       p != sync_fds.end();
       ++p) {
    int r = ::fdatasync(*p);
    assert(r == 0);
    VOID_TEMP_FAILURE_RETRY(::close(*p));
  }

  return 0;
}

int NewStore::_wal_replay()
{
  dout(10) << __func__ << " start" << dendl;
  KeyValueDB::Iterator it = db->get_iterator(PREFIX_WAL);
  it->lower_bound(string());
  KeyValueDB::Transaction cleanup = db->get_transaction();
  int count = 0;
  while (it->valid()) {
    bufferlist bl = it->value();
    bufferlist::iterator p = bl.begin();
    wal_transaction_t wt;
    try {
      ::decode(wt, p);
    } catch (buffer::error& e) {
      derr << __func__ << " failed to decode wal txn " << it->key() << dendl;
      return -EIO;
    }

    // Get the overlay data of the WAL for replay
    _do_read_all_overlays(wt);
    dout(20) << __func__ << " replay " << it->key() << dendl;
    int r = _do_wal_transaction(wt, NULL);  // don't bother with aio here
    if (r < 0)
      return r;
    cleanup->rmkey(PREFIX_WAL, it->key());
    ++count;
    it->next();
  }
  if (count) {
    dout(10) << __func__ << " cleanup" << dendl;
    db->submit_transaction_sync(cleanup);
  }
  dout(10) << __func__ << " completed " << count << " events" << dendl;
  return 0;
}

// ---------------------------
// transactions

int NewStore::queue_transactions(
    Sequencer *posr,
    vector<Transaction>& tls,
    TrackedOpRef op,
    ThreadPool::TPHandle *handle)
{
  Context *onreadable;
  Context *ondisk;
  Context *onreadable_sync;
  ObjectStore::Transaction::collect_contexts(
    tls, &onreadable, &ondisk, &onreadable_sync);
  int r;

  // set up the sequencer
  OpSequencer *osr;
  assert(posr);
  if (posr->p) {
    osr = static_cast<OpSequencer *>(posr->p.get());
    dout(5) << __func__ << " existing " << osr << " " << *osr << dendl;
  } else {
    osr = new OpSequencer(cct);
    osr->parent = posr;
    posr->p = osr;
    dout(5) << __func__ << " new " << osr << " " << *osr << dendl;
  }

  // prepare
  TransContext *txc = _txc_create(osr);
  txc->onreadable = onreadable;
  txc->onreadable_sync = onreadable_sync;
  txc->oncommit = ondisk;

  for (vector<Transaction>::iterator p = tls.begin(); p != tls.end(); ++p) {
    (*p).set_osr(osr);
    txc->ops += (*p).get_num_ops();
    txc->bytes += (*p).get_num_bytes();
    _txc_add_transaction(txc, &(*p));
  }

  r = _txc_finalize(osr, txc);
  assert(r == 0);

  throttle_ops.get(txc->ops);
  throttle_bytes.get(txc->bytes);
  throttle_wal_ops.get(txc->ops);
  throttle_wal_bytes.get(txc->bytes);

  // execute (start)
  _txc_state_proc(txc);
  return 0;
}

void NewStore::_txc_aio_submit(TransContext *txc)
{
  int num = txc->pending_aios.size();
  dout(10) << __func__ << " txc " << txc << " submitting " << num << dendl;
  assert(num > 0);
  txc->num_aio = num;

  // move these aside, and get our end iterator position now, as the
  // aios might complete as soon as they are submitted and queue more
  // wal aio's.
  list<aio_t>::iterator e = txc->submitted_aios.begin();
  txc->submitted_aios.splice(e, txc->pending_aios);
  list<aio_t>::iterator p = txc->submitted_aios.begin();
  assert(p != e);
  bool done = false;
  while (!done) {
    aio_t& aio = *p;
    dout(20) << __func__ << " aio " << &aio << " fd " << aio.fd << dendl;
    for (auto& iov : aio.iov) 
//    for (vector<iovec>::iterator q = aio.iov.begin(); q != aio.iov.end(); ++q)
      dout(30) << __func__ << "  iov " << (void*)iov.iov_base
	       << " len " << iov.iov_len << dendl;
    dout(30) << " fd " << aio.fd << " offset " << lseek64(aio.fd, 0, SEEK_CUR)
	     << dendl;

    // be careful: as soon as we submit aio we race with completion.
    // since we are holding a ref take care not to dereference txc at
    // all after that point.
    list<aio_t>::iterator cur = p;
    ++p;
    done = (p == e);

    // do not dereference txc (or it's contents) after we submit (if
    // done == true and we don't loop)
    int retries = 0;
    int r = aio_queue.submit(*cur, &retries);
    if (retries)
      derr << __func__ << " retries " << retries << dendl;
    if (r) {
      derr << " aio submit got " << cpp_strerror(r) << dendl;
      assert(r == 0);
    }
  }
}

int NewStore::_txc_add_transaction(TransContext *txc, Transaction *t)
{
  Transaction::iterator i = t->begin();
  int pos = 0;

  vector<CollectionRef> cvec(i.colls.size());
  unsigned j = 0;
  for (vector<coll_t>::iterator p = i.colls.begin(); p != i.colls.end();
       ++p, ++j) {
    cvec[j] = _get_collection(*p);

    // note first collection we reference
    if (!j && !txc->first_collection)
      txc->first_collection = cvec[j];
  }

  while (i.have_op()) {
    Transaction::Op *op = i.decode_op();
    int r = 0;
    CollectionRef &c = cvec[op->cid];

    switch (op->op) {
    case Transaction::OP_NOP:
      break;
    case Transaction::OP_TOUCH:
      {
        const ghobject_t &oid = i.get_oid(op->oid);
	r = _touch(txc, c, oid);
      }
      break;

    case Transaction::OP_WRITE:
      {
        const ghobject_t &oid = i.get_oid(op->oid);
        uint64_t off = op->off;
        uint64_t len = op->len;
	uint32_t fadvise_flags = i.get_fadvise_flags();
        bufferlist bl;
        i.decode_bl(bl);
	r = _write(txc, c, oid, off, len, bl, fadvise_flags);
      }
      break;

    case Transaction::OP_ZERO:
      {
        const ghobject_t &oid = i.get_oid(op->oid);
        uint64_t off = op->off;
        uint64_t len = op->len;
	r = _zero(txc, c, oid, off, len);
      }
      break;

    case Transaction::OP_TRIMCACHE:
      {
        // deprecated, no-op
      }
      break;

    case Transaction::OP_TRUNCATE:
      {
        const ghobject_t& oid = i.get_oid(op->oid);
        uint64_t off = op->off;
	r = _truncate(txc, c, oid, off);
      }
      break;

    case Transaction::OP_REMOVE:
      {
        const ghobject_t& oid = i.get_oid(op->oid);
	r = _remove(txc, c, oid);
      }
      break;

    case Transaction::OP_SETATTR:
      {
        const ghobject_t &oid = i.get_oid(op->oid);
        string name = i.decode_string();
        bufferlist bl;
        i.decode_bl(bl);
	map<string, bufferptr> to_set;
	to_set[name] = bufferptr(bl.c_str(), bl.length());
	r = _setattrs(txc, c, oid, to_set);
      }
      break;

    case Transaction::OP_SETATTRS:
      {
        const ghobject_t& oid = i.get_oid(op->oid);
        map<string, bufferptr> aset;
        i.decode_attrset(aset);
	r = _setattrs(txc, c, oid, aset);
      }
      break;

    case Transaction::OP_RMATTR:
      {
        const ghobject_t &oid = i.get_oid(op->oid);
	string name = i.decode_string();
	r = _rmattr(txc, c, oid, name);
      }
      break;

    case Transaction::OP_RMATTRS:
      {
        const ghobject_t &oid = i.get_oid(op->oid);
	r = _rmattrs(txc, c, oid);
      }
      break;

    case Transaction::OP_CLONE:
      {
        const ghobject_t& oid = i.get_oid(op->oid);
        const ghobject_t& noid = i.get_oid(op->dest_oid);
	r = _clone(txc, c, oid, noid);
      }
      break;

    case Transaction::OP_CLONERANGE:
      assert(0 == "deprecated");
      break;

    case Transaction::OP_CLONERANGE2:
      {
        const ghobject_t &oid = i.get_oid(op->oid);
        const ghobject_t &noid = i.get_oid(op->dest_oid);
        uint64_t srcoff = op->off;
        uint64_t len = op->len;
        uint64_t dstoff = op->dest_off;
	r = _clone_range(txc, c, oid, noid, srcoff, len, dstoff);
      }
      break;

    case Transaction::OP_MKCOLL:
      {
	assert(!c);
        coll_t cid = i.get_cid(op->cid);
	r = _create_collection(txc, cid, op->split_bits, &c);
      }
      break;

    case Transaction::OP_COLL_HINT:
      {
        coll_t cid = i.get_cid(op->cid);
        uint32_t type = op->hint_type;
        bufferlist hint;
        i.decode_bl(hint);
        bufferlist::iterator hiter = hint.begin();
        if (type == Transaction::COLL_HINT_EXPECTED_NUM_OBJECTS) {
          uint32_t pg_num;
          uint64_t num_objs;
          ::decode(pg_num, hiter);
          ::decode(num_objs, hiter);
          dout(10) << __func__ << " collection hint objects is a no-op, "
		   << " pg_num " << pg_num << " num_objects " << num_objs
		   << dendl;
        } else {
          // Ignore the hint
          dout(10) << __func__ << " unknown collection hint " << type << dendl;
        }
      }
      break;

    case Transaction::OP_RMCOLL:
      {
        coll_t cid = i.get_cid(op->cid);
	r = _remove_collection(txc, cid, &c);
      }
      break;

    case Transaction::OP_COLL_ADD:
      assert(0 == "not implmeented");
      break;

    case Transaction::OP_COLL_REMOVE:
      assert(0 == "not implmeented");
      break;

    case Transaction::OP_COLL_MOVE:
      assert(0 == "deprecated");
      break;

    case Transaction::OP_COLL_MOVE_RENAME:
      {
	assert(op->cid == op->dest_cid);
        ghobject_t oldoid = i.get_oid(op->oid);
        ghobject_t newoid = i.get_oid(op->dest_oid);
	r = _rename(txc, c, oldoid, newoid);
      }
      break;

    case Transaction::OP_COLL_SETATTR:
      r = -EOPNOTSUPP;
      break;

    case Transaction::OP_COLL_RMATTR:
      r = -EOPNOTSUPP;
      break;

    case Transaction::OP_COLL_RENAME:
      assert(0 == "not implmeneted");
      break;

    case Transaction::OP_OMAP_CLEAR:
      {
        ghobject_t oid = i.get_oid(op->oid);
	r = _omap_clear(txc, c, oid);
      }
      break;
    case Transaction::OP_OMAP_SETKEYS:
      {
        ghobject_t oid = i.get_oid(op->oid);
	bufferlist aset_bl;
        i.decode_attrset_bl(&aset_bl);
	r = _omap_setkeys(txc, c, oid, aset_bl);
      }
      break;
    case Transaction::OP_OMAP_RMKEYS:
      {
        ghobject_t oid = i.get_oid(op->oid);
	bufferlist keys_bl;
        i.decode_keyset_bl(&keys_bl);
	r = _omap_rmkeys(txc, c, oid, keys_bl);
      }
      break;
    case Transaction::OP_OMAP_RMKEYRANGE:
      {
        ghobject_t oid = i.get_oid(op->oid);
        string first, last;
        first = i.decode_string();
        last = i.decode_string();
	r = _omap_rmkey_range(txc, c, oid, first, last);
      }
      break;
    case Transaction::OP_OMAP_SETHEADER:
      {
        ghobject_t oid = i.get_oid(op->oid);
        bufferlist bl;
        i.decode_bl(bl);
	r = _omap_setheader(txc, c, oid, bl);
      }
      break;
    case Transaction::OP_SPLIT_COLLECTION:
      assert(0 == "deprecated");
      break;
    case Transaction::OP_SPLIT_COLLECTION2:
      {
        uint32_t bits = op->split_bits;
        uint32_t rem = op->split_rem;
	r = _split_collection(txc, c, cvec[op->dest_cid], bits, rem);
      }
      break;

    case Transaction::OP_SETALLOCHINT:
      {
        ghobject_t oid = i.get_oid(op->oid);
        uint64_t expected_object_size = op->expected_object_size;
        uint64_t expected_write_size = op->expected_write_size;
	r = _setallochint(txc, c, oid,
			  expected_object_size,
			  expected_write_size);
      }
      break;

    default:
      derr << "bad op " << op->op << dendl;
      assert(0);
    }

    if (r < 0) {
      bool ok = false;

      if (r == -ENOENT && !(op->op == Transaction::OP_CLONERANGE ||
			    op->op == Transaction::OP_CLONE ||
			    op->op == Transaction::OP_CLONERANGE2 ||
			    op->op == Transaction::OP_COLL_ADD))
	// -ENOENT is usually okay
	ok = true;
      if (r == -ENODATA)
	ok = true;

      if (!ok) {
	const char *msg = "unexpected error code";

	if (r == -ENOENT && (op->op == Transaction::OP_CLONERANGE ||
			     op->op == Transaction::OP_CLONE ||
			     op->op == Transaction::OP_CLONERANGE2))
	  msg = "ENOENT on clone suggests osd bug";

	if (r == -ENOSPC)
	  // For now, if we hit _any_ ENOSPC, crash, before we do any damage
	  // by partially applying transactions.
	  msg = "ENOSPC handling not implemented";

	if (r == -ENOTEMPTY) {
	  msg = "ENOTEMPTY suggests garbage data in osd data dir";
	}

	dout(0) << " error " << cpp_strerror(r) << " not handled on operation " << op->op
		<< " (op " << pos << ", counting from 0)" << dendl;
	dout(0) << msg << dendl;
	dout(0) << " transaction dump:\n";
	JSONFormatter f(true);
	f.open_object_section("transaction");
	t->dump(&f);
	f.close_section();
	f.flush(*_dout);
	*_dout << dendl;
	assert(0 == "unexpected error");
      }
    }

    ++pos;
  }

  return 0;
}



// -----------------
// write operations

int NewStore::_touch(TransContext *txc,
		     CollectionRef& c,
		     const ghobject_t& oid)
{
  dout(15) << __func__ << " " << c->cid << " " << oid << dendl;
  int r = 0;
  RWLock::WLocker l(c->lock);
  OnodeRef o = c->get_onode(oid, true);
  assert(o);
  o->exists = true;
  _assign_nid(txc, o);
  txc->write_onode(o);
  dout(10) << __func__ << " " << c->cid << " " << oid << " = " << r << dendl;
  return r;
}

int NewStore::_do_overlay_clear(TransContext *txc,
				OnodeRef o)
{
  dout(10) << __func__ << " " << o->oid << dendl;

  map<uint64_t,overlay_t>::iterator p = o->onode.overlay_map.begin();
  while (p != o->onode.overlay_map.end()) {
    dout(20) << __func__ << " rm " << p->first << " " << p->second << dendl;
    string key;
    get_overlay_key(o->onode.nid, p->first, &key);
    txc->t->rmkey(PREFIX_OVERLAY, key);
    o->onode.overlay_map.erase(p++);
  }
  o->onode.shared_overlays.clear();
  return 0;
}

int NewStore::_do_overlay_trim(TransContext *txc,
			       OnodeRef o,
			       uint64_t offset,
			       uint64_t length)
{
  dout(10) << __func__ << " " << o->oid << " "
	   << offset << "~" << length << dendl;
  int changed = 0;

  map<uint64_t,overlay_t>::iterator p =
    o->onode.overlay_map.lower_bound(offset);
  if (p != o->onode.overlay_map.begin()) {
    --p;
  }
  while (p != o->onode.overlay_map.end()) {
    if (p->first >= offset + length) {
      dout(20) << __func__ << " stop at " << p->first << " " << p->second
	       << dendl;
      break;
    }
    if (p->first + p->second.length <= offset) {
      dout(20) << __func__ << " skip " << p->first << " " << p->second
	       << dendl;
      ++p;
      continue;
    }
    if (p->first >= offset &&
	p->first + p->second.length <= offset + length) {
      dout(20) << __func__ << " rm " << p->first << " " << p->second
	       << dendl;
      if (o->onode.shared_overlays.count(p->second.key) == 0) {
	string key;
	get_overlay_key(o->onode.nid, p->first, &key);
	txc->t->rmkey(PREFIX_OVERLAY, key);
      }
      o->onode.overlay_map.erase(p++);
      ++changed;
      continue;
    }
    if (p->first >= offset) {
      dout(20) << __func__ << " trim_front " << p->first << " " << p->second
	       << dendl;
      overlay_t& ov = o->onode.overlay_map[offset + length] = p->second;
      uint64_t by = offset + length - p->first;
      ov.value_offset += by;
      ov.length -= by;
      o->onode.overlay_map.erase(p++);
      ++changed;
      continue;
    }
    if (p->first < offset &&
	p->first + p->second.length <= offset + length) {
      dout(20) << __func__ << " trim_tail " << p->first << " " << p->second
	       << dendl;
      p->second.length = offset - p->first;
      ++p;
      ++changed;
      continue;
    }
    dout(20) << __func__ << " split " << p->first << " " << p->second
	     << dendl;
    assert(p->first < offset);
    assert(p->first + p->second.length > offset + length);
    overlay_t& nov = o->onode.overlay_map[offset + length] = p->second;
    p->second.length = offset - p->first;
    uint64_t by = offset + length - p->first;
    nov.value_offset += by;
    nov.length -= by;
    o->onode.shared_overlays.insert(p->second.key);
    ++p;
    ++changed;
  }
  return changed;
}

int NewStore::_do_overlay_write(TransContext *txc,
				OnodeRef o,
				uint64_t offset,
				uint64_t length,
				const bufferlist& bl)
{
  _do_overlay_trim(txc, o, offset, length);

  dout(10) << __func__ << " " << o->oid << " "
	   << offset << "~" << length << dendl;
  overlay_t& ov = o->onode.overlay_map[offset] =
    overlay_t(++o->onode.last_overlay_key, 0, length);
  dout(20) << __func__ << " added " << offset << " " << ov << dendl;
  string key;
  get_overlay_key(o->onode.nid, o->onode.last_overlay_key, &key);
  txc->t->set(PREFIX_OVERLAY, key, bl);
  return 0;
}

int NewStore::_do_write_all_overlays(TransContext *txc,
				     OnodeRef o)
{
  if (o->onode.overlay_map.empty())
    return 0;

  for (map<uint64_t,overlay_t>::iterator p = o->onode.overlay_map.begin();
       p != o->onode.overlay_map.end(); ) {
    dout(10) << __func__ << " overlay " << p->first
	     << "~" << p->second.length << " " << p->second << dendl;

    // find or create frag
    uint64_t frag_first = 0;
    fragment_t *f;
    map<uint64_t,fragment_t>::iterator fp = o->onode.find_fragment(p->first);
    if (fp == o->onode.data_map.end() ||
	fp->first >= p->first + p->second.length ||
	fp->first + fp->second.length <= p->first) {
      dout(20) << __func__ << " frag " << fp->first << " " << fp->second
	       << dendl;
      frag_first = p->first - p->first % o->onode.frag_size;
      if (frag_first == fp->first) {
	// extend existing frag
	f = &fp->second;
	int r = _clean_fid_tail(txc, *f);
	if (r < 0)
	  return r;
	f->length = (p->first + p->second.length) - frag_first;
	dout(20) << __func__ << " extended " << f->fid << " to "
		 << frag_first << "~" << f->length << dendl;
      } else {
	// new frag
	f = &o->onode.data_map[frag_first];
	f->offset = 0;
	f->length = p->first + p->second.length - frag_first;
	assert(f->length <= o->onode.frag_size);
	int fd = _create_fid(txc, o, &f->fid, O_RDWR);
	if (fd < 0) {
	  return fd;
	}
	VOID_TEMP_FAILURE_RETRY(::close(fd));
	dout(20) << __func__ << " create " << f->fid << dendl;
      }
    } else {
      frag_first = fp->first;
      f = &fp->second;
    }
    assert(frag_first <= p->first);
    assert(p->first + p->second.length <= frag_first + f->length);

    wal_op_t *op = _get_wal_op(txc);
    op->op = wal_op_t::OP_WRITE;
    op->offset = p->first - frag_first;
    op->length = p->second.length;
    op->fid = f->fid;
    // The overlays will be removed from the db after applying the WAL
    op->nid = o->onode.nid;
    op->overlays.push_back(p->second);

    // Combine with later overlays if contiguous
    map<uint64_t,overlay_t>::iterator prev = p, next = p;
    ++next;
    while (next != o->onode.overlay_map.end()) {
      if (prev->first + prev->second.length == next->first &&
	  next->first < frag_first + o->onode.frag_size) {
        dout(10) << __func__ << " combining overlay " << next->first
                 << "~" << next->second << dendl;
        op->length += next->second.length;
        op->overlays.push_back(next->second);

	if (next->first + next->second.length > frag_first + f->length) {
	  f->length = next->first + next->second.length - frag_first;
	  dout(20) << __func__ << "  extended fragment " << f->fid
		   << " to " << frag_first << "~" << f->length << dendl;
	}

        ++prev;
        ++next;
      } else {
	break;
      }
    }
    p = next;
  }

  // put the shared overlay keys into the WAL transaction, so that we
  // can cleanup them later after applying the WAL
  for (set<uint64_t>::iterator p = o->onode.shared_overlays.begin();
       p != o->onode.shared_overlays.end();
       ++p) {
    dout(10) << __func__ << " shared overlay " << *p << dendl;
    string key;
    get_overlay_key(o->onode.nid, *p, &key);
    txc->wal_txn->shared_overlay_keys.push_back(key);
  }

  o->onode.overlay_map.clear();
  o->onode.shared_overlays.clear();
  txc->write_onode(o);
  return 0;
}

void NewStore::_do_read_all_overlays(wal_transaction_t& wt)
{
  for (list<wal_op_t>::iterator p = wt.ops.begin(); p != wt.ops.end(); ++p) {
    for (vector<overlay_t>::iterator q = p->overlays.begin();
         q != p->overlays.end(); ++q) {
      string key;
      get_overlay_key(p->nid, q->key, &key);
      bufferlist bl, bl_data;
      db->get(PREFIX_OVERLAY, key, &bl);
      bl_data.substr_of(bl, q->value_offset, q->length);
      p->data.claim_append(bl_data);
    }
  }
  return;
}

void NewStore::_dump_onode(OnodeRef o)
{
  dout(30) << __func__ << " " << o
	   << " nid " << o->onode.nid
	   << " size " << o->onode.size
	   << " frag_size " << o->onode.frag_size
	   << " expected_object_size " << o->onode.expected_object_size
	   << " expected_write_size " << o->onode.expected_write_size
	   << dendl;
  for (map<string,bufferptr>::iterator p = o->onode.attrs.begin();
       p != o->onode.attrs.end();
       ++p) {
    dout(30) << __func__ << "  attr " << p->first
	     << " len " << p->second.length() << dendl;
  }
  for (map<uint64_t,fragment_t>::iterator p = o->onode.data_map.begin();
       p != o->onode.data_map.end();
       ++p) {
    dout(30) << __func__ << "  fragment " << p->first << " " << p->second
	     << dendl;
  }
  for (map<uint64_t,overlay_t>::iterator p = o->onode.overlay_map.begin();
       p != o->onode.overlay_map.end();
       ++p) {
    dout(30) << __func__ << "  overlay " << p->first << " " << p->second
	     << dendl;
  }
  if (!o->onode.shared_overlays.empty()) {
    dout(30) << __func__ << "  shared_overlays " << o->onode.shared_overlays
	     << dendl;
  }
}

int NewStore::_do_write(TransContext *txc,
			OnodeRef o,
			uint64_t orig_offset, uint64_t orig_length,
			bufferlist& orig_bl,
			uint32_t fadvise_flags)
{
  int fd = -1;
  int r = 0;
  unsigned flags;

  dout(20) << __func__
	   << " " << o->oid << " " << orig_offset << "~" << orig_length
	   << " - have " << o->onode.size
	   << " bytes in " << o->onode.data_map.size()
	   << " fragments" << dendl;
  _dump_onode(o);
  o->exists = true;

  if (!o->onode.frag_size && o->onode.data_map.empty() &&
      o->onode.overlay_map.empty()) {
    o->onode.frag_size = cct->_conf->get_val<uint64_t>("newstore_min_frag_size");
    dout(20) << __func__ << " set frag_size " << o->onode.frag_size << dendl;
  }

  if (orig_offset + orig_length > o->onode.size) {
    dout(20) << __func__ << " extending size to " << orig_offset + orig_length
	     << dendl;
    o->onode.size = orig_offset + orig_length;
  }

  uint64_t frag_size = o->onode.frag_size;
  uint64_t length;
  for (uint64_t offset = orig_offset;
       offset < orig_offset + orig_length;
       offset += length) {
    // calc length for this fragment / chunk
    length = orig_offset + orig_length - offset;
    if (offset / frag_size != (offset + length) / frag_size) {
      length = (offset / frag_size + 1) * frag_size - offset;
    }
    dout(20) << __func__ << " chunk " << offset << "~" << length
	     << " (frag_size " << frag_size << ")"
	     << dendl;
    bufferlist bl;
    bl.substr_of(orig_bl, offset - orig_offset, length);

    if ((int)o->onode.overlay_map.size() < cct->_conf->get_val<uint64_t>("newstore_overlay_max") &&
	(int)length <= cct->_conf->get_val<uint64_t>("newstore_overlay_max_length")) {
      // write an overlay
      r = _do_overlay_write(txc, o, offset, length, bl);
      if (r < 0)
	goto out;
      txc->write_onode(o);
      continue;
    }

    flags = O_RDWR;
    if (cct->_conf->get_val<bool>("newstore_o_direct") &&
	(offset & ~CEPH_PAGE_MASK) == 0 &&
	(length & ~CEPH_PAGE_MASK) == 0) {
      dout(20) << __func__ << " page-aligned, can use O_DIRECT, "
	       << bl.buffers().size() << " buffers" << dendl;
      flags |= O_DIRECT | O_DSYNC;
      if (!bl.is_page_aligned()) {
	dout(20) << __func__ << " rebuilding buffer to be page-aligned" << dendl;
	bl.rebuild();
      }
    }

    map<uint64_t, fragment_t>::iterator fp =
      o->onode.data_map.lower_bound(offset);

    if ((fp == o->onode.data_map.end() || fp->first > offset) &&
	fp != o->onode.data_map.begin()) {
      --fp;
      if (offset < fp->first + frag_size &&
	  offset >= fp->first + fp->second.length) {
	// -- append (possibly with gap) --
	fragment_t &f = fp->second;
	fd = _open_fid(f.fid, flags);
	if (fd < 0) {
	  r = fd;
	  goto out;
	}
	r = _clean_fid_tail_fd(f, fd); // in case there is trailing crap
	if (r < 0) {
	  goto out;
	}
	f.length = (offset + length) - fp->first;
	uint64_t x_offset = offset - fp->first;
	dout(20) << __func__ << " append " << f.fid << " writing "
		 << offset << "~" << length << " to "
		 << x_offset << "~" << length << dendl;
#ifdef HAVE_LIBAIO
	if (cct->_conf->get_val<bool>("newstore_aio") && (flags & O_DIRECT)) {
	  txc->pending_aios.push_back(aio_t(txc, fd));
	  aio_t& aio = txc->pending_aios.back();
	  bl.prepare_iov(&aio.iov);
	  txc->aio_bl.append(bl);
	  aio.pwritev(x_offset, length);
	  dout(2) << __func__ << " prepared aio " << &aio << dendl;
	} else
#endif
        {
	  ::lseek64(fd, x_offset, SEEK_SET);
	  r = bl.write_fd(fd);
	  if (r < 0) {
	    derr << __func__ << " bl.write_fd error: " << cpp_strerror(r)
		 << dendl;
	    goto out;
	  }
	  txc->sync_fd(fd);
	}
	continue;
      }
      ++fp;
    }

    if (fp == o->onode.data_map.end() ||
	fp->first >= offset + length) {
      // -- new frag --
      // Note: unless we are at EOF, allocate the full frag_size,
      // even though we only write to part of it.
      uint64_t frag_first = offset - (offset % frag_size);
      fragment_t &f = o->onode.data_map[frag_first];
      f.offset = 0;
      f.length = MIN(frag_size, o->onode.size - frag_first);
      assert(f.length <= frag_size);
      fd = _create_fid(txc, o, &f.fid, flags);
      if (fd < 0) {
	r = fd;
	goto out;
      }
      uint64_t x_offset = offset - frag_first;
      dout(20) << __func__ << " create " << f.fid << " writing "
	       << offset << "~" << length
	       << " to " << x_offset << "~" << length << dendl;
#ifdef HAVE_LIBAIO
      if (cct->_conf->get_val<bool>("newstore_aio") && (flags & O_DIRECT)) {
	txc->pending_aios.push_back(aio_t(txc, fd));
	aio_t& aio = txc->pending_aios.back();
	bl.prepare_iov(&aio.iov);
	txc->aio_bl.append(bl);
	aio.pwritev(x_offset, length);
	dout(2) << __func__ << " prepared aio " << &aio << dendl;
      } else
#endif
      {
	::lseek64(fd, x_offset, SEEK_SET);
	r = bl.write_fd(fd);
	if (r < 0) {
	  derr << __func__ << " bl.write_fd error: " << cpp_strerror(r) << dendl;
	  goto out;
	}
	txc->sync_fd(fd);
      }
      continue;
    }

    if (fp != o->onode.data_map.end() &&
	fp->first == offset &&
	fp->second.length <= length) {
      // -- overwrite/replace entire frag --
      fragment_t& f = fp->second;

      _do_overlay_trim(txc, o, offset, length);

      wal_op_t *op = _get_wal_op(txc);
      op->op = wal_op_t::OP_REMOVE;
      op->fid = fp->second.fid;

      f.length = length;
      fd = _create_fid(txc, o, &f.fid, O_RDWR);
      if (fd < 0) {
	r = fd;
	goto out;
      }
      dout(20) << __func__ << " replace old fid " << op->fid
	       << " with new fid " << f.fid
	       << ", writing " << offset << "~" << length
	       << " to 0~" << length << dendl;

#ifdef HAVE_LIBAIO
      if (cct->_conf->get_val<bool>("newstore_aio") && (flags & O_DIRECT)) {
	txc->pending_aios.push_back(aio_t(txc, fd));
	aio_t& aio = txc->pending_aios.back();
	bl.prepare_iov(&aio.iov);
	txc->aio_bl.append(bl);
	aio.pwritev(0, bl.length());
	dout(2) << __func__ << " prepared aio " << &aio << dendl;
      } else
#endif
      {
	r = bl.write_fd(fd);
	if (r < 0) {
	  derr << __func__ << " bl.write_fd error: " << cpp_strerror(r) << dendl;
	  goto out;
	}
	txc->sync_fd(fd);
      }
      continue;
    }

    if (true) {
      // -- WAL --
      r = _do_write_all_overlays(txc, o);
      if (r < 0)
	goto out;
      fragment_t& f = fp->second;
      assert(fp->first <= offset);
      assert(offset + length <= fp->first + fp->second.length);
      r = _clean_fid_tail(txc, f);
      if (r < 0)
	goto out;
      wal_op_t *op = _get_wal_op(txc);
      op->op = wal_op_t::OP_WRITE;
      op->offset = offset - fp->first;
      op->length = length;
      op->fid = f.fid;
      op->data = bl;
      if (offset + length - fp->first > f.length) {
	f.length = offset + length - fp->first;
      }
      dout(20) << __func__ << " wal " << f.fid << " write "
	       << offset << "~" << length << " to "
	       << op->offset << "~" << op->length
	       << dendl;
      continue;
    }

    assert(0 == "don't get here");
  }
  r = 0;

 out:
  return r;
}

int NewStore::_clean_fid_tail_fd(const fragment_t& f, int fd)
{
  struct stat st;
  int r = ::fstat(fd, &st);
  if (r < 0) {
    r = -errno;
    derr << __func__ << " failed to fstat " << f.fid << ": "
	 << cpp_strerror(r) << dendl;
    return r;
  }
  if (st.st_size > f.offset + f.length) {
    dout(20) << __func__ << " frag " << f.fid << " is long (" << st.st_size
	     << "), truncating to " << (f.offset + f.length) << dendl;
    r = ::ftruncate(fd, f.offset + f.length);
    if (r < 0) {
      derr << __func__ << " failed to ftruncate " << f.fid << ": "
	   << cpp_strerror(r) << dendl;
      return r;
    }
    return 1;
  }
  return 0;
}

int NewStore::_clean_fid_tail(TransContext *txc, const fragment_t& f)
{
  int fd = _open_fid(f.fid, O_RDWR);
  if (fd < 0) {
    return fd;
  }
  int r = _clean_fid_tail_fd(f, fd);
  if (r < 0) {
    return r;
  }
  if (r > 0) {
    txc->sync_fd(fd);
  } else {
    // all good!
    VOID_TEMP_FAILURE_RETRY(::close(fd));
  }
  return 0;
}


int NewStore::_write(TransContext *txc,
		     CollectionRef& c,
		     const ghobject_t& oid,
		     uint64_t offset, size_t length,
		     bufferlist& bl,
		     uint32_t fadvise_flags)
{
  dout(15) << __func__ << " " << c->cid << " " << oid
	   << " " << offset << "~" << length
	   << dendl;
  RWLock::WLocker l(c->lock);
  OnodeRef o = c->get_onode(oid, true);
  _assign_nid(txc, o);
  int r = _do_write(txc, o, offset, length, bl, fadvise_flags);
  txc->write_onode(o);

  dout(10) << __func__ << " " << c->cid << " " << oid
	   << " " << offset << "~" << length
	   << " = " << r << dendl;
  return r;
}

int NewStore::_zero(TransContext *txc,
		    CollectionRef& c,
		    const ghobject_t& oid,
		    uint64_t offset, size_t length)
{
  dout(15) << __func__ << " " << c->cid << " " << oid
	   << " " << offset << "~" << length
	   << dendl;
  int r = 0;

  RWLock::WLocker l(c->lock);
  OnodeRef o = c->get_onode(oid, true);
  _assign_nid(txc, o);

  // overlay
  _do_overlay_trim(txc, o, offset, length);

  map<uint64_t,fragment_t>::iterator fp = o->onode.find_fragment(offset);
  while (fp != o->onode.data_map.end()) {
    if (fp->first >= offset + length)
      break;

    fragment_t& f = fp->second;
    if (offset <= fp->first &&
	offset + length >= fp->first + fp->second.length) {
      // remove fragment
      dout(20) << __func__ << " wal rm fragment " << fp->first << " "
	       << f << dendl;
      wal_op_t *op = _get_wal_op(txc);
      op->op = wal_op_t::OP_REMOVE;
      op->fid = f.fid;
      o->onode.data_map.erase(fp++);
      continue;
    }

    // start,end are offsets in the fragment
    uint64_t start = 0;
    if (offset > fp->first) {
      start = offset - fp->first;
    }
    assert(o->onode.frag_size);
    uint64_t end = MIN(offset + length - fp->first,
		       o->onode.frag_size);

    if (end >= f.length) {
      // truncate fragment
      wal_op_t *op = _get_wal_op(txc);
      op->op = wal_op_t::OP_TRUNCATE;
      op->fid = f.fid;
      op->offset = start;
      dout(20) << __func__ << " wal truncate fragment " << fp->first << " "
	       << f << " to " << start << dendl;
    } else {
      // WAL
      r = _clean_fid_tail(txc, f);
      if (r < 0)
	goto out;

      wal_op_t *op = _get_wal_op(txc);
      op->op = wal_op_t::OP_ZERO;
      op->offset = start;
      op->length = end - start;
      op->fid = f.fid;
    }

    // size fragment up?
    if (end > f.length) {
      f.length = end;
      dout(20) << __func__ << " frag " << f.fid << " sized up to "
	       << f.length << dendl;
    }
    fp++;
  }

  if (offset + length > o->onode.size) {
    o->onode.size = offset + length;
    dout(20) << __func__ << " extending size to " << offset + length
	     << dendl;
  }
  txc->write_onode(o);

 out:
  dout(10) << __func__ << " " << c->cid << " " << oid
	   << " " << offset << "~" << length
	   << " = " << r << dendl;
  return r;
}

int NewStore::_do_truncate(TransContext *txc, OnodeRef o, uint64_t offset)
{
  // trim down fragments
  map<uint64_t,fragment_t>::iterator fp = o->onode.data_map.end();
  if (fp != o->onode.data_map.begin())
    --fp;
  while (fp != o->onode.data_map.end()) {
    if (fp->first + fp->second.length <= offset) {
      break;
    }
    if (fp->first >= offset) {
      dout(20) << __func__ << " wal rm fragment " << fp->first << " "
	       << fp->second << dendl;
      wal_op_t *op = _get_wal_op(txc);
      op->op = wal_op_t::OP_REMOVE;
      op->fid = fp->second.fid;
      if (fp != o->onode.data_map.begin()) {
	o->onode.data_map.erase(fp--);
	continue;
      } else {
	o->onode.data_map.erase(fp);
	break;
      }
    } else {
      assert(fp->first + fp->second.length > offset);
      assert(fp->first < offset);
      uint64_t newlen = offset - fp->first;
      dout(20) << __func__ << " wal truncate fragment " << fp->first << " "
	       << fp->second << " to " << newlen << dendl;
      fragment_t& f = fp->second;
      f.length = newlen;
      wal_op_t *op = _get_wal_op(txc);
      op->op = wal_op_t::OP_TRUNCATE;
      op->offset = offset;
      op->fid = f.fid;
      break;
    }
  }

  // truncate up trailing fragment?

  // trim down overlays
  map<uint64_t,overlay_t>::iterator op = o->onode.overlay_map.end();
  if (op != o->onode.overlay_map.begin())
    --op;
  while (op != o->onode.overlay_map.end()) {
    if (op->first + op->second.length <= offset) {
      break;
    }
    if (op->first >= offset) {
      if (!o->onode.shared_overlays.count(op->second.key)) {
	dout(20) << __func__ << " rm overlay " << op->first << " "
		 << op->second << dendl;
	string key;
	get_overlay_key(o->onode.nid, op->second.key, &key);
	txc->t->rmkey(PREFIX_OVERLAY, key);
      } else {
	dout(20) << __func__ << " rm overlay " << op->first << " "
		 << op->second << " (shared)" << dendl;
      }
      if (op != o->onode.overlay_map.begin()) {
	o->onode.overlay_map.erase(op--);
	continue;
      } else {
	o->onode.overlay_map.erase(op);
	break;
      }
    } else {
      assert(op->first + op->second.length > offset);
      assert(op->first < offset);
      uint64_t newlen = offset - op->first;
      dout(20) << __func__ << " truncate overlay " << op->first << " "
	       << op->second << " to " << newlen << dendl;
      overlay_t& ov = op->second;
      ov.length = newlen;
      break;
    }
  }

  o->onode.size = offset;
  txc->write_onode(o);
  return 0;
}

int NewStore::_truncate(TransContext *txc,
			CollectionRef& c,
			const ghobject_t& oid,
			uint64_t offset)
{
  dout(15) << __func__ << " " << c->cid << " " << oid
	   << " " << offset
	   << dendl;
  int r = 0;

  RWLock::WLocker l(c->lock);
  OnodeRef o = c->get_onode(oid, false);
  if (!o->exists) {
    r = -ENOENT;
    goto out;
  }
  r = _do_truncate(txc, o, offset);

 out:
  dout(10) << __func__ << " " << c->cid << " " << oid
	   << " " << offset
	   << " = " << r << dendl;
  return r;
}

int NewStore::_do_remove(TransContext *txc,
			 OnodeRef o)
{
  string key;
  o->exists = false;
  if (!o->onode.data_map.empty()) {
    for (map<uint64_t,fragment_t>::iterator p = o->onode.data_map.begin();
	 p != o->onode.data_map.end();
	 ++p) {
      dout(20) << __func__ << " will wal remove " << p->second.fid << dendl;
      wal_op_t *op = _get_wal_op(txc);
      op->op = wal_op_t::OP_REMOVE;
      op->fid = p->second.fid;
    }
  }
  o->onode.data_map.clear();
  o->onode.size = 0;
  if (o->onode.omap_head) {
    _do_omap_clear(txc, o->onode.omap_head);
  }

  get_object_key(cct, o->oid, &key);
  txc->t->rmkey(PREFIX_OBJ, key);
  return 0;
}

int NewStore::_remove(TransContext *txc,
		      CollectionRef& c,
		      const ghobject_t& oid)
{
  dout(15) << __func__ << " " << c->cid << " " << oid << dendl;
  int r;
  RWLock::WLocker l(c->lock);
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }
  r = _do_remove(txc, o);

 out:
  dout(10) << __func__ << " " << c->cid << " " << oid << " = " << r << dendl;
  return r;
}

int NewStore::_setattr(TransContext *txc,
		       CollectionRef& c,
		       const ghobject_t& oid,
		       const string& name,
		       bufferptr& val)
{
  dout(15) << __func__ << " " << c->cid << " " << oid
	   << " " << name << " (" << val.length() << " bytes)"
	   << dendl;
  int r = 0;

  RWLock::WLocker l(c->lock);
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }
  o->onode.attrs[name] = val;
  txc->write_onode(o);
  r = 0;

 out:
  dout(10) << __func__ << " " << c->cid << " " << oid
	   << " " << name << " (" << val.length() << " bytes)"
	   << " = " << r << dendl;
  return r;
}

int NewStore::_setattrs(TransContext *txc,
			CollectionRef& c,
			const ghobject_t& oid,
			const map<string,bufferptr>& aset)
{
  dout(15) << __func__ << " " << c->cid << " " << oid
	   << " " << aset.size() << " keys"
	   << dendl;
  int r = 0;

  RWLock::WLocker l(c->lock);
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }
  for (map<string,bufferptr>::const_iterator p = aset.begin();
       p != aset.end(); ++p)
    o->onode.attrs[p->first] = p->second;
  txc->write_onode(o);
  r = 0;

 out:
  dout(10) << __func__ << " " << c->cid << " " << oid
	   << " " << aset.size() << " keys"
	   << " = " << r << dendl;
  return r;
}


int NewStore::_rmattr(TransContext *txc,
		      CollectionRef& c,
		      const ghobject_t& oid,
		      const string& name)
{
  dout(15) << __func__ << " " << c->cid << " " << oid
	   << " " << name << dendl;
  int r = 0;

  RWLock::WLocker l(c->lock);
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }
  o->onode.attrs.erase(name);
  txc->write_onode(o);
  r = 0;

 out:
  dout(10) << __func__ << " " << c->cid << " " << oid
	   << " " << name << " = " << r << dendl;
  return r;
}

int NewStore::_rmattrs(TransContext *txc,
		       CollectionRef& c,
		       const ghobject_t& oid)
{
  dout(15) << __func__ << " " << c->cid << " " << oid << dendl;
  int r = 0;

  RWLock::WLocker l(c->lock);
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }
  o->onode.attrs.clear();
  txc->write_onode(o);
  r = 0;

 out:
  dout(10) << __func__ << " " << c->cid << " " << oid << " = " << r << dendl;
  return r;
}

void NewStore::_do_omap_clear(TransContext *txc, uint64_t id)
{
  KeyValueDB::Iterator it = db->get_iterator(PREFIX_OMAP);
  string prefix, tail;
  get_omap_header(id, &prefix);
  get_omap_tail(id, &tail);
  it->lower_bound(prefix);
  while (it->valid()) {
    if (it->key() >= tail) {
      dout(30) << __func__ << "  stop at " << tail << dendl;
      break;
    }
    txc->t->rmkey(PREFIX_OMAP, it->key());
    dout(30) << __func__ << "  rm " << it->key() << dendl;
    it->next();
  }
}

int NewStore::_omap_clear(TransContext *txc,
			  CollectionRef& c,
			  const ghobject_t& oid)
{
  dout(15) << __func__ << " " << c->cid << " " << oid << dendl;
  int r = 0;

  RWLock::WLocker l(c->lock);
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }
  if (o->onode.omap_head != 0) {
    _do_omap_clear(txc, o->onode.omap_head);
  }
  r = 0;

 out:
  dout(10) << __func__ << " " << c->cid << " " << oid << " = " << r << dendl;
  return r;
}

int NewStore::_omap_setkeys(TransContext *txc,
			    CollectionRef& c,
			    const ghobject_t& oid,
			    bufferlist &bl)
{
  dout(15) << __func__ << " " << c->cid << " " << oid << dendl;
  int r = 0;
  bufferlist::iterator p = bl.begin();
  __u32 num;

  RWLock::WLocker l(c->lock);
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }
  if (!o->onode.omap_head) {
    o->onode.omap_head = o->onode.nid;
    txc->write_onode(o);
  }
  ::decode(num, p);
  while (num--) {
    string key;
    bufferlist value;
    ::decode(key, p);
    ::decode(value, p);
    string final_key;
    get_omap_key(o->onode.omap_head, key, &final_key);
    dout(30) << __func__ << "  " << pretty_binary_string(final_key)
	     << " <- " << key << dendl;
    txc->t->set(PREFIX_OMAP, final_key, value);
  }
  r = 0;

 out:
  dout(10) << __func__ << " " << c->cid << " " << oid << " = " << r << dendl;
  return r;
}

int NewStore::_omap_setheader(TransContext *txc,
			      CollectionRef& c,
			      const ghobject_t& oid,
			      bufferlist& bl)
{
  dout(15) << __func__ << " " << c->cid << " " << oid << dendl;
  int r = 0;

  RWLock::WLocker l(c->lock);
  OnodeRef o = c->get_onode(oid, false);
  string key;
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }
  if (!o->onode.omap_head) {
    o->onode.omap_head = o->onode.nid;
    txc->write_onode(o);
  }
  get_omap_header(o->onode.omap_head, &key);
  txc->t->set(PREFIX_OMAP, key, bl);
  r = 0;

 out:
  dout(10) << __func__ << " " << c->cid << " " << oid << " = " << r << dendl;
  return r;
}

int NewStore::_omap_rmkeys(TransContext *txc,
			   CollectionRef& c,
			   const ghobject_t& oid,
			   bufferlist& bl)
{
  dout(15) << __func__ << " " << c->cid << " " << oid << dendl;
  int r = 0;
  bufferlist::iterator p = bl.begin();
  __u32 num;

  RWLock::WLocker l(c->lock);
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }
  if (!o->onode.omap_head) {
    r = 0;
    goto out;
  }
  if (!o->onode.omap_head) {
    o->onode.omap_head = o->onode.nid;
    txc->write_onode(o);
  }
  ::decode(num, p);
  while (num--) {
    string key;
    ::decode(key, p);
    string final_key;
    get_omap_key(o->onode.omap_head, key, &final_key);
    dout(30) << __func__ << "  rm " << pretty_binary_string(final_key)
	     << " <- " << key << dendl;
    txc->t->rmkey(PREFIX_OMAP, final_key);
  }
  r = 0;

 out:
  dout(10) << __func__ << " " << c->cid << " " << oid << " = " << r << dendl;
  return r;
}

int NewStore::_omap_rmkey_range(TransContext *txc,
				CollectionRef& c,
				const ghobject_t& oid,
				const string& first, const string& last)
{
  dout(15) << __func__ << " " << c->cid << " " << oid << dendl;
  int r = 0;
  KeyValueDB::Iterator it;
  string key_first, key_last;

  RWLock::WLocker l(c->lock);
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }
  if (!o->onode.omap_head) {
    r = 0;
    goto out;
  }
  it = db->get_iterator(PREFIX_OMAP);
  get_omap_key(o->onode.omap_head, first, &key_first);
  get_omap_key(o->onode.omap_head, last, &key_last);
  it->lower_bound(key_first);
  while (it->valid()) {
    if (it->key() >= key_last) {
      dout(30) << __func__ << "  stop at " << pretty_binary_string(key_last)
	       << dendl;
      break;
    }
    txc->t->rmkey(PREFIX_OMAP, it->key());
    dout(30) << __func__ << "  rm " << it->key() << dendl;
    it->next();
  }
  r = 0;

 out:
  dout(10) << __func__ << " " << c->cid << " " << oid << " = " << r << dendl;
  return r;
}

int NewStore::_setallochint(TransContext *txc,
			    CollectionRef& c,
			    const ghobject_t& oid,
			    uint64_t expected_object_size,
			    uint64_t expected_write_size)
{
  dout(15) << __func__ << " " << c->cid << " " << oid
	   << " object_size " << expected_object_size
	   << " write_size " << expected_write_size
	   << dendl;
  int r = 0;
  RWLock::WLocker l(c->lock);
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }

  o->onode.expected_object_size = expected_object_size;
  o->onode.expected_write_size = expected_write_size;
  txc->write_onode(o);

  if (o->onode.data_map.empty() && o->onode.overlay_map.empty()) {
    // FIXME: we could do something clever with onode.frag_size here.
  }

 out:
  dout(10) << __func__ << " " << c->cid << " " << oid
	   << " object_size " << expected_object_size
	   << " write_size " << expected_write_size
	   << " = " << r << dendl;
  return r;
}

int NewStore::_clone(TransContext *txc,
		     CollectionRef& c,
		     const ghobject_t& old_oid,
		     const ghobject_t& new_oid)
{
  dout(15) << __func__ << " " << c->cid << " " << old_oid << " -> "
	   << new_oid << dendl;
  int r = 0;

  RWLock::WLocker l(c->lock);
  bufferlist bl;
  OnodeRef newo;
  OnodeRef oldo = c->get_onode(old_oid, false);
  if (!oldo || !oldo->exists) {
    r = -ENOENT;
    goto out;
  }
  newo = c->get_onode(new_oid, true);
  assert(newo);
  newo->exists = true;
  _assign_nid(txc, newo);

  r = _do_read(oldo, 0, oldo->onode.size, bl, 0);
  if (r < 0)
    goto out;

  // truncate any old data
  while (!newo->onode.data_map.empty()) {
    wal_op_t *op = _get_wal_op(txc);
    op->op = wal_op_t::OP_REMOVE;
    op->fid = newo->onode.data_map.rbegin()->second.fid;
    newo->onode.data_map.erase(newo->onode.data_map.rbegin()->first);
  }

  r = _do_write(txc, newo, 0, oldo->onode.size, bl, 0);

  newo->onode.attrs = oldo->onode.attrs;

  // clone omap
  if (newo->onode.omap_head) {
    dout(20) << __func__ << " clearing old omap data" << dendl;
    _do_omap_clear(txc, newo->onode.omap_head);
  }
  if (oldo->onode.omap_head) {
    dout(20) << __func__ << " copying omap data" << dendl;
    if (!newo->onode.omap_head) {
      newo->onode.omap_head = newo->onode.nid;
    }
    KeyValueDB::Iterator it = db->get_iterator(PREFIX_OMAP);
    string head, tail;
    get_omap_header(oldo->onode.omap_head, &head);
    get_omap_tail(oldo->onode.omap_head, &tail);
    it->lower_bound(head);
    while (it->valid()) {
      string key;
      if (it->key() >= tail) {
	dout(30) << __func__ << "  reached tail" << dendl;
	break;
      } else {
	dout(30) << __func__ << "  got header/data "
		 << pretty_binary_string(it->key()) << dendl;
	assert(it->key() < tail);
	rewrite_omap_key(newo->onode.omap_head, it->key(), &key);
	txc->t->set(PREFIX_OMAP, key, it->value());
      }
      it->next();
    }
  }

  txc->write_onode(newo);

  r = 0;

 out:
  dout(10) << __func__ << " " << c->cid << " " << old_oid << " -> "
	   << new_oid << " = " << r << dendl;
  return r;
}

int NewStore::_clone_range(TransContext *txc,
			   CollectionRef& c,
			   const ghobject_t& old_oid,
			   const ghobject_t& new_oid,
			   uint64_t srcoff, uint64_t length, uint64_t dstoff)
{
  dout(15) << __func__ << " " << c->cid << " " << old_oid << " -> "
	   << new_oid << " from " << srcoff << "~" << length
	   << " to offset " << dstoff << dendl;
  int r = 0;

  RWLock::WLocker l(c->lock);
  bufferlist bl;
  OnodeRef newo;
  OnodeRef oldo = c->get_onode(old_oid, false);
  if (!oldo || !oldo->exists) {
    r = -ENOENT;
    goto out;
  }
  newo = c->get_onode(new_oid, true);
  assert(newo);
  newo->exists = true;

  r = _do_read(oldo, srcoff, length, bl, 0);
  if (r < 0)
    goto out;

  r = _do_write(txc, newo, dstoff, bl.length(), bl, 0);

  txc->write_onode(newo);

  r = 0;

 out:
  dout(10) << __func__ << " " << c->cid << " " << old_oid << " -> "
	   << new_oid << " from " << srcoff << "~" << length
	   << " to offset " << dstoff
	   << " = " << r << dendl;
  return r;
}

int NewStore::_rename(TransContext *txc,
		      CollectionRef& c,
		      const ghobject_t& old_oid,
		      const ghobject_t& new_oid)
{
  dout(15) << __func__ << " " << c->cid << " " << old_oid << " -> "
	   << new_oid << dendl;
  int r;

  RWLock::WLocker l(c->lock);
  bufferlist bl;
  string old_key, new_key;
  OnodeRef newo;
  OnodeRef oldo = c->get_onode(old_oid, false);
  if (!oldo || !oldo->exists) {
    r = -ENOENT;
    goto out;
  }
  newo = c->get_onode(new_oid, true);
  assert(newo);

  if (newo->exists) {
    r = _do_remove(txc, newo);
    if (r < 0)
      return r;
  }

  get_object_key(cct, old_oid, &old_key);
  get_object_key(cct, new_oid, &new_key);

  c->onode_map.rename(old_oid, new_oid);
  oldo->oid = new_oid;
  oldo->key = new_key;

  txc->t->rmkey(PREFIX_OBJ, old_key);
  txc->write_onode(oldo);
  r = 0;

 out:
  dout(10) << __func__ << " " << c->cid << " " << old_oid << " -> "
	   << new_oid << " = " << r << dendl;
  return r;
}

// collections

int NewStore::_create_collection(
  TransContext *txc,
  coll_t cid,
  unsigned bits,
  CollectionRef *c)
{
  dout(15) << __func__ << " " << cid << " bits " << bits << dendl;
  int r;
  bufferlist bl;

  {
    RWLock::WLocker l(coll_lock);
    if (*c) {
      r = -EEXIST;
      goto out;
    }
    c->reset(new Collection(cct, this, cid));
    (*c)->cnode.bits = bits;
    coll_map[cid] = *c;
  }
  ::encode((*c)->cnode, bl);
  txc->t->set(PREFIX_COLL, stringify(cid), bl);
  r = 0;

 out:
  dout(10) << __func__ << " " << cid << " bits " << bits << " = " << r << dendl;
  return r;
}

int NewStore::_remove_collection(TransContext *txc, coll_t cid,
				 CollectionRef *c)
{
  dout(15) << __func__ << " " << cid << dendl;
  int r;
  bufferlist empty;

  {
    RWLock::WLocker l(coll_lock);
    if (!*c) {
      r = -ENOENT;
      goto out;
    }
    pair<ghobject_t,OnodeRef> next;
    while ((*c)->onode_map.get_next(next.first, &next)) {
      if (next.second->exists) {
	r = -ENOTEMPTY;
	goto out;
      }
    }
    coll_map.erase(cid);
    txc->removed_collections.push_back(*c);
    c->reset();
  }
  txc->t->rmkey(PREFIX_COLL, stringify(cid));
  r = 0;

 out:
  dout(10) << __func__ << " " << cid << " = " << r << dendl;
  return r;
}

int NewStore::_split_collection(TransContext *txc,
				CollectionRef& c,
				CollectionRef& d,
				unsigned bits, int rem)
{
  dout(15) << __func__ << " " << c->cid << " to " << d->cid << " "
	   << " bits " << bits << dendl;
  int r;
  RWLock::WLocker l(c->lock);
  RWLock::WLocker l2(d->lock);
  c->onode_map.clear();
  d->onode_map.clear();
  c->cnode.bits = bits;
  assert(d->cnode.bits == bits);
  r = 0;

  dout(10) << __func__ << " " << c->cid << " to " << d->cid << " "
	   << " bits " << bits << " = " << r << dendl;
  return r;
}

// ===========================================

