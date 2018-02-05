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

#include "MemoryDevice.h"

#define dout_context cct
#define dout_subsys ceph_subsys_petmd
#undef dout_prefix
#define dout_prefix *_dout << "petmd(" << this << " " << path << ") "

// public

MemoryDevice::MemoryDevice(CephContext* cct, const std::string& path) 
  : PetDevice(cct, path)
{
}

int MemoryDevice::mkfs()
{
  string fn = path + "/collections";
  derr << path << dendl;
  bufferlist bl;
  set<coll_t> collections;
  ::encode(collections, bl);
  return bl.write_file(fn.c_str());
}

int MemoryDevice::mount()
{
  return _load();
}

int MemoryDevice::umount()
{
  return _save();
}

// private

int MemoryDevice::_save()
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

int MemoryDevice::_load()
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

