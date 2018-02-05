/// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
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

#define dout_context cct
#define dout_subsys ceph_subsys_petstore
#undef dout_prefix
#define dout_prefix *_dout << "VectorObject(" << this << ") "

#include "VectorObject.h"

int VectorObject::read(uint64_t offset, uint64_t len, bufferlist &bl)
{
  const auto start = offset;
  const auto end = offset + len;
  buffer::ptr buf(len);


  dout(10) << __func__ << " data.size(): " << data.size() << ", offset: "
           << offset << ", len: " << len << dendl;
  assert(data.size() >= end);

  std::lock_guard<decltype(mutex)> lock(mutex);

  std::vector<char> tmp(data.begin() + start, data.begin() + end);
  buf.copy_in(0, len, &tmp[0]);
  bl.append(std::move(buf));
  return len;
}

int VectorObject::write(uint64_t offset, const bufferlist &src)
{
  unsigned len = src.length();
  const auto start = offset;
  const auto end = offset + len;
  std::vector<char> buf(len);

  dout(10) << __func__ << " data.size(): " << data.size() << ", offset: "
           << offset << ", len: " << src.length() << dendl;

  // Copy the bufferlist data into buf
  src.copy(0, len, &buf[0]);

  return _write(offset, buf);
}

int VectorObject::_write(uint64_t offset, std::vector<char>& buf) {
  std::lock_guard<decltype(mutex)> lock(mutex);

  // If we have a hole, fill it with zeros
  if (offset > data.size()) 
    data.insert(data.end(), offset - data.size(), 0);

  // Move buf to our data vector the specified offset. 
  if (offset + buf.size() > data.size())
    data.resize(offset + buf.size());
  std::move(buf.begin(), buf.end(), data.begin() + offset);

  dout(10) << __func__ << " data.size(): " << data.size() << dendl;

  return 0;
}

int VectorObject::clone(PetObject *o, uint64_t srcoff, uint64_t len,
                        uint64_t dstoff)
{
  auto vo = dynamic_cast<VectorObject*>(o);
  if (vo == nullptr) return -ENOTSUP;

  vector<char> buf;
  {
    std::lock_guard<decltype(vo->mutex)> lock(vo->mutex);
    if (srcoff == dstoff && len == vo->get_size()) {
     data = vo->data;
     return 0;
    }
    buf = vector<char>(vo->data.begin() + srcoff, vo->data.begin() + len);
  }
  return _write(dstoff, buf);
}

int VectorObject::truncate(uint64_t size)
{
  std::lock_guard<decltype(mutex)> lock(mutex);
  data.resize(size, 0);
  data.shrink_to_fit();
  return 0;
}

int VectorObject::setattrs(map<string,bufferptr>& aset)
{
  std::lock_guard<std::mutex> lock(xattr_mutex);
  for (map<string,bufferptr>::const_iterator p = aset.begin(); p != aset.end(); ++p)
    xattr[p->first] = p->second;
  return 0;
}

int VectorObject::rmattr(const char *name)
{
  std::lock_guard<std::mutex> lock(xattr_mutex);
  auto i = xattr.find(name);
  if (i == xattr.end())
    return -ENODATA;
  xattr.erase(i);
  return 0;
}

int VectorObject::rmattrs()
{
  std::lock_guard<std::mutex> lock(xattr_mutex);
  xattr.clear();
  return 0;
}

int VectorObject::omap_clear()
{
  std::lock_guard<std::mutex> lock(omap_mutex);
  omap.clear();
  omap_header.clear();
}

int VectorObject::omap_setkeys(bufferlist& aset_bl)
{
  std::lock_guard<std::mutex> lock(omap_mutex);
  bufferlist::iterator p = aset_bl.begin();
  __u32 num;
  ::decode(num, p);
  while (num--) {
    string key;
    ::decode(key, p);
    ::decode(omap[key], p);
  }
  return 0;
}

int VectorObject::omap_rmkeys(bufferlist& keys_bl)
{
  std::lock_guard<std::mutex> lock(omap_mutex);
  bufferlist::iterator p = keys_bl.begin();
  __u32 num;
  ::decode(num, p);
  while (num--) {
    string key;
    ::decode(key, p);
    omap.erase(key);
  }
  return 0;
}

int VectorObject::omap_rmkeyrange(const string& first, const string& last)
{
  std::lock_guard<std::mutex> lock(omap_mutex);
  map<string,bufferlist>::iterator p = omap.lower_bound(first);
  map<string,bufferlist>::iterator e = omap.lower_bound(last);
  omap.erase(p, e);
  return 0;
}

int VectorObject::omap_setheader(const bufferlist &bl)
{
  std::lock_guard<std::mutex> lock(omap_mutex);
  omap_header = bl;
  return 0;
}
