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

#include "BufferListObject.h"

int BufferlistObject::read(uint64_t offset, uint64_t len,
                                     bufferlist &bl)
{
  std::lock_guard<decltype(mutex)> lock(mutex);
  bl.substr_of(data, offset, len);
  return bl.length();
}

int BufferlistObject::write(uint64_t offset, const bufferlist &src)
{
  unsigned len = src.length();

  std::lock_guard<decltype(mutex)> lock(mutex);

  // before
  bufferlist newdata;
  if (get_size() >= offset) {
    newdata.substr_of(data, 0, offset);
  } else {
    if (get_size()) {
      newdata.substr_of(data, 0, get_size());
    }
    newdata.append_zero(offset - get_size());
  }

  newdata.append(src);

  // after
  if (get_size() > offset + len) {
    bufferlist tail;
    tail.substr_of(data, offset + len, get_size() - (offset + len));
    newdata.append(tail);
  }

  data.claim(newdata);
  return 0;
}

int BufferlistObject::clone(PetObject *src, uint64_t srcoff,
                                      uint64_t len, uint64_t dstoff)
{
  auto srcbl = dynamic_cast<BufferlistObject*>(src);
  if (srcbl == nullptr)
    return -ENOTSUP;

  bufferlist bl;
  {
    std::lock_guard<decltype(srcbl->mutex)> lock(srcbl->mutex);
    if (srcoff == dstoff && len == src->get_size()) {
      data = srcbl->data;
      return 0;
    }
    bl.substr_of(srcbl->data, srcoff, len);
  }
  return write(dstoff, bl);
}

int BufferlistObject::truncate(uint64_t size)
{
  std::lock_guard<decltype(mutex)> lock(mutex);
  if (get_size() > size) {
    bufferlist bl;
    bl.substr_of(data, 0, size);
    data.claim(bl);
  } else if (get_size() == size) {
    // do nothing
  } else {
    data.append_zero(size - get_size());
  }
  return 0;
}

int BufferlistObject::setattrs(map<string,bufferptr>& aset)
{
  std::lock_guard<std::mutex> lock(xattr_mutex);
  for (map<string,bufferptr>::const_iterator p = aset.begin(); p != aset.end(); ++p)
    xattr[p->first] = p->second;
  return 0;
}

int BufferlistObject::rmattr(const char *name)
{
  std::lock_guard<std::mutex> lock(xattr_mutex);
  auto i = xattr.find(name);
  if (i == xattr.end())
    return -ENODATA;
  xattr.erase(i);
  return 0;
}

int BufferlistObject::rmattrs()
{
  std::lock_guard<std::mutex> lock(xattr_mutex);
  xattr.clear();
  return 0;
}

int BufferlistObject::omap_clear()
{
  std::lock_guard<std::mutex> lock(omap_mutex);
  omap.clear();
  omap_header.clear();
}

int BufferlistObject::omap_setkeys(bufferlist& aset_bl)
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

int BufferlistObject::omap_rmkeys(bufferlist& keys_bl)
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

int BufferlistObject::omap_rmkeyrange(const string& first, const string& last)
{
  std::lock_guard<std::mutex> lock(omap_mutex);
  map<string,bufferlist>::iterator p = omap.lower_bound(first);
  map<string,bufferlist>::iterator e = omap.lower_bound(last);
  omap.erase(p, e);
  return 0;
}

int BufferlistObject::omap_setheader(const bufferlist &bl)
{
  std::lock_guard<std::mutex> lock(omap_mutex);
  omap_header = bl;
  return 0;
}
