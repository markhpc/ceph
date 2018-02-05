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
#ifndef CEPH_OSD_PETSTORE_BUFFERLISTOBJECT_H
#define CEPH_OSD_PETSTORE_BUFFERLISTOBJECT_H

#include <boost/intrusive_ptr.hpp>

#include "PetObject.h"

struct BufferlistObject : public PetObject {
  ceph::spinlock mutex;
  bufferlist data;

  BufferlistObject(CephContext *cct, const ghobject_t& oid) : PetObject(cct, oid) {}

  size_t get_size() const override { return data.length(); }
  
  int read(uint64_t offset, uint64_t len, bufferlist &bl) override;
  int write(uint64_t offset, const bufferlist &bl) override;
  int clone(PetObject *src, uint64_t srcoff, uint64_t len,
            uint64_t dstoff) override;
  int truncate(uint64_t offset) override;

  int setattrs(map<string,bufferptr>& aset);
  int rmattr(const char *name);
  int rmattrs();
  int omap_clear();
  int omap_setkeys(bufferlist& aset_bl);
  int omap_rmkeys(bufferlist& keys_bl);
  int omap_rmkeyrange(const string& first, const string& last);
  int omap_setheader(const bufferlist &bl);

  void encode(bufferlist& bl) const override {
    ENCODE_START(1, 1, bl);
    ::encode(data, bl);
    encode_base(bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& p) override {
    DECODE_START(1, p);
    ::decode(data, p);
    decode_base(p);
    DECODE_FINISH(p);
  }
};

#endif
