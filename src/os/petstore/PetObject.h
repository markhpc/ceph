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
#ifndef CEPH_OSD_PETSTORE_OBJECT_H
#define CEPH_OSD_PETSTORE_OBJECT_H

#include "common/RefCountedObj.h"
#include "os/ObjectStore.h"

struct PetObject : public RefCountedObject {
  ghobject_t oid;
  std::mutex xattr_mutex;
  std::mutex omap_mutex;
  map<string,bufferptr> xattr;
  bufferlist omap_header;
  map<string,bufferlist> omap;

  typedef boost::intrusive_ptr<PetObject> Ref;
  friend void intrusive_ptr_add_ref(PetObject *o) { o->get(); }
  friend void intrusive_ptr_release(PetObject *o) { o->put(); }

  const ghobject_t &get_oid() {
    return oid;
  }

  PetObject(CephContext *cct, const ghobject_t& oid) : RefCountedObject(cct, 0), oid(oid) {}
  // interface for object data
  virtual size_t get_size() const = 0;
  virtual int read(uint64_t offset, uint64_t len, bufferlist &bl) = 0;
  virtual int write(uint64_t offset, const bufferlist &bl) = 0;
  virtual int clone(PetObject *src, uint64_t srcoff, uint64_t len,
                      uint64_t dstoff) = 0;
  virtual int truncate(uint64_t offset) = 0;

  virtual int setattrs(map<string,bufferptr>& aset) = 0;
  virtual int rmattr(const char *name) = 0;
  virtual int rmattrs() = 0;
  virtual int omap_clear() = 0;
  virtual int omap_setkeys(bufferlist& aset_bl) = 0;
  virtual int omap_rmkeys(bufferlist& keys_bl) = 0;
  virtual int omap_rmkeyrange(const string& first, const string& last) = 0;
  virtual int omap_setheader(const bufferlist &bl) = 0;

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

#endif
