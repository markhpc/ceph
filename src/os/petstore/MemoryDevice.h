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

#ifndef CEPH_OS_PETSTORE_MEMORYDEVICE_H
#define CEPH_OS_PETSTORE_MEMORYDEVICE_H

#include "PetDevice.h"
#include "Collection.h"

class MemoryDevice : public PetDevice {

public:
  MemoryDevice(CephContext* cct);

  int mkfs();
  int mount();
  int unmount();
  void set_fsid(uuid_d u);
  uuid_d get_fsid();

private:
  ceph::unordered_map<coll_t, CollectionRef> coll_map;
  RWLock coll_lock = {"PetStore::coll_lock"};    ///< rwlock to protect coll_map

  Finisher finisher;


  CollectionRef get_collection(const coll_t& cid);

  int _save();
  int _load();

};
#endif //CEPH_OS_PETSTORE_MEMORYDEVICE_H
