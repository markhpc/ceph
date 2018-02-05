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

#ifndef CEPH_OS_PETSTORE_DEVICE_H
#define CEPH_OS_PETSTORE_DEVICE_H

using std::string;

class PetDevice {
public:
  CephContext* cct;
  PetDevice (CephContext* cct, const string& _path)
    : cct(cct), path(_path) {}
  virtual ~PetDevice() = default;

  virtual int mkfs() = 0;
  virtual int mount() = 0;
  virtual int unmount() = 0;

protected:
  string path;
};

#endif //CEPH_OS_PETSTORE_DEVICE_H
