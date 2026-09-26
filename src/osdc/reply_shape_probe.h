// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#ifndef CEPH_OSDC_REPLY_SHAPE_PROBE_H
#define CEPH_OSDC_REPLY_SHAPE_PROBE_H

#include <cstdint>

namespace ceph::osdc {

/**
 * The exact live compatibility-copy condition from
 * Objecter::handle_osd_op_reply(): the caller provided a pre-sized outbl
 * whose length equals the payload length and the payload fits in a single
 * buffer.  Both the real output branch and the benchmark classifier below
 * call THIS predicate, so test and production cannot drift apart.  Using
 * it in the branch changes runtime behavior in no way.
 */
inline bool compatibility_copy_applies(uint64_t outbl_len,
                                       uint64_t data_len,
                                       unsigned num_buffers) {
  return outbl_len == data_len && num_buffers <= 1;
}

/**
 * Benchmark/prototype-only census of how an Objecter read reply's payload
 * lands, measured at Objecter::handle_osd_op_reply() before the output
 * branch runs (that branch moves or copies the payload; classification must
 * read the shapes first).
 *
 * What this proves: the COPY-ELISION UPPER BOUND AT THE OBJECTER LAYER.
 * The compatibility copy branch is the only shape in which the reply
 * payload is copied into a caller-provided pre-sized buffer; those bytes
 * are the ceiling on what any future copy-elision (including a direct
 * receive) could remove at this layer.  The reason counters quantify why a
 * reply cannot take that shape.
 *
 * Precision on the claim path: claim_data moves the reply buffers, so it
 * avoids an Objecter-layer copy.  That is NOT an end-to-end copy-freedom
 * claim -- other layers (e.g. messenger staging) may still copy these
 * bytes; this census says nothing about them.
 *
 * What this does NOT prove: direct-receive eligibility.  Nothing here
 * shows the payload was a one-op plain OSD READ, that the network frame
 * passed the msgr2 probe's session/layout potential
 * (msg/async/direct_rx_probe.h -- an independent, per-connection
 * measurement), or that an armed fixed-size target exists.  A
 * copy_pre_sized frame only means "caller had a pre-sized single-part
 * destination that matched the payload length".
 *
 * Unresolved caveat (deliberately not assumed here): which client API
 * path the NIXL benchmark's reads actually take into the Objecter -- and
 * therefore which outbl state (pre-sized, empty, or absent) those replies
 * present to this census -- is not yet measured or identified.  Mapping
 * this Objecter census onto NIXL benchmark traffic stays an open item
 * until measurement or code-path identification resolves it.
 *
 * Classification mirrors the real branch exactly, sharing
 * compatibility_copy_applies() with it:
 *
 *     payload present, outbl present, predicate true
 *         -> copy_pre_sized       (compatibility copy branch runs)
 *     payload present, outbl present, predicate false
 *         -> a claim_data delivery, split by why the predicate failed:
 *            claim_empty_outbl    (outbl length 0: caller provided no
 *                                  pre-sized destination bytes)
 *            claim_length_mismatch (non-zero outbl length != payload
 *                                    length)
 *            claim_multi_part     (lengths equal, multi-buffer payload)
 *     payload present, no outbl
 *         -> no_pre_sized_output
 *     no payload
 *         -> not_counted
 *
 * Replies with no payload bytes are not counted (write acks and empty reads
 * would otherwise dominate the census).  This includes the case where the
 * caller pre-sized an outbl but the OSD returned zero data: the real output
 * branch claim-overwrites that buffer with an empty payload, yet no payload
 * bytes flowed, so nothing is counted here.  Payload present but no
 * top-level outbl counts as no_pre_sized_output: there is no caller
 * pre-sized destination to elide a copy into -- this bucket also covers
 * per-op out-bl/handler reads, whose data demuxing happens later and is
 * outside this census.
 *
 * Gated by ms_benchmark_direct_rx_probe (off by default); when off, no
 * probe call site is reached and all reply_probe counters stay zero.
 * Counting is strictly read-only: output ownership, payload handling,
 * dispatch, and completion behavior are untouched.
 */
enum class ReplyOutputShape : uint8_t {
  not_counted = 0,
  copy_pre_sized,
  claim_empty_outbl,
  claim_length_mismatch,
  claim_multi_part,
  no_pre_sized_output,
};

/**
 * Pure classification over reply-shape facts.  Callers pass values captured
 * from the real objects before any claim/copy mutates them:
 * has_outbl is op->outbl != nullptr, outbl_len is its declared length when
 * present, and data_len / num_buffers come from the reply payload
 * bufferlist.  The copy-vs-claim decision uses the same
 * compatibility_copy_applies() predicate the live branch uses.
 */
inline ReplyOutputShape classify_reply_output_shape(bool has_outbl,
                                                    uint64_t outbl_len,
                                                    uint64_t data_len,
                                                    unsigned num_buffers) {
  if (data_len == 0) {
    return ReplyOutputShape::not_counted;
  }
  if (!has_outbl) {
    return ReplyOutputShape::no_pre_sized_output;
  }
  if (compatibility_copy_applies(outbl_len, data_len, num_buffers)) {
    return ReplyOutputShape::copy_pre_sized;
  }
  if (outbl_len == 0) {
    return ReplyOutputShape::claim_empty_outbl;
  }
  if (outbl_len != data_len) {
    return ReplyOutputShape::claim_length_mismatch;
  }
  return ReplyOutputShape::claim_multi_part;
}

constexpr const char* reply_output_shape_name(ReplyOutputShape shape) {
  switch (shape) {
  case ReplyOutputShape::not_counted:
    return "not_counted";
  case ReplyOutputShape::copy_pre_sized:
    return "copy_pre_sized";
  case ReplyOutputShape::claim_empty_outbl:
    return "claim_empty_outbl";
  case ReplyOutputShape::claim_length_mismatch:
    return "claim_length_mismatch";
  case ReplyOutputShape::claim_multi_part:
    return "claim_multi_part";
  case ReplyOutputShape::no_pre_sized_output:
    return "no_pre_sized_output";
  }
  return "invalid";
}

} // namespace ceph::osdc

#endif // CEPH_OSDC_REPLY_SHAPE_PROBE_H
