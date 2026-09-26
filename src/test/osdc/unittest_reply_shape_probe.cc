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

// Deterministic, cluster-free coverage of the benchmark-only Objecter
// reply output-shape census (ms_benchmark_direct_rx_probe).  Objecter's
// live output branch and the classifier both express the copy-vs-claim
// decision through the shared compatibility_copy_applies() predicate, so
// they cannot drift; the classifier adds the reason split.  These tests
// drive it with real ceph::buffer::list shapes so the multi-part and
// length facts come from the buffer code, not from hand-picked numbers.

#include "osdc/reply_shape_probe.h"

#include "include/buffer.h"

#include <gtest/gtest.h>

using ceph::buffer::list;
using ceph::osdc::classify_reply_output_shape;
using ceph::osdc::compatibility_copy_applies;
using ceph::osdc::reply_output_shape_name;
using ceph::osdc::ReplyOutputShape;

namespace {

// one append -> single-buffer payload (the copy-branch precondition)
static list make_part(size_t len, char fill) {
  list bl;
  bl.append(std::string(len, fill));
  return bl;
}

static list make_single_part(size_t len) {
  return make_part(len, 'D');
}

// append(const list&) shares whole bufferptrs without merging into the
// last buffer's unused tail space, so two parts stay two buffers
// (char-appends would coalesce and defeat the multi-part fixture)
static list make_multi_part(size_t a, size_t b) {
  list bl = make_part(a, 'D');
  bl.append(make_part(b, 'd'));
  return bl;
}

// Pin the fixtures' real buffer properties so the classifier inputs mean
// what the Objecter call site would compute.
TEST(ReplyShapeProbe, FixturesHaveExpectedBufferShapes) {
  list one = make_single_part(64);
  EXPECT_EQ(one.length(), 64u);
  EXPECT_LE(one.get_num_buffers(), 1u);

  list many = make_multi_part(32, 32);
  EXPECT_EQ(many.length(), 64u);
  EXPECT_GT(many.get_num_buffers(), 1u);
}

TEST(ReplyShapeProbe, CopyBranchMirror) {
  // the real handle_osd_op_reply() copy branch runs iff
  // compatibility_copy_applies(outbl->length(), bl.length(),
  //                             bl.get_num_buffers())
  // i.e. exactly outbl->length() == bl.length() &&
  //      bl.get_num_buffers() <= 1
  EXPECT_TRUE(compatibility_copy_applies(4096, 4096, 0));
  EXPECT_TRUE(compatibility_copy_applies(4096, 4096, 1));
  EXPECT_FALSE(compatibility_copy_applies(4096, 4096, 2));
  EXPECT_FALSE(compatibility_copy_applies(4095, 4096, 1));
  EXPECT_FALSE(compatibility_copy_applies(4097, 4096, 1));
  EXPECT_FALSE(compatibility_copy_applies(0, 4096, 1));

  list data = make_single_part(4096);
  auto shape = classify_reply_output_shape(
      true, 4096, data.length(), data.get_num_buffers());
  EXPECT_EQ(shape, ReplyOutputShape::copy_pre_sized);
  EXPECT_STREQ(reply_output_shape_name(shape), "copy_pre_sized");
}

TEST(ReplyShapeProbe, ClaimBranchReasons) {
  // length mismatch on a NON-ZERO pre-sized outbl: equal-buffer or not,
  // mismatch keys after the empty-outbl split below
  list single = make_single_part(100);
  EXPECT_EQ(classify_reply_output_shape(true, 64, single.length(),
                                        single.get_num_buffers()),
            ReplyOutputShape::claim_length_mismatch);
  list many = make_multi_part(50, 50);
  EXPECT_EQ(classify_reply_output_shape(true, 64, many.length(),
                                        many.get_num_buffers()),
            ReplyOutputShape::claim_length_mismatch);

  // exact length, multi-part payload: the multipart rejection
  list two = make_multi_part(32, 32);
  EXPECT_GT(two.get_num_buffers(), 1u);
  auto shape = classify_reply_output_shape(true, two.length(),
                                           two.length(),
                                           two.get_num_buffers());
  EXPECT_EQ(shape, ReplyOutputShape::claim_multi_part);
  EXPECT_STREQ(reply_output_shape_name(shape), "claim_multi_part");

  // zero-length outbl with a non-empty payload is its own claim reason:
  // the caller provided a list, but no pre-sized destination bytes.  It
  // outranks the multi-part split when both apply.
  EXPECT_EQ(classify_reply_output_shape(true, 0, 4096, 1),
            ReplyOutputShape::claim_empty_outbl);
  EXPECT_EQ(classify_reply_output_shape(true, 0, 4096, 3),
            ReplyOutputShape::claim_empty_outbl);
  EXPECT_STREQ(
      reply_output_shape_name(ReplyOutputShape::claim_empty_outbl),
      "claim_empty_outbl");
}

TEST(ReplyShapeProbe, ZeroPayloadNotCounted) {
  // no data bytes: nothing to copy or claim (write acks, empty reads, and
  // the zero-data reply that claim-overwrites a pre-sized outbl)
  list empty;
  EXPECT_EQ(classify_reply_output_shape(true, 512, empty.length(),
                                        empty.get_num_buffers()),
            ReplyOutputShape::not_counted);
  EXPECT_EQ(classify_reply_output_shape(false, 0, 0, 0),
            ReplyOutputShape::not_counted);
}

TEST(ReplyShapeProbe, NoPreSizedOutput) {
  // payload present, op->outbl null (per-op handler reads included):
  // no caller pre-sized destination exists for this census
  list data = make_single_part(4096);
  auto shape = classify_reply_output_shape(
      false, 0, data.length(), data.get_num_buffers());
  EXPECT_EQ(shape, ReplyOutputShape::no_pre_sized_output);
  EXPECT_STREQ(reply_output_shape_name(shape), "no_pre_sized_output");
}

TEST(ReplyShapeProbe, PrecedenceAndNames) {
  // data_len == 0 dominates everything
  EXPECT_EQ(classify_reply_output_shape(false, 0, 0, 3),
            ReplyOutputShape::not_counted);
  // no outbl dominates shape facts when data exists
  EXPECT_EQ(classify_reply_output_shape(false, 0, 4096, 1),
            ReplyOutputShape::no_pre_sized_output);
  // predicate wins when true
  EXPECT_EQ(classify_reply_output_shape(true, 4096, 4096, 1),
            ReplyOutputShape::copy_pre_sized);
  // claim reasons after the predicate: empty outbl first, then length
  // mismatch, then multi-part
  EXPECT_EQ(classify_reply_output_shape(true, 0, 4096, 3),
            ReplyOutputShape::claim_empty_outbl);
  EXPECT_EQ(classify_reply_output_shape(true, 1, 2, 7),
            ReplyOutputShape::claim_length_mismatch);
  EXPECT_EQ(classify_reply_output_shape(true, 8, 8, 2),
            ReplyOutputShape::claim_multi_part);

  EXPECT_STREQ(reply_output_shape_name(ReplyOutputShape::not_counted),
               "not_counted");
  EXPECT_STREQ(reply_output_shape_name(ReplyOutputShape::copy_pre_sized),
               "copy_pre_sized");
  EXPECT_STREQ(
      reply_output_shape_name(ReplyOutputShape::claim_empty_outbl),
      "claim_empty_outbl");
  EXPECT_STREQ(
      reply_output_shape_name(ReplyOutputShape::claim_length_mismatch),
      "claim_length_mismatch");
  EXPECT_STREQ(reply_output_shape_name(ReplyOutputShape::claim_multi_part),
               "claim_multi_part");
  EXPECT_STREQ(
      reply_output_shape_name(ReplyOutputShape::no_pre_sized_output),
      "no_pre_sized_output");
}

// The classifier's copy-vs-claim decision must agree with the shared
// predicate -- the same one the live branch calls -- on an exhaustive
// small grid: classified copy iff has_outbl, data non-empty, and the
// predicate holds; every other live-with-outbl frame is a claim reason.
TEST(ReplyShapeProbe, ClassifierAgreesWithSharedPredicateOnGrid) {
  for (uint64_t ol = 0; ol <= 6; ++ol) {
    for (uint64_t dl = 0; dl <= 6; ++dl) {
      for (unsigned nb = 0; nb <= 3; ++nb) {
        const auto s = classify_reply_output_shape(true, ol, dl, nb);
        const bool pred = compatibility_copy_applies(ol, dl, nb);
        if (dl == 0) {
          EXPECT_EQ(s, ReplyOutputShape::not_counted);
          continue;
        }
        if (pred) {
          EXPECT_EQ(s, ReplyOutputShape::copy_pre_sized);
          continue;
        }
        // claim path; reason split must partition it exactly
        switch (s) {
        case ReplyOutputShape::claim_empty_outbl:
          EXPECT_EQ(ol, 0u);
          break;
        case ReplyOutputShape::claim_length_mismatch:
          EXPECT_NE(ol, 0u);
          EXPECT_NE(ol, dl);
          break;
        case ReplyOutputShape::claim_multi_part:
          EXPECT_NE(ol, 0u);
          EXPECT_EQ(ol, dl);
          EXPECT_GT(nb, 1u);
          break;
        default:
          ADD_FAILURE() << "outbl-backed claim misclassified: ol=" << ol
                        << " dl=" << dl << " nb=" << nb;
        }
      }
    }
  }
}

TEST(ReplyShapeProbe, ClaimDeliveryAvoidsObjecterLayerCopy) {
  // Documented behavior proof that the claim branch moves reply buffers,
  // avoiding an Objecter-layer copy (NOT an end-to-end copy-freedom
  // claim): a multi-part payload delivered by claim keeps its buffer
  // structure in the caller's outbl.  The copy branch instead copies into
  // the caller's existing memory.  This is why the copy class is the
  // Objecter-layer copy-elision upper bound.
  list caller = make_single_part(64);
  list payload = make_multi_part(32, 32);
  const unsigned parts = payload.get_num_buffers();
  ASSERT_GT(parts, 1u);

  // mirror of the real else-branch (Message::claim_data does
  // bl = std::move(data)): equal length but multi-buffer payload ->
  // multipart rejection -> claim path moves the buffers, no copy
  ASSERT_EQ(caller.length(), payload.length());
  caller = std::move(payload);
  EXPECT_EQ(caller.length(), 64u);
  EXPECT_EQ(caller.get_num_buffers(), parts);
  EXPECT_EQ(payload.length(), 0u);  // ownership moved, not copied

  // mirror of the real copy branch: single-part equal-length payload is
  // copied into the caller buffer's existing storage.  Distinct fill
  // bytes prove the payload actually landed in the caller's memory.
  list caller2 = make_single_part(4096);            // 'D' x 4096
  list payload2;
  payload2.append(std::string(4096, 'P'));          // 'P' x 4096
  ASSERT_EQ(caller2.length(), payload2.length());
  ASSERT_LE(payload2.get_num_buffers(), 1u);
  list t;
  t = std::move(caller2);
  t.invalidate_crc();
  payload2.begin().copy(payload2.length(), t.c_str());
  caller2.substr_of(t, 0, payload2.length());
  EXPECT_EQ(caller2.length(), 4096u);
  EXPECT_TRUE(payload2.contents_equal(caller2));

  // an empty outbl with a non-empty payload is NOT the copy shape (the
  // predicate is false: no pre-sized destination bytes): the live branch
  // takes the claim path, moving the payload buffers in without any
  // Objecter-layer copy
  EXPECT_FALSE(compatibility_copy_applies(0, 4096, 1));
  list caller3;
  EXPECT_EQ(caller3.length(), 0u);
  list payload3 = make_single_part(4096);
  caller3 = std::move(payload3);
  EXPECT_EQ(caller3.length(), 4096u);
  EXPECT_EQ(payload3.length(), 0u);
}

}  // namespace
