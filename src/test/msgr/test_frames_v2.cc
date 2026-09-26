// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "msg/async/frames_v2.h"

#include <memory>
#include <numeric>
#include <ostream>
#include <string>
#include <tuple>

#include "msg/async/compression_meta.h"
#include "msg/async/direct_rx_probe.h"
#include "msg/async/Stack.h"
#include "common/perf_counters.h"
#include "auth/Auth.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "global/global_context.h"
#include "include/Context.h"

#include <gtest/gtest.h>

#define COMP_THRESHOLD 1 << 10
#define EXPECT_COMPRESSED(is_compressed, val1, val2) \
  if (is_compressed && val1 > COMP_THRESHOLD) { \
    EXPECT_GE(val1, val2); \
  } else { \
    EXPECT_EQ(val1, val2); \
  }

using namespace std;

namespace ceph::msgr::v2 {

// MessageFrame with the first segment not fixed to ceph_msg_header2
struct TestFrame : Frame<TestFrame,
                         /* four segments */
                         segment_t::DEFAULT_ALIGNMENT,
                         segment_t::DEFAULT_ALIGNMENT,
                         segment_t::DEFAULT_ALIGNMENT,
                         segment_t::PAGE_SIZE_ALIGNMENT> {
  static constexpr Tag tag = static_cast<Tag>(123);

  static TestFrame Encode(const bufferlist& header,
                          const bufferlist& front,
                          const bufferlist& middle,
                          const bufferlist& data) {
    TestFrame f;
    f.segments[SegmentIndex::Msg::HEADER] = header;
    f.segments[SegmentIndex::Msg::FRONT] = front;
    f.segments[SegmentIndex::Msg::MIDDLE] = middle;
    f.segments[SegmentIndex::Msg::DATA] = data;

    // discard cached crcs for perf tests
    f.segments[SegmentIndex::Msg::HEADER].invalidate_crc();
    f.segments[SegmentIndex::Msg::FRONT].invalidate_crc();
    f.segments[SegmentIndex::Msg::MIDDLE].invalidate_crc();
    f.segments[SegmentIndex::Msg::DATA].invalidate_crc();
    return f;
  }

  static TestFrame Decode(segment_bls_t& segment_bls) {
    TestFrame f;
    // Transfer segments' bufferlists.  If segment_bls contains
    // less than SegmentsNumV segments, the missing ones will be
    // seen as empty.
    for (size_t i = 0; i < segment_bls.size(); i++) {
      f.segments[i] = std::move(segment_bls[i]);
    }
    return f;
  }

  bufferlist& header() {
    return segments[SegmentIndex::Msg::HEADER];
  }
  bufferlist& front() {
    return segments[SegmentIndex::Msg::FRONT];
  }
  bufferlist& middle() {
    return segments[SegmentIndex::Msg::MIDDLE];
  }
  bufferlist& data() {
    return segments[SegmentIndex::Msg::DATA];
  }

protected:
  using Frame::Frame;
};

struct mode_t {
  bool is_rev1;
  bool is_secure;
  bool is_compress;
};

static std::ostream& operator<<(std::ostream& os, const mode_t& m) {
  os << "msgr2." << (m.is_rev1 ? "1" : "0")
     << (m.is_secure ? "-secure" : "-crc")
     << (m.is_compress ? "-compress": "-nocompress");
  return os;
}

static const mode_t modes[] = {
  {false, false, false},
  {false, true, false},
  {true, false, false},
  {true, true, false},
  {false, false, true},
  {false, true, true},
  {true, false, true},
  {true, true, true}
};

struct round_trip_instance_t {
  uint32_t header_len;
  uint32_t front_len;
  uint32_t middle_len;
  uint32_t data_len;

  // expected number of segments (same for each mode)
  size_t num_segments;
  // expected layout (different for each mode)
  uint32_t onwire_lens[4][MAX_NUM_SEGMENTS + 2];
};

static std::ostream& operator<<(std::ostream& os,
                                const round_trip_instance_t& rti) {
  os << rti.header_len << "+" << rti.front_len << "+"
     << rti.middle_len << "+" << rti.data_len;
  return os;
}

static bufferlist make_bufferlist(size_t len, char c) {
  bufferlist bl;
  if (len > 0) {
    bl.reserve(len);
    bl.append(std::string(len, c));
  }
  return bl;
}

bool disassemble_frame(FrameAssembler& frame_asm, bufferlist& frame_bl,
                       Tag& tag, segment_bls_t& segment_bls) {
  bufferlist preamble_bl;
  frame_bl.splice(0, frame_asm.get_preamble_onwire_len(), &preamble_bl);
  tag = frame_asm.disassemble_preamble(preamble_bl);

  do {
    size_t seg_idx = segment_bls.size();
    segment_bls.emplace_back();

    uint32_t onwire_len = frame_asm.get_segment_onwire_len(seg_idx);
    if (onwire_len > 0) {
      frame_bl.splice(0, onwire_len, &segment_bls.back());
    }
  } while (segment_bls.size() < frame_asm.get_num_segments());

  bufferlist epilogue_bl;
  uint32_t epilogue_onwire_len = frame_asm.get_epilogue_onwire_len();
  if (epilogue_onwire_len > 0) {
    frame_bl.splice(0, epilogue_onwire_len, &epilogue_bl);
  }
  
  return frame_asm.disassemble_segments(preamble_bl, segment_bls.data(), epilogue_bl);
}

class RoundTripTestBase : public ::testing::TestWithParam<
                              std::tuple<round_trip_instance_t, mode_t>> {
protected:
  RoundTripTestBase()
      : m_tx_frame_asm(&m_tx_crypto, std::get<1>(GetParam()).is_rev1, true,
                                                 &m_tx_comp),
        m_rx_frame_asm(&m_rx_crypto, std::get<1>(GetParam()).is_rev1, true,
                                                 &m_rx_comp),
        m_header(make_bufferlist(std::get<0>(GetParam()).header_len, 'H')),
        m_front(make_bufferlist(std::get<0>(GetParam()).front_len, 'F')),
        m_middle(make_bufferlist(std::get<0>(GetParam()).middle_len, 'M')),
        m_data(make_bufferlist(std::get<0>(GetParam()).data_len, 'D')) {
    const auto& m = std::get<1>(GetParam());
    if (m.is_secure) {
      AuthConnectionMeta auth_meta;
      auth_meta.con_mode = CEPH_CON_MODE_SECURE;
      // see AuthConnectionMeta::get_connection_secret_length()
      auth_meta.connection_secret.resize(64);
      g_ceph_context->random()->get_bytes(auth_meta.connection_secret.data(),
                                          auth_meta.connection_secret.size());
      m_tx_crypto = ceph::crypto::onwire::rxtx_t::create_handler_pair(
          g_ceph_context, auth_meta, /*new_nonce_format=*/m.is_rev1,
          /*crossed=*/false);
      m_rx_crypto = ceph::crypto::onwire::rxtx_t::create_handler_pair(
          g_ceph_context, auth_meta, /*new_nonce_format=*/m.is_rev1,
          /*crossed=*/true);
    }
    
    if (m.is_compress) {
      CompConnectionMeta comp_meta;
      comp_meta.con_mode = Compressor::COMP_FORCE;
      comp_meta.con_method = Compressor::COMP_ALG_SNAPPY;
      m_tx_comp = ceph::compression::onwire::rxtx_t::create_handler_pair(
        g_ceph_context, comp_meta, /*min_compress_size=*/COMP_THRESHOLD
      );
      m_rx_comp = ceph::compression::onwire::rxtx_t::create_handler_pair(
        g_ceph_context, comp_meta, /*min_compress_size=*/COMP_THRESHOLD
      );
    }
  }

  void check_frame_assembler(const FrameAssembler& frame_asm) {
    const auto& [rti, m] = GetParam();
    const auto& onwire_lens = rti.onwire_lens[m.is_rev1 << 1 | m.is_secure];

    EXPECT_COMPRESSED(m.is_compress, rti.header_len + rti.front_len + rti.middle_len + rti.data_len,
              frame_asm.get_frame_logical_len());
    ASSERT_EQ(rti.num_segments, frame_asm.get_num_segments());
    EXPECT_COMPRESSED(m.is_compress, onwire_lens[0], frame_asm.get_preamble_onwire_len());
    for (size_t i = 0; i < rti.num_segments; i++) {
      EXPECT_COMPRESSED(m.is_compress, onwire_lens[i + 1], frame_asm.get_segment_onwire_len(i));
    }
    EXPECT_COMPRESSED(m.is_compress, onwire_lens[rti.num_segments + 1],
              frame_asm.get_epilogue_onwire_len());
    EXPECT_COMPRESSED(m.is_compress,
                      std::accumulate(std::begin(onwire_lens), std::end(onwire_lens),
                                      uint64_t(0)),
                      frame_asm.get_frame_onwire_len());
  }

  void test_round_trip() {
    auto tx_frame = TestFrame::Encode(m_header, m_front, m_middle, m_data);
    auto onwire_bl = tx_frame.get_buffer(m_tx_frame_asm);
    check_frame_assembler(m_tx_frame_asm);
    EXPECT_EQ(m_tx_frame_asm.get_frame_onwire_len(), onwire_bl.length());

    Tag rx_tag;
    segment_bls_t rx_segment_bls;
    EXPECT_TRUE(disassemble_frame(m_rx_frame_asm, onwire_bl, rx_tag,
                                  rx_segment_bls));
    check_frame_assembler(m_rx_frame_asm);
    EXPECT_EQ(0, onwire_bl.length());
    EXPECT_EQ(TestFrame::tag, rx_tag);
    EXPECT_EQ(m_rx_frame_asm.get_num_segments(), rx_segment_bls.size());

    auto rx_frame = TestFrame::Decode(rx_segment_bls);
    EXPECT_TRUE(m_header.contents_equal(rx_frame.header()));
    EXPECT_TRUE(m_front.contents_equal(rx_frame.front()));
    EXPECT_TRUE(m_middle.contents_equal(rx_frame.middle()));
    EXPECT_TRUE(m_data.contents_equal(rx_frame.data()));
  }

  ceph::crypto::onwire::rxtx_t m_tx_crypto;
  ceph::crypto::onwire::rxtx_t m_rx_crypto;
  ceph::compression::onwire::rxtx_t m_tx_comp;
  ceph::compression::onwire::rxtx_t m_rx_comp;
  FrameAssembler m_tx_frame_asm;
  FrameAssembler m_rx_frame_asm;

  const bufferlist m_header;
  const bufferlist m_front;
  const bufferlist m_middle;
  const bufferlist m_data;
};

class RoundTripTest : public RoundTripTestBase {};

TEST_P(RoundTripTest, Basic) {
  test_round_trip();
}

TEST_P(RoundTripTest, Reuse) {
  for (int i = 0; i < 3; i++) {
    test_round_trip();
  }
}

static const round_trip_instance_t round_trip_instances[] = {
  // first segment is empty
  { 0,   0,   0,   0, 1, {{32,  0,  17,   0,   0,  0},
                          {32,  0,  32,   0,   0,  0},
                          {32,  0,   0,   0,   0,  0},
                          {96,  0,   0,   0,   0,  0}}},
  { 0,   0,   0, 303, 4, {{32,  0,   0,   0, 303, 17},
                          {32,  0,   0,   0, 304, 32},
                          {32,  0,   0,   0, 303, 13},
                          {96,  0,   0,   0, 304, 32}}},
  { 0,   0, 202,   0, 3, {{32,  0,   0, 202,  17,  0},
                          {32,  0,   0, 208,  32,  0},
                          {32,  0,   0, 202,  13,  0},
                          {96,  0,   0, 208,  32,  0}}},
  { 0,   0, 202, 303, 4, {{32,  0,   0, 202, 303, 17},
                          {32,  0,   0, 208, 304, 32},
                          {32,  0,   0, 202, 303, 13},
                          {96,  0,   0, 208, 304, 32}}},
  { 0, 101,   0,   0, 2, {{32,  0, 101,  17,   0,  0},
                          {32,  0, 112,  32,   0,  0},
                          {32,  0, 101,  13,   0,  0},
                          {96,  0, 112,  32,   0,  0}}},
  { 0, 101,   0, 303, 4, {{32,  0, 101,   0, 303, 17},
                          {32,  0, 112,   0, 304, 32},
                          {32,  0, 101,   0, 303, 13},
                          {96,  0, 112,   0, 304, 32}}},
  { 0, 101, 202,   0, 3, {{32,  0, 101, 202,  17,  0},
                          {32,  0, 112, 208,  32,  0},
                          {32,  0, 101, 202,  13,  0},
                          {96,  0, 112, 208,  32,  0}}},
  { 0, 101, 202, 303, 4, {{32,  0, 101, 202, 303, 17},
                          {32,  0, 112, 208, 304, 32},
                          {32,  0, 101, 202, 303, 13},
                          {96,  0, 112, 208, 304, 32}}},

  // first segment is fully inlined, inline buffer is not full
  { 1,   0,   0,   0, 1, {{32,  1,  17,   0,   0,  0},
                          {32, 16,  32,   0,   0,  0},
                          {32,  5,   0,   0,   0,  0},
                          {96,  0,   0,   0,   0,  0}}},
  { 1,   0,   0, 303, 4, {{32,  1,   0,   0, 303, 17},
                          {32, 16,   0,   0, 304, 32},
                          {32,  5,   0,   0, 303, 13},
                          {96,  0,   0,   0, 304, 32}}},
  { 1,   0, 202,   0, 3, {{32,  1,   0, 202,  17,  0},
                          {32, 16,   0, 208,  32,  0},
                          {32,  5,   0, 202,  13,  0},
                          {96,  0,   0, 208,  32,  0}}},
  { 1,   0, 202, 303, 4, {{32,  1,   0, 202, 303, 17},
                          {32, 16,   0, 208, 304, 32},
                          {32,  5,   0, 202, 303, 13},
                          {96,  0,   0, 208, 304, 32}}},
  { 1, 101,   0,   0, 2, {{32,  1, 101,  17,   0,  0},
                          {32, 16, 112,  32,   0,  0},
                          {32,  5, 101,  13,   0,  0},
                          {96,  0, 112,  32,   0,  0}}},
  { 1, 101,   0, 303, 4, {{32,  1, 101,   0, 303, 17},
                          {32, 16, 112,   0, 304, 32},
                          {32,  5, 101,   0, 303, 13},
                          {96,  0, 112,   0, 304, 32}}},
  { 1, 101, 202,   0, 3, {{32,  1, 101, 202,  17,  0},
                          {32, 16, 112, 208,  32,  0},
                          {32,  5, 101, 202,  13,  0},
                          {96,  0, 112, 208,  32,  0}}},
  { 1, 101, 202, 303, 4, {{32,  1, 101, 202, 303, 17},
                          {32, 16, 112, 208, 304, 32},
                          {32,  5, 101, 202, 303, 13},
                          {96,  0, 112, 208, 304, 32}}},

  // first segment is fully inlined, inline buffer is full
  {48,   0,   0,   0, 1, {{32, 48,  17,   0,   0,  0},
                          {32, 48,  32,   0,   0,  0},
                          {32, 52,   0,   0,   0,  0},
                          {96,  0,   0,   0,   0,  0}}},
  {48,   0,   0, 303, 4, {{32, 48,   0,   0, 303, 17},
                          {32, 48,   0,   0, 304, 32},
                          {32, 52,   0,   0, 303, 13},
                          {96,  0,   0,   0, 304, 32}}},
  {48,   0, 202,   0, 3, {{32, 48,   0, 202,  17,  0},
                          {32, 48,   0, 208,  32,  0},
                          {32, 52,   0, 202,  13,  0},
                          {96,  0,   0, 208,  32,  0}}},
  {48,   0, 202, 303, 4, {{32, 48,   0, 202, 303, 17},
                          {32, 48,   0, 208, 304, 32},
                          {32, 52,   0, 202, 303, 13},
                          {96,  0,   0, 208, 304, 32}}},
  {48, 101,   0,   0, 2, {{32, 48, 101,  17,   0,  0},
                          {32, 48, 112,  32,   0,  0},
                          {32, 52, 101,  13,   0,  0},
                          {96,  0, 112,  32,   0,  0}}},
  {48, 101,   0, 303, 4, {{32, 48, 101,   0, 303, 17},
                          {32, 48, 112,   0, 304, 32},
                          {32, 52, 101,   0, 303, 13},
                          {96,  0, 112,   0, 304, 32}}},
  {48, 101, 202,   0, 3, {{32, 48, 101, 202,  17,  0},
                          {32, 48, 112, 208,  32,  0},
                          {32, 52, 101, 202,  13,  0},
                          {96,  0, 112, 208,  32,  0}}},
  {48, 101, 202, 303, 4, {{32, 48, 101, 202, 303, 17},
                          {32, 48, 112, 208, 304, 32},
                          {32, 52, 101, 202, 303, 13},
                          {96,  0, 112, 208, 304, 32}}},

  // first segment is partially inlined
  {49,   0,   0,   0, 1, {{32, 49,  17,   0,   0,  0},
                          {32, 64,  32,   0,   0,  0},
                          {32, 53,   0,   0,   0,  0},
                          {96, 32,   0,   0,   0,  0}}},
  {49,   0,   0, 303, 4, {{32, 49,   0,   0, 303, 17},
                          {32, 64,   0,   0, 304, 32},
                          {32, 53,   0,   0, 303, 13},
                          {96, 32,   0,   0, 304, 32}}},
  {49,   0, 202,   0, 3, {{32, 49,   0, 202,  17,  0},
                          {32, 64,   0, 208,  32,  0},
                          {32, 53,   0, 202,  13,  0},
                          {96, 32,   0, 208,  32,  0}}},
  {49,   0, 202, 303, 4, {{32, 49,   0, 202, 303, 17},
                          {32, 64,   0, 208, 304, 32},
                          {32, 53,   0, 202, 303, 13},
                          {96, 32,   0, 208, 304, 32}}},
  {49, 101,   0,   0, 2, {{32, 49, 101,  17,   0,  0},
                          {32, 64, 112,  32,   0,  0},
                          {32, 53, 101,  13,   0,  0},
                          {96, 32, 112,  32,   0,  0}}},
  {49, 101,   0, 303, 4, {{32, 49, 101,   0, 303, 17},
                          {32, 64, 112,   0, 304, 32},
                          {32, 53, 101,   0, 303, 13},
                          {96, 32, 112,   0, 304, 32}}},
  {49, 101, 202,   0, 3, {{32, 49, 101, 202,  17,  0},
                          {32, 64, 112, 208,  32,  0},
                          {32, 53, 101, 202,  13,  0},
                          {96, 32, 112, 208,  32,  0}}},
  {49, 101, 202, 303, 4, {{32, 49, 101, 202, 303, 17},
                          {32, 64, 112, 208, 304, 32},
                          {32, 53, 101, 202, 303, 13},
                          {96, 32, 112, 208, 304, 32}}},
};

INSTANTIATE_TEST_SUITE_P(
    RoundTripTests, RoundTripTest, ::testing::Combine(
        ::testing::ValuesIn(round_trip_instances),
        ::testing::ValuesIn(modes)));

class RoundTripPerfTest : public RoundTripTestBase {};

TEST_P(RoundTripPerfTest, DISABLED_Basic) {
  for (int i = 0; i < 100000; i++) {
    auto tx_frame = TestFrame::Encode(m_header, m_front, m_middle, m_data);
    auto onwire_bl = tx_frame.get_buffer(m_tx_frame_asm);

    Tag rx_tag;
    segment_bls_t rx_segment_bls;
    ASSERT_TRUE(disassemble_frame(m_rx_frame_asm, onwire_bl, rx_tag,
                                  rx_segment_bls));
  }
}

static const round_trip_instance_t round_trip_perf_instances[] = {
  {41, 250, 0,       0, 2, {{32, 41, 250, 17,       0,  0},
                            {32, 48, 256, 32,       0,  0},
                            {32, 45, 250, 13,       0,  0},
                            {96,  0, 256, 32,       0,  0}}},
  {41, 250, 0,     512, 4, {{32, 41, 250,  0,     512, 17},
                            {32, 48, 256,  0,     512, 32},
                            {32, 45, 250,  0,     512, 13},
                            {96,  0, 256,  0,     512, 32}}},
  {41, 250, 0,    4096, 4, {{32, 41, 250,  0,    4096, 17},
                            {32, 48, 256,  0,    4096, 32},
                            {32, 45, 250,  0,    4096, 13},
                            {96,  0, 256,  0,    4096, 32}}},
  {41, 250, 0,   32768, 4, {{32, 41, 250,  0,   32768, 17},
                            {32, 48, 256,  0,   32768, 32},
                            {32, 45, 250,  0,   32768, 13},
                            {96,  0, 256,  0,   32768, 32}}},
  {41, 250, 0,  131072, 4, {{32, 41, 250,  0,  131072, 17},
                            {32, 48, 256,  0,  131072, 32},
                            {32, 45, 250,  0,  131072, 13},
                            {96,  0, 256,  0,  131072, 32}}},
  {41, 250, 0, 4194304, 4, {{32, 41, 250,  0, 4194304, 17},
                            {32, 48, 256,  0, 4194304, 32},
                            {32, 45, 250,  0, 4194304, 13},
                            {96,  0, 256,  0, 4194304, 32}}},
};

INSTANTIATE_TEST_SUITE_P(
    RoundTripPerfTests, RoundTripPerfTest, ::testing::Combine(
        ::testing::ValuesIn(round_trip_perf_instances),
        ::testing::ValuesIn(modes)));

// ---------------------------------------------------------------------------
// Benchmark-only direct-receive probe (ms_benchmark_direct_rx_probe):
// deterministic, cluster-free coverage of the preamble-visible
// direct-path-potential classification and of counter accounting through a
// real PerfCounters logger registered exactly the way the async Worker
// registers it.  The probe never touches payload handling and never claims
// full direct-read eligibility: "potential" only means the session guards
// pass and the frame declares a valid DATA-carrying layout.  FRONT/MIDDLE
// metadata segments (e.g. a MOSDOpReply front) are preserved for a future
// direct DATA path and must never disqualify a frame.
// ---------------------------------------------------------------------------

static DirectRxFrameDesc make_probe_potential(uint32_t data_len) {
  DirectRxFrameDesc f;
  // plaintext, uncompressed, data-crc-enabled session defaults:
  f.data_crc = true;
  // valid envelope-header segment plus one declared, non-empty DATA
  // segment; FRONT/MIDDLE never enter the descriptor:
  f.num_segments = DIRECT_RX_MSG_NUM_SEGMENTS;
  f.header_len = sizeof(ceph_msg_header2);
  f.expected_header_len = sizeof(ceph_msg_header2);
  f.data_len = data_len;
  return f;
}

TEST(DirectRxProbe, DataCarryingFrameHasPotential) {
  EXPECT_EQ(classify_direct_rx_frame(make_probe_potential(4096)),
            DirectRxRejection::none);
}

TEST(DirectRxProbe, SessionGuards) {
  auto f = make_probe_potential(4096);
  f.crypto_active = true;
  EXPECT_EQ(classify_direct_rx_frame(f),
            DirectRxRejection::session_crypto_active);
  f.crypto_active = false;
  f.compression_active = true;
  EXPECT_EQ(classify_direct_rx_frame(f),
            DirectRxRejection::session_compression_active);
  f.compression_active = false;
  f.data_crc = false;
  EXPECT_EQ(classify_direct_rx_frame(f),
            DirectRxRejection::data_crc_disabled);
}

TEST(DirectRxProbe, FrameLayoutRejections) {
  // malformed layout: declared segment count outside 1..4
  auto f = make_probe_potential(4096);
  f.num_segments = 0;
  EXPECT_EQ(classify_direct_rx_frame(f),
            DirectRxRejection::segment_layout_invalid);
  f = make_probe_potential(4096);
  f.num_segments = DIRECT_RX_MSG_NUM_SEGMENTS + 1;
  EXPECT_EQ(classify_direct_rx_frame(f),
            DirectRxRejection::segment_layout_invalid);

  // absent/invalid header segment
  f = make_probe_potential(4096);
  f.header_len = sizeof(ceph_msg_header2) - 1;  // truncated envelope header
  EXPECT_EQ(classify_direct_rx_frame(f),
            DirectRxRejection::header_segment_invalid);
  f = make_probe_potential(4096);
  f.expected_header_len = 0;  // degenerate call site: header unprovable
  EXPECT_EQ(classify_direct_rx_frame(f),
            DirectRxRejection::header_segment_invalid);

  // absent DATA segment: trailing empty segments are trimmed, so a
  // metadata-only reply legitimately declares fewer than 4 segments
  f = make_probe_potential(4096);
  f.num_segments = 3;
  f.data_len = 0;
  EXPECT_EQ(classify_direct_rx_frame(f),
            DirectRxRejection::data_segment_absent);

  // declared but empty DATA segment is a distinct rejection
  EXPECT_EQ(classify_direct_rx_frame(make_probe_potential(0)),
            DirectRxRejection::data_segment_empty);
}

TEST(DirectRxProbe, ReasonPrecedence) {
  // first matching rule wins: session crypto, session compression,
  // data CRC, layout, header, DATA presence, DATA emptiness
  auto f = make_probe_potential(0);
  f.num_segments = 9;
  f.crypto_active = true;
  f.compression_active = true;
  f.data_crc = false;
  EXPECT_EQ(classify_direct_rx_frame(f),
            DirectRxRejection::session_crypto_active);
  f.crypto_active = false;
  EXPECT_EQ(classify_direct_rx_frame(f),
            DirectRxRejection::session_compression_active);
  f.compression_active = false;
  EXPECT_EQ(classify_direct_rx_frame(f),
            DirectRxRejection::data_crc_disabled);
  f.data_crc = true;
  EXPECT_EQ(classify_direct_rx_frame(f),
            DirectRxRejection::segment_layout_invalid);
  f.num_segments = DIRECT_RX_MSG_NUM_SEGMENTS;
  f.expected_header_len = 0;
  EXPECT_EQ(classify_direct_rx_frame(f),
            DirectRxRejection::header_segment_invalid);
  f.expected_header_len = sizeof(ceph_msg_header2);
  f.num_segments = 1;  // header only: header valid, DATA absent
  EXPECT_EQ(classify_direct_rx_frame(f),
            DirectRxRejection::data_segment_absent);
  f.num_segments = DIRECT_RX_MSG_NUM_SEGMENTS;  // DATA declared but empty
  EXPECT_EQ(classify_direct_rx_frame(f),
            DirectRxRejection::data_segment_empty);
}

TEST(DirectRxProbe, RejectionNames) {
  EXPECT_STREQ(direct_rx_rejection_name(DirectRxRejection::none), "none");
  EXPECT_STREQ(direct_rx_rejection_name(
                   DirectRxRejection::session_crypto_active),
               "session_crypto_active");
  EXPECT_STREQ(direct_rx_rejection_name(
                   DirectRxRejection::session_compression_active),
               "session_compression_active");
  EXPECT_STREQ(direct_rx_rejection_name(
                   DirectRxRejection::data_crc_disabled),
               "data_crc_disabled");
  EXPECT_STREQ(direct_rx_rejection_name(
                   DirectRxRejection::segment_layout_invalid),
               "segment_layout_invalid");
  EXPECT_STREQ(direct_rx_rejection_name(
                   DirectRxRejection::header_segment_invalid),
               "header_segment_invalid");
  EXPECT_STREQ(direct_rx_rejection_name(
                   DirectRxRejection::data_segment_absent),
               "data_segment_absent");
  EXPECT_STREQ(direct_rx_rejection_name(
                   DirectRxRejection::data_segment_empty),
               "data_segment_empty");
}

// A real msgr2.1 crc-mode frame with non-empty FRONT and MIDDLE metadata
// (MOSDOpReply-style) is disassembled exactly as far as ProtocolV2 does at
// preamble time, and classifies as potential: the probe never looks at the
// metadata segments.  The metadata-only variant, whose trailing empty DATA
// segment gets trimmed by the assembler, rejects as data_segment_absent.
TEST(DirectRxProbe, RealFramePreambleWithMetadataKeepsPotential) {
  ceph::crypto::onwire::rxtx_t tx_crypto, rx_crypto;
  ceph::compression::onwire::rxtx_t tx_comp, rx_comp;
  FrameAssembler tx_asm(&tx_crypto, true, true, &tx_comp);
  FrameAssembler rx_asm(&rx_crypto, true, true, &rx_comp);

  const auto probe_desc = [&rx_asm]() {
    DirectRxFrameDesc desc;
    desc.data_crc = rx_asm.get_with_data_crc();
    desc.num_segments = rx_asm.get_num_segments();
    desc.expected_header_len = sizeof(ceph_msg_header2);
    const auto len = [&rx_asm](std::size_t i) -> uint32_t {
      return i < rx_asm.get_num_segments()
                 ? rx_asm.get_segment_logical_len(i)
                 : 0;
    };
    desc.header_len = len(SegmentIndex::Msg::HEADER);
    desc.data_len = len(SegmentIndex::Msg::DATA);
    return desc;
  };

  ceph_msg_header2 hdr{};
  auto reply = MessageFrame::Encode(hdr, make_bufferlist(64, 'F'),
                                    make_bufferlist(32, 'M'),
                                    make_bufferlist(4096, 'D'));
  auto frame_bl = reply.get_buffer(tx_asm);
  bufferlist preamble_bl;
  frame_bl.splice(0, rx_asm.get_preamble_onwire_len(), &preamble_bl);
  ASSERT_EQ(rx_asm.disassemble_preamble(preamble_bl), Tag::MESSAGE);

  auto desc = probe_desc();
  EXPECT_EQ(desc.num_segments, DIRECT_RX_MSG_NUM_SEGMENTS);
  EXPECT_EQ(desc.header_len, sizeof(ceph_msg_header2));
  EXPECT_EQ(desc.data_len, 4096u);
  EXPECT_EQ(classify_direct_rx_frame(desc), DirectRxRejection::none);

  auto ack = MessageFrame::Encode(hdr, make_bufferlist(64, 'F'),
                                  make_bufferlist(0, 'M'),
                                  make_bufferlist(0, 'D'));
  auto ack_bl = ack.get_buffer(tx_asm);
  bufferlist ack_preamble;
  ack_bl.splice(0, rx_asm.get_preamble_onwire_len(), &ack_preamble);
  ASSERT_EQ(rx_asm.disassemble_preamble(ack_preamble), Tag::MESSAGE);

  auto ack_desc = probe_desc();
  EXPECT_EQ(ack_desc.num_segments, 2u);  // HEADER + FRONT, DATA trimmed
  EXPECT_EQ(classify_direct_rx_frame(ack_desc),
            DirectRxRejection::data_segment_absent);
}

TEST(DirectRxProbe, CounterAccountingThroughPerfCounters) {
  // create_perf_counters() asserts every slot in (first, last) is
  // registered; the production Worker registers the entire range, so the
  // test instead builds a tight window over exactly the probe counters.
  // The pin below requires the probe block to end at l_msgr_last with no
  // gaps, so a future counter cannot slip in unregistered.
  static_assert(l_msgr_direct_rx_potential_frames < l_msgr_last);
  static_assert(l_msgr_direct_rx_fallback_data_empty_frames + 1 ==
                l_msgr_last);
  PerfCountersBuilder plb(g_ceph_context,
                          "AsyncMessenger::Worker-direct-rx-probe-test",
                          l_msgr_direct_rx_potential_frames - 1,
                          l_msgr_last);
  add_direct_rx_probe_counters(plb);
  std::unique_ptr<PerfCounters> logger(plb.create_perf_counters());
  ASSERT_TRUE(g_ceph_context->_conf->perf);  // accumulation gate

  // documented no-op, must not crash or touch anything
  record_direct_rx_probe_frame(nullptr, make_probe_potential(64));

  // potential frames still take the stock copy path in this phase: the
  // only delivery counter stays structurally zero
  record_direct_rx_probe_frame(logger.get(), make_probe_potential(4096));
  record_direct_rx_probe_frame(logger.get(), make_probe_potential(8192));
  EXPECT_EQ(logger->get(l_msgr_direct_rx_potential_frames), 2u);
  EXPECT_EQ(logger->get(l_msgr_direct_rx_potential_bytes), 12288u);
  EXPECT_EQ(logger->get(l_msgr_direct_rx_hit_frames), 0u);
  EXPECT_EQ(logger->get(l_msgr_direct_rx_fallback_frames), 0u);

  auto crypto = make_probe_potential(100);
  crypto.crypto_active = true;
  record_direct_rx_probe_frame(logger.get(), crypto);

  auto layout = make_probe_potential(0);
  layout.num_segments = 0;
  record_direct_rx_probe_frame(logger.get(), layout);

  auto header = make_probe_potential(7);
  header.header_len = sizeof(ceph_msg_header2) - 1;
  record_direct_rx_probe_frame(logger.get(), header);

  auto absent = make_probe_potential(0);
  absent.num_segments = 3;
  record_direct_rx_probe_frame(logger.get(), absent);

  record_direct_rx_probe_frame(logger.get(), make_probe_potential(0));

  EXPECT_EQ(logger->get(l_msgr_direct_rx_fallback_frames), 5u);
  // DATA bytes are attributed only when the layout declares a DATA
  // segment: crypto rejection (100) and header rejection (7)
  EXPECT_EQ(logger->get(l_msgr_direct_rx_fallback_bytes), 107u);
  EXPECT_EQ(logger->get(l_msgr_direct_rx_fallback_crypto_frames), 1u);
  EXPECT_EQ(logger->get(l_msgr_direct_rx_fallback_layout_frames), 1u);
  EXPECT_EQ(logger->get(l_msgr_direct_rx_fallback_header_frames), 1u);
  EXPECT_EQ(logger->get(l_msgr_direct_rx_fallback_data_absent_frames), 1u);
  EXPECT_EQ(logger->get(l_msgr_direct_rx_fallback_data_empty_frames), 1u);
  EXPECT_EQ(logger->get(l_msgr_direct_rx_fallback_compression_frames), 0u);
  EXPECT_EQ(logger->get(l_msgr_direct_rx_fallback_crc_disabled_frames), 0u);

  // potential stays exact: no double counting between the classes
  EXPECT_EQ(logger->get(l_msgr_direct_rx_potential_frames), 2u);
  EXPECT_EQ(logger->get(l_msgr_direct_rx_potential_bytes), 12288u);

  // accounting invariants: the reason counters sum to the fallback frames,
  // potential + fallback equals every recorded frame, and hits stay zero
  const uint64_t reasons =
      logger->get(l_msgr_direct_rx_fallback_crypto_frames) +
      logger->get(l_msgr_direct_rx_fallback_compression_frames) +
      logger->get(l_msgr_direct_rx_fallback_crc_disabled_frames) +
      logger->get(l_msgr_direct_rx_fallback_layout_frames) +
      logger->get(l_msgr_direct_rx_fallback_header_frames) +
      logger->get(l_msgr_direct_rx_fallback_data_absent_frames) +
      logger->get(l_msgr_direct_rx_fallback_data_empty_frames);
  EXPECT_EQ(reasons, logger->get(l_msgr_direct_rx_fallback_frames));
  EXPECT_EQ(logger->get(l_msgr_direct_rx_potential_frames) +
                logger->get(l_msgr_direct_rx_fallback_frames),
            7u);
  EXPECT_EQ(logger->get(l_msgr_direct_rx_hit_frames), 0u);
}

TEST(DirectRxProbe, BenchmarkProbeIsDisabledByDefault) {
  // the shipped default must be off: unarmed/default runs reach no probe
  // call site, so every counter above stays zero
  ASSERT_FALSE(g_ceph_context->_conf->ms_benchmark_direct_rx_probe);
}

}  // namespace ceph::msgr::v2

int main(int argc, char* argv[]) {
  auto args = argv_to_vec(argc, argv);

  auto cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_UTILITY,
                         CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
