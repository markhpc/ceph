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

#ifndef _MSG_ASYNC_DIRECT_RX_PROBE_
#define _MSG_ASYNC_DIRECT_RX_PROBE_

#include <cstddef>
#include <cstdint>

// Declares TOPNSPC-namespaced PerfCounters with global-scope using-aliases,
// so the unqualified PerfCounters references below resolve.
#include "common/perf_counters.h"

namespace ceph::msgr::v2 {

/**
 * Benchmark/prototype-only accounting of preamble-visible *direct-path
 * potential* for a hypothetical msgr2 direct receive (delivering a
 * DATA-frame payload straight into a caller-provided buffer instead of the
 * stock staged-bufferlist copy path).
 *
 * NOTHING here changes payload handling, and nothing here claims full
 * direct-receive eligibility.  At preamble time the probe can only observe
 * session guards (negotiated crypto/compression handlers, data-CRC setting)
 * and the declared frame layout.  It cannot prove that the message is a
 * one-op plain OSD READ, that its DATA length matches an armed fixed-size
 * target, or that any target exists at all -- those need the later
 * target-registration/early-lookup phases.  "Potential" therefore means
 * only: the session guards pass and the frame declares a layout that a
 * direct DATA path could serve.
 *
 * FRONT and MIDDLE are legitimate metadata segments (e.g. a MOSDOpReply
 * always carries reply metadata in FRONT).  A future direct DATA path
 * preserves them and delivers only the DATA segment directly, so a non-empty
 * FRONT or MIDDLE is NOT a potential failure.  The only frame-shape
 * rejections are a malformed segment layout, a missing or truncated
 * envelope-header segment, an absent DATA segment, and a declared but empty
 * DATA segment.
 *
 * This is a local experimental fork feature; it carries no upstreamability
 * claim.  Enable it only through ms_benchmark_direct_rx_probe (off by
 * default).  When the option is off no probe call site is reached and all
 * probe counters stay zero, including msgr_direct_rx_hit_frames.
 */
enum class DirectRxRejection : uint8_t {
  none = 0,                   // frame shows preamble-visible direct-path
                              // potential (NOT full eligibility)
  session_crypto_active,      // rx crypto handler: frame is not plaintext
  session_compression_active, // rx compression handler: payload is compressed
  data_crc_disabled,          // ms_crc_data off: no data CRC to validate on
                              // direct delivery
  segment_layout_invalid,     // declared segment count outside 1..4
  header_segment_invalid,     // segment 0 absent/truncated, or expected
                              // header size unknown
  data_segment_absent,        // frame declares no DATA segment
  data_segment_empty,         // DATA declared but zero length
};

// MessageFrame segment slots: HEADER, FRONT, MIDDLE, DATA (see
// SegmentIndex::Msg in frames_v2.h; kept literal so this header stays
// payload-type-free).  A conforming DATA-carrying MessageFrame declares
// exactly this many segments because trailing empty segments are trimmed by
// the assembler.
inline constexpr std::size_t DIRECT_RX_MSG_NUM_SEGMENTS = 4;

// A preamble-time view of one incoming MESSAGE frame.  The caller fills it
// from FrameAssembler segment descriptors plus the session's negotiated
// handler state; lengths are declared segment logical lengths, and a
// segment index the frame does not declare must be reported as length 0.
// FRONT/MIDDLE lengths are deliberately not part of this descriptor: they
// never affect direct-path potential.
struct DirectRxFrameDesc {
  bool crypto_active = false;
  bool compression_active = false;
  bool data_crc = false;
  std::size_t num_segments = 0;
  uint32_t header_len = 0;
  uint32_t expected_header_len = 0;  // sizeof(ceph_msg_header2) at call site
  uint32_t data_len = 0;
};

/**
 * Pure classification; the first matching rule wins in the order
 * session_crypto_active, session_compression_active, data_crc_disabled,
 * segment_layout_invalid, header_segment_invalid, data_segment_absent,
 * data_segment_empty.  Returns DirectRxRejection::none (direct-path
 * potential) only when all session guards pass and the frame declares a
 * valid layout carrying a non-empty DATA segment.
 */
inline DirectRxRejection classify_direct_rx_frame(const DirectRxFrameDesc& f) {
  if (f.crypto_active) {
    return DirectRxRejection::session_crypto_active;
  }
  if (f.compression_active) {
    return DirectRxRejection::session_compression_active;
  }
  if (!f.data_crc) {
    return DirectRxRejection::data_crc_disabled;
  }
  if (f.num_segments < 1 || f.num_segments > DIRECT_RX_MSG_NUM_SEGMENTS) {
    return DirectRxRejection::segment_layout_invalid;
  }
  if (f.expected_header_len == 0 || f.header_len != f.expected_header_len) {
    return DirectRxRejection::header_segment_invalid;
  }
  if (f.num_segments < DIRECT_RX_MSG_NUM_SEGMENTS) {
    return DirectRxRejection::data_segment_absent;
  }
  if (f.data_len == 0) {
    return DirectRxRejection::data_segment_empty;
  }
  return DirectRxRejection::none;
}

constexpr const char* direct_rx_rejection_name(DirectRxRejection reason) {
  switch (reason) {
  case DirectRxRejection::none:
    return "none";
  case DirectRxRejection::session_crypto_active:
    return "session_crypto_active";
  case DirectRxRejection::session_compression_active:
    return "session_compression_active";
  case DirectRxRejection::data_crc_disabled:
    return "data_crc_disabled";
  case DirectRxRejection::segment_layout_invalid:
    return "segment_layout_invalid";
  case DirectRxRejection::header_segment_invalid:
    return "header_segment_invalid";
  case DirectRxRejection::data_segment_absent:
    return "data_segment_absent";
  case DirectRxRejection::data_segment_empty:
    return "data_segment_empty";
  }
  return "invalid";
}

/**
 * Classify one frame and accumulate the result into the worker's
 * PerfCounters (l_msgr_direct_rx_* indices): potential frames/bytes, and
 * for rejected frames the fallback frames/bytes plus exactly one reason
 * counter.  Observation only: never touches frame bytes.  A null logger is
 * ignored.  Invariants for one logger: every recorded frame increments
 * exactly one of potential/fallback frames; reason counters sum to fallback
 * frames; msgr_direct_rx_hit_frames has no writer and stays zero because no
 * direct delivery exists in this phase.
 */
void record_direct_rx_probe_frame(PerfCounters* logger,
                                  const DirectRxFrameDesc& frame);

} // namespace ceph::msgr::v2

#endif // _MSG_ASYNC_DIRECT_RX_PROBE_
