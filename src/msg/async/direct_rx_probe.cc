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

#include "direct_rx_probe.h"

#include "Stack.h"
#include "common/perf_counters.h"

namespace ceph::msgr::v2 {

void record_direct_rx_probe_frame(PerfCounters* logger,
                                  const DirectRxFrameDesc& frame) {
  if (!logger) {
    return;
  }

  switch (classify_direct_rx_frame(frame)) {
  case DirectRxRejection::none:
    // Preamble-visible direct-path potential only -- not a full-eligibility
    // claim; see direct_rx_probe.h.
    logger->inc(l_msgr_direct_rx_potential_frames);
    logger->inc(l_msgr_direct_rx_potential_bytes, frame.data_len);
    return;
  case DirectRxRejection::session_crypto_active:
    logger->inc(l_msgr_direct_rx_fallback_crypto_frames);
    break;
  case DirectRxRejection::session_compression_active:
    logger->inc(l_msgr_direct_rx_fallback_compression_frames);
    break;
  case DirectRxRejection::data_crc_disabled:
    logger->inc(l_msgr_direct_rx_fallback_crc_disabled_frames);
    break;
  case DirectRxRejection::segment_layout_invalid:
    logger->inc(l_msgr_direct_rx_fallback_layout_frames);
    break;
  case DirectRxRejection::header_segment_invalid:
    logger->inc(l_msgr_direct_rx_fallback_header_frames);
    break;
  case DirectRxRejection::data_segment_absent:
    logger->inc(l_msgr_direct_rx_fallback_data_absent_frames);
    break;
  case DirectRxRejection::data_segment_empty:
    logger->inc(l_msgr_direct_rx_fallback_data_empty_frames);
    break;
  }

  logger->inc(l_msgr_direct_rx_fallback_frames);
  // frame.data_len is the declared DATA segment length, or 0 when the
  // layout does not carry one; fallback_bytes is therefore best-effort on
  // rejected layouts by design.
  logger->inc(l_msgr_direct_rx_fallback_bytes, frame.data_len);
}

} // namespace ceph::msgr::v2
