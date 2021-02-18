// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "KVManager.h"

void ClassicKVManager::start()
{
  dout(10) << __func__ << dendl;

  sync_thread.create("classic_kvm_sync");
  finalize_thread.create("classic_kvm_final");
}

void ClassicKVManager::stop()
{
  dout(10) << __func__ << dendl;
  {
    std::unique_lock l{sync_lock};
    while (!sync_started) {
      sync_cond.wait(l);
    }
    sync_stop = true;
    sync_cond.notify_all();
  }
  {
    std::unique_lock l{finalize_lock};
    while (!finalize_started) {
      finalize_cond.wait(l);
    }
    finalize_stop = true;
    finalize_cond.notify_all();
  }
  sync_thread.join();
  finalize_thread.join();
  //FIXME: This can't happen here anymore
  ceph_assert(removed_collections.empty());
  {
    std::lock_guard l(sync_lock);
    sync_stop = false;
  }
  {
    std::lock_guard l(finalize_lock);
    finalize_stop = false;
  }
  //FIXME: This can't happen here anymore either
  dout(10) << __func__ << " stopping finishers" << dendl;
  finisher.wait_for_empty();
  finisher.stop();
  dout(10) << __func__ << " stopped" << dendl;
}

void ClassicKVManager::_sync_thread()
{
  dout(10) << __func__ << " start" << dendl;
  deque<DeferredBatch*> deferred_stable_queue; ///< deferred ios done + stable
  std::unique_lock l{sync_lock};
  ceph_assert(!sync_started);
  sync_started = true;
  sync_cond.notify_all();

  auto t0 = mono_clock::now();
  timespan twait = ceph::make_timespan(0);
  size_t kv_submitted = 0;

  while (true) {
    auto period = cct->_conf->bluestore_kv_sync_util_logging_s;
    auto observation_period =
      ceph::make_timespan(period);
    auto elapsed = mono_clock::now() - t0;
    if (period && elapsed >= observation_period) {
      dout(5) << __func__ << " utilization: idle "
              << twait << " of " << elapsed
              << ", submitted: " << kv_submitted
              <<dendl;
      t0 = mono_clock::now();
      twait = ceph::make_timespan(0);
      kv_submitted = 0;
    }
    ceph_assert(kv_committing.empty());
    if (kv_queue.empty() &&
        ((deferred_done_queue.empty() && deferred_stable_queue.empty()) ||
         !deferred_aggressive)) {
      if (sync_stop)
        break; 
      dout(20) << __func__ << " sleep" << dendl;
      auto t = mono_clock::now();
      sync_in_progress = false;
      sync_cond.wait(l);
      twait += mono_clock::now() - t;

      dout(20) << __func__ << " wake" << dendl;
    } else {
     deque<TransContext*> kv_submitting;
      deque<DeferredBatch*> deferred_done, deferred_stable;
      uint64_t aios = 0, costs = 0;

      dout(20) << __func__ << " committing " << kv_queue.size()
               << " submitting " << kv_queue_unsubmitted.size()
               << " deferred done " << deferred_done_queue.size()
               << " stable " << deferred_stable_queue.size()
               << dendl;
      kv_committing.swap(kv_queue);
      kv_submitting.swap(kv_queue_unsubmitted);
      deferred_done.swap(deferred_done_queue);
      deferred_stable.swap(deferred_stable_queue);
      aios = kv_ios;
      costs = kv_throttle_costs;
      kv_ios = 0;
      kv_throttle_costs = 0;
      l.unlock();

      dout(30) << __func__ << " committing " << kv_committing << dendl;
      dout(30) << __func__ << " submitting " << kv_submitting << dendl;
      dout(30) << __func__ << " deferred_done " << deferred_done << dendl;
      dout(30) << __func__ << " deferred_stable " << deferred_stable << dendl;

      auto start = mono_clock::now();

      bool force_flush = false;
      // if bluefs is sharing the same device as data (only), then we
      // can rely on the bluefs commit to flush the device and make
      // deferred aios stable.  that means that if we do have done deferred
      // txcs AND we are not on a single device, we need to force a flush.
      if (bluefs && bluefs_layout.single_shared_device()) {
        if (aios) {
          force_flush = true;
        } else if (kv_committing.empty() && deferred_stable.empty()) {
          force_flush = true;  // there's nothing else to commit!
        } else if (deferred_aggressive) {
          force_flush = true;
        }
      } else {
        if (aios || !deferred_done.empty()) {
          force_flush = true;
        } else {
          dout(20) << __func__ << " skipping flush (no aios, no deferred_done)" << dendl;
        }
      }

      if (force_flush) {
        dout(20) << __func__ << " num_aios=" << aios
                 << " force_flush=" << (int)force_flush
                 << ", flushing, deferred done->stable" << dendl;
        // flush/barrier on block device
        //FIXME: Going to need access to bdev
        bdev->flush();

        // if we flush then deferred done are now deferred stable
        deferred_stable.insert(deferred_stable.end(), deferred_done.begin(),
                               deferred_done.end());
        deferred_done.clear();
      }
      auto after_flush = mono_clock::now();

      // we will use one final transaction to force a sync
      KeyValueDB::Transaction synct = db->get_transaction();

      // increase {nid,blobid}_max?  note that this covers both the
      // case where we are approaching the max and the case we passed
      // it.  in either case, we increase the max in the earlier txn
      // we submit.
      uint64_t new_nid_max = 0, new_blobid_max = 0;
      if (nid_last + cct->_conf->bluestore_nid_prealloc/2 > nid_max) {
        KeyValueDB::Transaction t =
          kv_submitting.empty() ? synct : kv_submitting.front()->t;
        new_nid_max = nid_last + cct->_conf->bluestore_nid_prealloc;
        bufferlist bl;
        encode(new_nid_max, bl);
        t->set(PREFIX_SUPER, "nid_max", bl);
        dout(10) << __func__ << " new_nid_max " << new_nid_max << dendl;
      }
      if (blobid_last + cct->_conf->bluestore_blobid_prealloc/2 > blobid_max) {
        KeyValueDB::Transaction t =
          kv_submitting.empty() ? synct : kv_submitting.front()->t;
        new_blobid_max = blobid_last + cct->_conf->bluestore_blobid_prealloc;
        bufferlist bl;
        encode(new_blobid_max, bl);
        t->set(PREFIX_SUPER, "blobid_max", bl);
        dout(10) << __func__ << " new_blobid_max " << new_blobid_max << dendl;
      }

      for (auto txc : kv_committing) {
        //FIXME: need access to throttle
        throttle.log_state_latency(*txc, logger, l_bluestore_state_kv_queued_lat);
        if (txc->get_state() == TransContext::STATE_KV_QUEUED) {
          ++kv_submitted;
          _txc_apply_kv(txc, false);
          --txc->osr->kv_committing_serially;
        } else {
          ceph_assert(txc->get_state() == TransContext::STATE_KV_SUBMITTED);
        }
        if (txc->had_ios) {
          --txc->osr->txc_with_unstable_io;
        }
      }

      // release throttle *before* we commit.  this allows new ops
      // to be prepared and enter pipeline while we are waiting on
      // the kv commit sync/flush.  then hopefully on the next
      // iteration there will already be ops awake.  otherwise, we
      // end up going to sleep, and then wake up when the very first
      // transaction is ready for commit.
      // FIXME: need access to throttle
      throttle.release_kv_throttle(costs);

      // cleanup sync deferred keys
      for (auto b : deferred_stable) {
        for (auto& txc : b->txcs) {
          bluestore_deferred_transaction_t& wt = *txc.deferred_txn;
          ceph_assert(wt.released.empty()); // only kraken did this
          string key;
          get_deferred_key(wt.seq, &key);
          synct->rm_single_key(PREFIX_DEFERRED, key);
        }
      }

#if defined(WITH_LTTNG)
      auto sync_start = mono_clock::now();
#endif
      // submit synct synchronously (block and wait for it to commit)
      int r = cct->_conf->bluestore_debug_omit_kv_commit ? 0 : db->submit_transaction_sync(synct);
      ceph_assert(r == 0);

#ifdef WITH_BLKIN
      for (auto txc : kv_committing) {
        if (txc->trace) {
          txc->trace.event("db sync submit");
          txc->trace.keyval("kv_committing size", kv_committing.size());
        }
      }
#endif

      int committing_size = kv_committing.size();
      int deferred_size = deferred_stable.size();

#if defined(WITH_LTTNG)
      double sync_latency = ceph::to_seconds<double>(mono_clock::now() - sync_start);
      for (auto txc: kv_committing) {
        if (txc->tracing) {
          tracepoint(
            bluestore,
            transaction_kv_sync_latency,
            txc->osr->get_sequencer_id(),
            txc->seq,
            kv_committing.size(),
            deferred_done.size(),
            deferred_stable.size(),
            sync_latency);
        }
      }
#endif

      {
        std::unique_lock m{finalize_lock};
        if (committing_to_finalize.empty()) {
          committing_to_finalize.swap(kv_committing);
        } else {
          committing_to_finalize.insert(
              committing_to_finalize.end(),
              kv_committing.begin(),
              kv_committing.end());
          kv_committing.clear();
        }
        if (deferred_stable_to_finalize.empty()) {
          deferred_stable_to_finalize.swap(deferred_stable);
        } else {
          deferred_stable_to_finalize.insert(
              deferred_stable_to_finalize.end(),
              deferred_stable.begin(),
              deferred_stable.end());
          deferred_stable.clear();
        }
        if (!finalize_in_progress) {
          finalize_in_progress = true;
          finalize_cond.notify_one();
        }
      }

      if (new_nid_max) {
        nid_max = new_nid_max;
        dout(10) << __func__ << " nid_max now " << nid_max << dendl;
      }
      if (new_blobid_max) {
        blobid_max = new_blobid_max;
        dout(10) << __func__ << " blobid_max now " << blobid_max << dendl;
      }

      {
        auto finish = mono_clock::now();
        ceph::timespan dur_flush = after_flush - start;
        ceph::timespan dur_kv = finish - after_flush;
        ceph::timespan dur = finish - start;
        dout(20) << __func__ << " committed " << committing_size
          << " cleaned " << deferred_size
          << " in " << dur
          << " (" << dur_flush << " flush + " << dur_kv << " kv commit)"
          << dendl;
        log_latency("kv_flush",
          l_bluestore_kv_flush_lat,
          dur_flush,
          cct->_conf->bluestore_log_op_age);
        log_latency("kv_commit",
          l_bluestore_kv_commit_lat,
          dur_kv,
          cct->_conf->bluestore_log_op_age);
        log_latency("kv_sync",
          l_bluestore_kv_sync_lat,
          dur,
          cct->_conf->bluestore_log_op_age);
      }

      l.lock();
      // previously deferred "done" are now "stable" by virtue of this
      // commit cycle.
      deferred_stable_queue.swap(deferred_done);
    }
  }
  dout(10) << __func__ << " finish" << dendl;
  sync_started = false;
}

void ClassicKVManager::_finalize_thread()
{
  deque<TransContext*> committed;
  deque<DeferredBatch*> deferred_stable;
  dout(10) << __func__ << " start" << dendl;
  std::unique_lock l(finalize_lock);
  ceph_assert(!finalize_started);
  finalize_started = true;
  finalize_cond.notify_all();
  while (true) {
    ceph_assert(committed.empty());
    ceph_assert(deferred_stable.empty());
    if (committing_to_finalize.empty() &&
        deferred_stable_to_finalize.empty()) {
      if (finalize_stop) {
        break;
      }
      dout(20) << __func__ << " sleep" << dendl;
      finalize_in_progress = false;
      finalize_cond.wait(l);
      dout(20) << __func__ << " wake" << dendl;
    } else {
      committed.swap(committing_to_finalize);
      deferred_stable.swap(deferred_stable_to_finalize);
      l.unlock();
      dout(20) << __func__ << " committed " << committed << dendl;
      dout(20) << __func__ << " deferred_stable " << deferred_stable << dendl;

      auto start = mono_clock::now();

      while (!committed.empty()) {
        TransContext *txc = committed.front();
        ceph_assert(txc->get_state() == TransContext::STATE_KV_SUBMITTED);
        //FIXME: Reach back into bluestore to call _txc_state_proc?
        _txc_state_proc(txc);
        committed.pop_front();
      }

      for (auto b : deferred_stable) {
        auto p = b->txcs.begin();
        while (p != b->txcs.end()) {
          TransContext *txc = &*p;
          p = b->txcs.erase(p); // unlink here because
          //FIXME: Reach back itno bluestore to call _txc_state_proc?
          _txc_state_proc(txc); // this may destroy txc
        }
        delete b;
      }
      deferred_stable.clear();

      if (!deferred_aggressive) {
        if (deferred_queue_size >= deferred_batch_ops.load() ||
            throttle.should_submit_deferred()) {
          deferred_try_submit();
        }
      }

      // this is as good a place as any ...
      //FIXME: Reach back into bluestore to call _reap_collections()? 
      _reap_collections();

      logger->set(l_bluestore_fragmentation,
          (uint64_t)(shared_alloc.a->get_fragmentation() * 1000));

      log_latency("kv_final",
        l_bluestore_kv_final_lat,
        mono_clock::now() - start,
        cct->_conf->bluestore_log_op_age);

      l.lock();
    }
  }
  dout(10) << __func__ << " finish" << dendl;
  finalize_started = false;
}

