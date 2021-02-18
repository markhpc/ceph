// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_OS_BLUESTORE_KVMANAGER_H
#define CEPH_OS_BLUESTORE_KVMANAGER_H

class KVManager {
public:
  CephContext* cct;
  KVManager(CephContext* cct) :: cct(cct) {}
  virtual ~KVManager() {}
  virtual void wake() = 0;
  virtual void submit_queue(TransContext *txc) = 0;
  virtual void submit_deferred_done(DefferedBatch *b) = 0;
};

class ClassicKVManager : public KVManager {
  struct SyncThread : public Thread {
    ClassicKVManager *kvm;
    explicit SyncThread(ClassicKVManager *m) : kvm(m) {}
    void *entry() override {
      kvm->_sync_thread();
      return NULL;
    }
  } sync_thread;
  struct FinalizeThread : public Thread {
    ClassicKVManger *kvm;
    explicit FinalizeThread(ClassicKVManager *m) : kvm(m) {}
    void *entry() override {
      kvm->_finalize_thread();
      return NULL;
    }
  } finalize_thread;

  SyncThread sync_thread;
  ceph::mutex sync_lock = ceph::make_mutex("ClassicKVManager::sync_lock");
  ceph::condition_variable sync_cond;
  bool _kv_only = false;
  bool sync_started = false;
  bool sync_stop = false;
  std::deque<TransContext*> kv_queue;             ///< ready, already submitted
  std::deque<TransContext*> kv_queue_unsubmitted; ///< ready, need submit by kv thread
  std::deque<TransContext*> committing;           ///< currently syncing
  std::deque<DeferredBatch*> deferred_done_queue; ///< deferred ios done
  bool sync_in_progress = false;

  FinalizeThread finalize_thread;
  ceph::mutex finalize_lock = ceph::make_mutex("ClassicKVManager::finalize_lock");
  ceph::condition_variable finalize_cond;
  bool finalize_started = false;
  bool finalize_stop = false;
  std::deque<TransContext*> committing_to_finalize;   ///< pending finalization
  std::deque<DeferredBatch*> deferred_stable_to_finalize; ///< pending finalization
  bool finalize_in_progress = false;

public:
  void wake() override {
    std::lock_guard l(sync_lock);
    _wake();
  }

  void submit_queue(TransContext *txc) override {
    std::lock_guard l(sync_lock);
    _submit_queue(txc);
  }

  void submit_deferred_done(DeferredBatch *b) override {
    std::lock_guard l(sync_lock);
    _submit_deferred_done(b);
  }

  void _start();
  void _stop();
  void _sync_thread();
  void _finalize_thread();

  void _wake() {
    if (!sync_in_progress) {
      sync_in_progress = true;
      sync_cond.notify_one();
    }
  }

  void _submit_queue(TransContext *txc) {
    kv_queue.push_back(txc);
    _wake();
    if (txc->get_state() != TransContext::STATE_KV_SUBMITTED) {
      kv_queue_unsubmitted.push_back(txc);
      ++txc->osr->kv_committing_serially;
    }
    if (txc->had_ios) {
      kv_ios++;
    }
    kv_throttle_costs += txc->cost;
  }

  void _submit_deferred_done(DeferredBatch *b) override {
    deferred_done_queue.emplace_back(b);
    if (deferred_aggressive) {
      _wake();
    }
  }
};

#endif

