// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef KEY_VALUE_DB_H
#define KEY_VALUE_DB_H

#include "include/buffer.h"
#include <ostream>
#include <set>
#include <map>
#include <string>
#include "include/memory.h"
#include <boost/scoped_ptr.hpp>
#include "include/encoding.h"
#include "common/Formatter.h"
#include "common/perf_counters.h"
#include "common/PriorityCache.h"

using std::string;
using std::vector;
/**
 * Defines virtual interface to be implemented by key value store
 *
 * Kyoto Cabinet or LevelDB should implement this
 */
class KeyValueDB : public PriorityCache::PriCache {
public:
  /*
   *  See RocksDB's definition of a column family(CF) and how to use it.
   *  The interfaces of KeyValueDB is extended, when a column family is created.
   *  Prefix will be the name of column family to use.
   */
  struct ColumnFamily {
    string name;      //< name of this individual column family
    string option;    //< configure option string for this CF
    ColumnFamily(const string &name, const string &option)
      : name(name), option(option) {}
  };

  class TransactionImpl {
  public:
    /// Set Keys
    void set(
      const std::string &prefix,                      ///< [in] Prefix for keys, or CF name
      const std::map<std::string, bufferlist> &to_set ///< [in] keys/values to set
    ) {
      std::map<std::string, bufferlist>::const_iterator it;
      for (it = to_set.begin(); it != to_set.end(); ++it)
	set(prefix, it->first, it->second);
    }

    /// Set Keys (via encoded bufferlist)
    void set(
      const std::string &prefix,      ///< [in] prefix, or CF name
      bufferlist& to_set_bl           ///< [in] encoded key/values to set
      ) {
      bufferlist::iterator p = to_set_bl.begin();
      uint32_t num;
      decode(num, p);
      while (num--) {
	string key;
	bufferlist value;
	decode(key, p);
	decode(value, p);
	set(prefix, key, value);
      }
    }

    /// Set Key
    virtual void set(
      const std::string &prefix,      ///< [in] Prefix or CF for the key
      const std::string &k,	      ///< [in] Key to set
      const bufferlist &bl            ///< [in] Value to set
      ) = 0;
    virtual void set(
      const std::string &prefix,
      const char *k,
      size_t keylen,
      const bufferlist& bl) {
      set(prefix, string(k, keylen), bl);
    }

    /// Removes Keys (via encoded bufferlist)
    void rmkeys(
      const std::string &prefix,     ///< [in] Prefix or CF to search for
      bufferlist &keys_bl            ///< [in] Keys to remove
    ) {
      bufferlist::iterator p = keys_bl.begin();
      uint32_t num;
      decode(num, p);
      while (num--) {
	string key;
	decode(key, p);
	rmkey(prefix, key);
      }
    }

    /// Removes Keys
    void rmkeys(
      const std::string &prefix,        ///< [in] Prefix/CF to search for
      const std::set<std::string> &keys ///< [in] Keys to remove
    ) {
      std::set<std::string>::const_iterator it;
      for (it = keys.begin(); it != keys.end(); ++it)
	rmkey(prefix, *it);
    }

    /// Remove Key
    virtual void rmkey(
      const std::string &prefix,       ///< [in] Prefix/CF to search for
      const std::string &k	       ///< [in] Key to remove
      ) = 0;
    virtual void rmkey(
      const std::string &prefix,   ///< [in] Prefix to search for
      const char *k,	      ///< [in] Key to remove
      size_t keylen
      ) {
      rmkey(prefix, string(k, keylen));
    }

    /// Remove Single Key which exists and was not overwritten.
    /// This API is only related to performance optimization, and should only be 
    /// re-implemented by log-insert-merge tree based keyvalue stores(such as RocksDB). 
    /// If a key is overwritten (by calling set multiple times), then the result
    /// of calling rm_single_key on this key is undefined.
    virtual void rm_single_key(
      const std::string &prefix,      ///< [in] Prefix/CF to search for
      const std::string &k	      ///< [in] Key to remove
      ) { return rmkey(prefix, k);}

    /// Removes keys beginning with prefix
    virtual void rmkeys_by_prefix(
      const std::string &prefix       ///< [in] Prefix/CF by which to remove keys
      ) = 0;

    virtual void rm_range_keys(
      const string &prefix,    ///< [in] Prefix by which to remove keys
      const string &start,     ///< [in] The start bound of remove keys
      const string &end        ///< [in] The start bound of remove keys
      ) = 0;

    /// Merge value into key
    virtual void merge(
      const std::string &prefix,   ///< [in] Prefix/CF ==> MUST match some established merge operator
      const std::string &key,      ///< [in] Key to be merged
      const bufferlist  &value     ///< [in] value to be merged into key
    ) { assert(0 == "Not implemented"); }

    virtual ~TransactionImpl() {}
  };
  typedef ceph::shared_ptr< TransactionImpl > Transaction;

  /// create a new instance
  static KeyValueDB *create(CephContext *cct, const std::string& type,
			    const std::string& dir,
			    map<std::string,std::string> options = {},
			    void *p = NULL);

  /// test whether we can successfully initialize; may have side effects (e.g., create)
  static int test_init(const std::string& type, const std::string& dir);
  virtual int init(string option_str="") = 0;
  virtual int open(std::ostream &out, const vector<ColumnFamily>& cfs = {}) = 0;
  // vector cfs contains column families to be created when db is created.
  virtual int create_and_open(std::ostream &out,
			      const vector<ColumnFamily>& cfs = {}) = 0;
  virtual void close() { }

  /// Try to repair K/V database. leveldb and rocksdb require that database must be not opened.
  virtual int repair(std::ostream &out) { return 0; }

  virtual Transaction get_transaction() = 0;
  virtual int submit_transaction(Transaction) = 0;
  virtual int submit_transaction_sync(Transaction t) {
    return submit_transaction(t);
  }

  /// Retrieve Keys
  virtual int get(
    const std::string &prefix,               ///< [in] Prefix/CF for key
    const std::set<std::string> &key,        ///< [in] Key to retrieve
    std::map<std::string, bufferlist> *out   ///< [out] Key value retrieved
    ) = 0;
  virtual int get(const std::string &prefix, ///< [in] prefix or CF name
		  const std::string &key,    ///< [in] key
		  bufferlist *value) {       ///< [out] value
    std::set<std::string> ks;
    ks.insert(key);
    std::map<std::string,bufferlist> om;
    int r = get(prefix, ks, &om);
    if (om.find(key) != om.end()) {
      *value = om[key];
    } else {
      *value = bufferlist();
      r = -ENOENT;
    }
    return r;
  }
  virtual int get(const string &prefix,
		  const char *key, size_t keylen,
		  bufferlist *value) {
    return get(prefix, string(key, keylen), value);
  }

  // This superclass is used both by kv iterators *and* by the ObjectMap
  // omap iterator.  The class hiearchies are unfortunatley tied together
  // by the legacy DBOjectMap implementation :(.
  class SimplestIteratorImpl {
  public:
    virtual int seek_to_first() = 0;
    virtual int upper_bound(const std::string &after) = 0;
    virtual int lower_bound(const std::string &to) = 0;
    virtual bool valid() = 0;
    virtual int next(bool validate=true) = 0;
    virtual std::string key() = 0;
    virtual bufferlist value() = 0;
    virtual int status() = 0;
    virtual ~SimplestIteratorImpl() {}
  };

  class IteratorImpl : public SimplestIteratorImpl {
  public:
    virtual ~IteratorImpl() {}
    virtual int seek_to_last() = 0;
    virtual int prev(bool validate=true) = 0;
    virtual std::pair<std::string, std::string> raw_key() = 0;
    virtual bufferptr value_as_ptr() {
      bufferlist bl = value();
      if (bl.length() == 1) {
        return *bl.buffers().begin();
      } else if (bl.length() == 0) {
        return bufferptr();
      } else {
	ceph_abort();
      }
    }
  };
  typedef ceph::shared_ptr< IteratorImpl > Iterator;

  // This is the low-level iterator implemented by the underlying KV store.
  class WholeSpaceIteratorImpl {
  public:
    virtual int seek_to_first() = 0;
    virtual int seek_to_first(const std::string &prefix) = 0;
    virtual int seek_to_last() = 0;
    virtual int seek_to_last(const std::string &prefix) = 0;
    virtual int upper_bound(const std::string &prefix, const std::string &after) = 0;
    virtual int lower_bound(const std::string &prefix, const std::string &to) = 0;
    virtual bool valid() = 0;
    virtual int next() = 0;
    virtual int prev() = 0;
    virtual std::string key() = 0;
    virtual std::pair<std::string,std::string> raw_key() = 0;
    virtual bool raw_key_is_prefixed(const std::string &prefix) = 0;
    virtual bufferlist value() = 0;
    virtual bufferptr value_as_ptr() {
      bufferlist bl = value();
      if (bl.length()) {
        return *bl.buffers().begin();
      } else {
        return bufferptr();
      }
    }
    virtual int status() = 0;
    virtual size_t key_size() {
      return 0;
    }
    virtual size_t value_size() {
      return 0;
    }
    virtual ~WholeSpaceIteratorImpl() { }
  };
  typedef ceph::shared_ptr< WholeSpaceIteratorImpl > WholeSpaceIterator;

private:
  int64_t cache_bytes[PriorityCache::Priority::LAST+1] = { 0 };
  double cache_ratio = 0;

  // This class filters a WholeSpaceIterator by a prefix.
  class PrefixIteratorImpl : public IteratorImpl {
    const std::string prefix;
    WholeSpaceIterator generic_iter;
  public:
    PrefixIteratorImpl(const std::string &prefix, WholeSpaceIterator iter) :
      prefix(prefix), generic_iter(iter) { }
    ~PrefixIteratorImpl() override { }

    int seek_to_first() override {
      return generic_iter->seek_to_first(prefix);
    }
    int seek_to_last() override {
      return generic_iter->seek_to_last(prefix);
    }
    int upper_bound(const std::string &after) override {
      return generic_iter->upper_bound(prefix, after);
    }
    int lower_bound(const std::string &to) override {
      return generic_iter->lower_bound(prefix, to);
    }
    bool valid() override {
      if (!generic_iter->valid())
	return false;
      return generic_iter->raw_key_is_prefixed(prefix);
    }
    // Note that next() and prev() shouldn't validate iters,
    // it's responsibility of caller to ensure they're valid.
    int next(bool validate=true) override {
      if (validate) {
        if (valid())
          return generic_iter->next();
        return status();
      } else {
        return generic_iter->next();  
      }      
    }
    
    int prev(bool validate=true) override {
      if (validate) {
        if (valid())
          return generic_iter->prev();
        return status();
      } else {
        return generic_iter->prev();  
      }      
    }
    std::string key() override {
      return generic_iter->key();
    }
    std::pair<std::string, std::string> raw_key() override {
      return generic_iter->raw_key();
    }
    bufferlist value() override {
      return generic_iter->value();
    }
    bufferptr value_as_ptr() override {
      return generic_iter->value_as_ptr();
    }
    int status() override {
      return generic_iter->status();
    }
  };
public:

  virtual WholeSpaceIterator get_wholespace_iterator() = 0;
  virtual Iterator get_iterator(const std::string &prefix) {
    return std::make_shared<PrefixIteratorImpl>(
      prefix,
      get_wholespace_iterator());
  }

  void add_column_family(const std::string& cf_name, void *handle) {
    cf_handles.insert(std::make_pair(cf_name, handle));
  }

  bool is_column_family(const std::string& prefix) {
    return cf_handles.count(prefix);
  }

  virtual uint64_t get_estimated_size(std::map<std::string,uint64_t> &extra) = 0;
  virtual int get_statfs(struct store_statfs_t *buf) {
    return -EOPNOTSUPP;
  }

  virtual int set_cache_size(uint64_t) {
    return -EOPNOTSUPP;
  }

  // PriCache

  virtual int64_t request_cache_bytes(PriorityCache::Priority pri, uint64_t chunk_bytes) const {
    return -EOPNOTSUPP;
  }

  virtual int64_t get_cache_bytes(PriorityCache::Priority pri) const {
    return cache_bytes[pri];
  }

  virtual int64_t get_cache_bytes() const {
    int64_t total = 0;

    for (int i = 0; i < PriorityCache::Priority::LAST + 1; i++) {
      PriorityCache::Priority pri = static_cast<PriorityCache::Priority>(i);
      total += get_cache_bytes(pri);
    }
    return total;
  }

  virtual void set_cache_bytes(PriorityCache::Priority pri, int64_t bytes) {
    cache_bytes[pri] = bytes;
  }

  virtual void add_cache_bytes(PriorityCache::Priority pri, int64_t bytes) {
    cache_bytes[pri] += bytes;
  }

  virtual int64_t commit_cache_size() {
    return -EOPNOTSUPP;
  }

  virtual double get_cache_ratio() const {
    return cache_ratio;
  }

  virtual void set_cache_ratio(double ratio) {
    cache_ratio = ratio;
  }

  virtual string get_cache_name() const {
    return "Unknown KeyValueDB Cache";
  } 

  // End PriCache

  virtual int set_cache_high_pri_pool_ratio(double ratio) {
    return -EOPNOTSUPP;
  }

  virtual int64_t get_cache_usage() const {
    return -EOPNOTSUPP;
  }

  virtual ~KeyValueDB() {}

  /// compact the underlying store
  virtual void compact() {}

  /// compact db for all keys with a given prefix
  virtual void compact_prefix(const std::string& prefix) {}
  /// compact db for all keys with a given prefix, async
  virtual void compact_prefix_async(const std::string& prefix) {}
  virtual void compact_range(const std::string& prefix,
			     const std::string& start, const std::string& end) {}
  virtual void compact_range_async(const std::string& prefix,
				   const std::string& start, const std::string& end) {}

  // See RocksDB merge operator definition, we support the basic
  // associative merge only right now.
  class MergeOperator {
    public:
    /// Merge into a key that doesn't exist
    virtual void merge_nonexistent(
      const char *rdata, size_t rlen,
      std::string *new_value) = 0;
    /// Merge into a key that does exist
    virtual void merge(
      const char *ldata, size_t llen,
      const char *rdata, size_t rlen,
      std::string *new_value) = 0;
    /// We use each operator name and each prefix to construct the overall RocksDB operator name for consistency check at open time.
    virtual string name() const = 0;

    virtual ~MergeOperator() {}
  };

  /// Setup one or more operators, this needs to be done BEFORE the DB is opened.
  virtual int set_merge_operator(const std::string& prefix,
				 std::shared_ptr<MergeOperator> mop) {
    return -EOPNOTSUPP;
  }

  virtual void get_statistics(Formatter *f) {
    return;
  }

  /**
   * Return your perf counters if you have any.  Subclasses are not
   * required to implement this, and callers must respect a null return
   * value.
   */
  virtual PerfCounters *get_perf_counters() {
    return nullptr;
  }
protected:
  /// List of matching prefixes/ColumnFamilies and merge operators
  std::vector<std::pair<std::string,
			std::shared_ptr<MergeOperator> > > merge_ops;

  /// column families in use, name->handle
  std::unordered_map<std::string, void *> cf_handles;
};

#endif
