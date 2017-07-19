#ifndef FORESTDB_STORE_H
#define FORESTDB_STORE_H

#include "include/types.h"
#include "include/buffer.h"
#include "KeyValueDB.h"
#include <set>
#include <map>
#include <queue>
#include <string>

#include <errno.h>
#include "common/errno.h"
#include "common/dout.h"
#include "include/assert.h"
#include "common/Formatter.h"

#include "common/ceph_context.h"

/**
 * Uses ForestDB to implement the KeyValueDB interface
 */

class ForestDBStore : public KeyValueDB {
  CephContext *cct;
  fdb_file_handle *fhandle;
  fdb_kv_handle *kvhandle;
  fdb_config config;
  fdb_kvs_config kvs_config;

  int do_open(ostream &out, bool create_if_missing);

public:

  static int _test_init(const string& dir);
  int init(string option_str="");

struct options_t {

  options_t() {}
} options;

ForestDBStore(CephContext *c, const string &path);

~ForestDBStore();

static bool check_omap_dir(string &omap_dir);
  /// Opens underlying db
  int open(ostream &out) {
    return do_open(out, false);
  }
  /// Creates underlying db if missing and opens it
  int create_and_open(ostream &out) override;

  void close();

  class ForestDBTransactionImpl : public KeyValueDB::TransactionImpl {
  public:
    list<bufferlist> buffers;
    list<string> keys;
    ForestDBStore *db;
    ForestDBTransactionImpl(ForestDBStore *_db);
    ~ForestDBTransactionImpl();
    void set(
      const string &prefix,
      const string &key,
      const bufferlist &bl);
    void rmkey(
      const string &prefix,
      const string &key);
    void rmkeys_by_prefix(
      const string &prefix
      );
    void rm_range_keys(
      const string &prefix,
      const string &start,
      const string &end
      );
    void merge(
      const string& prefix,
      const string& key,
      const bufferlist &bl) override;
  };

  KeyValueDB::Transaction get_transaction() override {
    return std::make_shared<ForestDBTransactionImpl>(this);
  }

  int submit_transaction(KeyValueDB::Transaction t);
  int submit_transaction_sync(KeyValueDB::Transaction t);
  int get(
    const string &prefix,
    const std::set<string> &key,
    std::map<string, bufferlist> *out
    );

  class ForestDBWholeSpaceIteratorImpl :
    public KeyValueDB::WholeSpaceIteratorImpl {
  protected:
    ForestDBStore *store;
    bool invalid;
  public:
    ForestDBWholeSpaceIteratorImpl(ForestDBStore *_store);
   
    ~ForestDBWholeSpaceIteratorImpl();

    int seek_to_first();
    int seek_to_first(const string &prefix);
    int seek_to_last();
    int seek_to_last(const string &prefix);
    int upper_bound(const string &prefix, const string &after);
    int lower_bound(const string &prefix, const string &to);
    bool valid();
    int next();
    int prev();
    string key();
    pair<string,string> raw_key();
    bool raw_key_is_prefixed(const string &prefix) override;
    bufferlist value();
    int status();
  };

  /// Utility
  static string combine_strings(const string &prefix, const string &value);
  static int split_key(string &in, string *prefix, string *key);
  static bufferlist to_bufferlist(string &in);
  static string past_prefix(const string &prefix);
  int set_merge_operator(const std::string& prefix,
		                 std::shared_ptr<KeyValueDB::MergeOperator> mop) override;
  virtual uint64_t get_estimated_size(map<string,uint64_t> &extra) {
    DIR *store_dir = opendir(path.c_str());
    if (!store_dir) {
      lderr(cct) << __func__ << " something happened opening the store: "
                 << cpp_strerror(errno) << dendl;
      return 0;
    }

    uint64_t total_size = 0;
    uint64_t data_size = 0;
    uint64_t misc_size = 0;

    struct dirent *entry = NULL;
    while ((entry = readdir(store_dir)) != NULL) {
      string n(entry->d_name);

      if (n == "." || n == "..")
        continue;

      string fpath = path + '/' + n;
      struct stat s;
      int err = stat(fpath.c_str(), &s);
      if (err < 0)
	err = -errno;
      // we may race against lmdb while reading files; this should only
      // happen when those files are being updated, data is being shuffled
      // and files get removed, in which case there's not much of a problem
      // as we'll get to them next time around.
      if (err == -ENOENT) {
	continue;
      }
      if (err < 0) {
        lderr(cct) << __func__ << " error obtaining stats for " << fpath
                   << ": " << cpp_strerror(err) << dendl;
        goto err;
      }

      size_t pos = n.find_last_of('.');
      if (pos == string::npos) {
        misc_size += s.st_size;
        continue;
      }

      string ext = n.substr(0, pos);
      if (ext == "data") {
        data_size += s.st_size;
      } else {
        misc_size += s.st_size;
      }
    }

    total_size = data_size + misc_size;

    extra["data"] = data_size;
    extra["misc"] = misc_size;
    extra["total"] = total_size;

err:
    closedir(store_dir);
    return total_size;
  }


protected:
  WholeSpaceIterator _get_iterator();

  WholeSpaceIterator _get_snapshot_iterator() {
    return _get_iterator();
  }

};

#endif
