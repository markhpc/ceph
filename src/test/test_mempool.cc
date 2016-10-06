// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2016 Western Digital Corporation
 *
 * Author: Allen Samuels <allen.samuels@sandisk.com>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

#include <stdio.h>

#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include "global/global_context.h"
#include "gtest/gtest.h"
#include "include/mempool.h"

void CheckUsage1() {
   map<const char *,mempool::StatsByTypeID_t> m;
   mempool::pool_t::StatsByTypeID("unittest_1",m,10000);
   size_t usage = unittest_1::allocated_bytes();
   size_t sum = 0;
   for (auto& p : m) {
      sum += p.second.bytes;
   }
   if (sum != usage) {
      ceph::TableFormatter jf;
      mempool::DumpStatsByTypeID("unittest1",&jf,100);
      jf.flush(std::cout);
   }
   EXPECT_EQ(sum,usage);
}
void CheckUsage2() {
   map<const char *,mempool::StatsByTypeID_t> m;
   mempool::pool_t::StatsByTypeID("unittest_2",m,10000);
   size_t usage = unittest_2::allocated_bytes();
   size_t sum = 0;
   for (auto& p : m) {
      sum += p.second.bytes;
   }
   if (sum != usage) {
      ceph::TableFormatter jf;
      mempool::DumpStatsByTypeID("unittest_2",&jf,100);
      jf.flush(std::cout);
   }
   EXPECT_EQ(sum,usage);
}

TEST(test_mempool, mempool_context) {
   CheckUsage1();
   unittest_1::map<int,int,3,3> m1;
   m1[1] = 2;
   EXPECT_EQ(m1.size(),size_t(1));

   multimap<size_t,mempool::StatsBySlots_t> Slots;
   mempool::pool_t::StatsBySlots("unittest_1",Slots,100);
   EXPECT_EQ(Slots.size(),1u);
   EXPECT_NE(Slots.find(1),Slots.end());
   EXPECT_EQ(Slots.find(1)->second.slabs,0u);
   EXPECT_EQ(Slots.find(1)->second.bytes,0u);
   CheckUsage1();

   m1[2] = 2;
   m1[3] = 3;
   m1[4] = 4;

   CheckUsage1();
   Slots.clear();
   mempool::pool_t::StatsBySlots("unittest_1",Slots,100);
   EXPECT_EQ(Slots.size(),1u);
   EXPECT_NE(Slots.find(4),Slots.end());
   EXPECT_EQ(Slots.find(4)->second.slabs,1u);
   EXPECT_NE(Slots.find(4)->second.bytes,0u);

   EXPECT_EQ(mempool::GetPool(mempool::unittest_1).allocated_bytes(),
             Slots.find(4)->second.bytes);
   CheckUsage1();
}

template<typename A, typename B> void eq_elements(const A& a, const B& b) {
   auto lhs = a.begin();
   auto rhs = b.begin();
   while (lhs != a.end()) {
      EXPECT_EQ(*lhs,*rhs);
      lhs++;
      rhs++;
   }
   EXPECT_EQ(rhs,b.end());
}

template<typename A, typename B> void eq_pairs(const A& a, const B& b) {
   auto lhs = a.begin();
   auto rhs = b.begin();
   while (lhs != a.end()) {
      EXPECT_EQ(lhs->first,rhs->first);
      EXPECT_EQ(lhs->second,rhs->second);
      lhs++;
      rhs++;
   }
   EXPECT_EQ(rhs,b.end());
}

#define MAKE_INSERTER(inserter) \
template<typename A,typename B> void do_##inserter(A& a, B& b, int count, int base) { \
   for (int i = 0; i < count; ++i) { \
      a.inserter(base + i); \
      b.inserter(base + i); \
   } \
}

MAKE_INSERTER(push_back);
MAKE_INSERTER(insert);

template<typename A,typename B> void do_insert_key(A& a, B& b, int count, int base) { \
   for (int i = 0; i < count; ++i) {
      a.insert(make_pair(base+i,base+i));
      b.insert(make_pair(base+i,base+i));
      CheckUsage1();
   }
}

TEST(test_slab_containers, vector_context) {
   multimap<size_t,mempool::StatsBySlots_t> Slotz;
   mempool::pool_t::StatsBySlots("unittest_1",Slotz,100);
   EXPECT_EQ(Slotz.size(),0u);
   CheckUsage1();
   for (int i = 0; i < 10; ++i) {
      vector<int> a;
      unittest_1::vector<int,4> b,c;
      eq_elements(a,b);
      do_push_back(a,b,i,i);
      eq_elements(a,b);
      CheckUsage1();
      multimap<size_t,mempool::StatsBySlabs_t> Slabs;
      mempool::pool_t::StatsBySlabs("unittest_1",Slabs,100);
      EXPECT_EQ(Slabs.size(),2u);
      if (i > 4) {
	EXPECT_NE(Slabs.find(1),Slabs.end());
	EXPECT_NE(Slabs.find(1)->second.bytes,0u);
      } else {
	EXPECT_NE(Slabs.find(0),Slabs.end());
        EXPECT_EQ(Slabs.find(0)->second.bytes,0u);
      }
      c.swap(b);
      eq_elements(a,c);
      CheckUsage1();
      a.clear();
      b.clear();
      c.clear();
   }
}

TEST(test_slab_containers, list_context) {
   for (int i = 1; i < 10; ++i) {
      list<int> a;
      unittest_1::list<int,4> b,c;
      eq_elements(a,b);
      do_push_back(a,b,i,i);
      eq_elements(a,b);
      c.swap(b);
      eq_elements(a,c);
      a.erase(a.begin());
      c.erase(c.begin());
      eq_elements(a,c);
      a.clear();
      b.clear();
      c.clear();
      do_push_back(a,b,i,i);
      c.splice(c.begin(),b,b.begin(),b.end());
      eq_elements(a,c);
      CheckUsage1();
   }
   //
   // Now with reserve calls
   //
   for (int i = 1; i < 10; ++i) {
      list<int> a;
      unittest_1::list<int,4> b,c;
      eq_elements(a,b);
      b.reserve(i);
      c.reserve(i);
      do_push_back(a,b,i,i);
      eq_elements(a,b);
      CheckUsage1();
      c.swap(b);
      eq_elements(a,c);
      a.erase(a.begin());
      c.erase(c.begin());
      CheckUsage1();
      eq_elements(a,c);
      a.clear();
      b.clear();
      c.clear();
      do_push_back(a,b,i,i);
      c.splice(c.begin(),b,b.begin(),b.end());
      eq_elements(a,c);
   }
}

TEST(test_slab_containers, set_context) {
   for (int i = 0; i < 10; ++i) {
      set<int> a;
      unittest_1::set<int,4> b;
      do_insert(a,b,i,i);
      eq_elements(a,b);
      CheckUsage1();
   }

   for (int i = 1; i < 10; ++i) {
      set<int> a;
      unittest_1::set<int,4> b;
      do_insert(a,b,i,0);
      EXPECT_NE(a.find(i/2),a.end());
      EXPECT_NE(b.find(i/2),b.end());
      a.erase(a.find(i/2));
      b.erase(b.find(i/2));
      eq_elements(a,b);
      CheckUsage1();
   }
   for (int i = 1; i < 10; ++i) {
      set<int> a;
      unittest_1::set<int,4> b;
      b.reserve(i);
      do_insert(a,b,i,0);
      EXPECT_NE(a.find(i/2),a.end());
      EXPECT_NE(b.find(i/2),b.end());
      a.erase(a.find(i/2));
      b.erase(b.find(i/2));
      eq_elements(a,b);
      CheckUsage1();
   }
}

struct obj {
  MEMBER_OF_MEMPOOL();
  int a;
  int b;
  obj() : a(1), b(1) {}
  obj(int _a) : a(_a), b(2) {}
  obj(int _a,int _b) : a(_a), b(_b) {}
};

DEFINE_OBJECT_IN_MEMPOOL(obj,obj,unittest_2,1);

TEST(test_slab_containers, test_factory) {
   obj *o1 = new obj();
   obj *o2 = new obj(10);
   obj *o3 = new obj(20,30);
   CheckUsage2();
   EXPECT_NE(o1,nullptr);
   EXPECT_EQ(o1->a,1);
   EXPECT_EQ(o1->b,1);
   EXPECT_EQ(o2->a,10);
   EXPECT_EQ(o2->b,2);
   EXPECT_EQ(o3->a,20);
   EXPECT_EQ(o3->b,30);

   delete o1;
   delete o2;
   delete o3;
   CheckUsage2();
}

int main(int argc, char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}


/*
 * Local Variables:
 * compile-command: "cd ../../build ; make -j4 &&
 *   make unittest_mempool &&
 *   valgrind --tool=memcheck ./unittest_mempool --gtest_filter=*.*"
 * End:
 */
