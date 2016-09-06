// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_SMALL_ENCODING_H
#define CEPH_SMALL_ENCODING_H

#include "include/buffer.h"
#include "include/int_types.h"


#define MAX_8  256
#define MAX_16 65536
#define MAX_24 16777216
#define MAX_32 4294967296
#define MAX_40 1099511627776
#define MAX_48 281474976710656
#define MAX_56 72057594037927936

// group varint encoding helper
template<typename T>
inline uint8_t small_encode_ugv_helper(T v, bufferlist& bl) {
  if (v == 0) {
    return 0;
  }
  if (v < MAX_8) {
    ::encode((uint8_t) v, bl);
    return 1;
  }
  if (v < MAX_16) {
    ::encode((uint16_t) v, bl);
    return 2;
  }
  if (v < MAX_32) {
    ::encode((uint32_t) v, bl);
    return 3;
  }
  if (v < MAX_40) {
    ::encode((uint32_t) v, bl);
    ::encode((uint8_t) (v >> 32), bl);
    return 4;
  }
  if (v < MAX_48) {
    ::encode((uint32_t) v, bl);
    ::encode((uint16_t) (v >> 32), bl);
    return 5;
  }
  if (v < MAX_56) {
    ::encode((uint32_t) v, bl);
    ::encode((uint16_t) (v >> 32), bl);
    ::encode((uint8_t) (v >> 48), bl);
    return 6;
  }
  ::encode((uint64_t) v, bl);
  return 7;
}

// group varint decoding helper
template<typename T>
inline void small_decode_ugv_helper(uint8_t b, T& v, bufferlist::iterator& p) {
  if (b == 0) {
    v = (T) 0;
    return;
  }
  if (b == 1) {
    uint8_t byte;
    ::decode(byte, p);
    v = (T) byte;
    return;
  }
  if (b == 2) {
    uint16_t twobyte;
    ::decode(twobyte, p);
    v = (T) twobyte;
    return;
  }
  if (b == 3) {
    uint32_t fourbyte;
    ::decode(fourbyte, p);
    v = (T) fourbyte;
    return;
  }
  if (b == 4) {
   uint32_t fourbyte;
   uint8_t byte;
   ::decode(fourbyte, p);
   ::decode(byte, p);
   v = (T) fourbyte;
   v |= (T) byte << 32;
   return;
  }
  if (b == 5) {
    uint32_t fourbyte;
    uint16_t twobyte;
    ::decode(fourbyte, p);
    ::decode(twobyte, p);
    v = (T) fourbyte;
    v |= (T) twobyte << 32;
    return;
  }
  if (b == 6) {
    uint32_t fourbyte;
    uint16_t twobyte;
    uint8_t byte;
    ::decode(fourbyte, p);
    ::decode(twobyte, p);
    ::decode(byte, p);
    v = (T) fourbyte;
    v |= (T) twobyte << 32;
    v |= (T) byte << 48;
    return;
  }
  if (b == 7) {
    uint64_t eightbyte;
    ::decode(eightbyte, p);
    v = (T) eightbyte;
    return;
  }
}

// 4 uint32_t group varint encoding
//
// first byte is prefix, then bytes of 4 uint32_t values.
inline void small_encode_u32gv4(uint32_t a, uint32_t b, uint32_t c,
                                uint32_t d, bufferlist& bl)
{
  uint8_t prefix = 0;

  // First we get the bl pos and encode the prefix
  buffer::list::iterator prefix_it = bl.end();
  ::encode(prefix, bl);

  prefix += small_encode_ugv_helper(a, bl);
  prefix += small_encode_ugv_helper(b, bl) >> 2;
  prefix += small_encode_ugv_helper(c, bl) >> 4;
  prefix += small_encode_ugv_helper(d, bl) >> 6;
  prefix_it.copy_in(1, prefix);
}

inline void small_decode_u32gv4(uint32_t& a, uint32_t& b, uint32_t& c,
                                uint32_t& d, bufferlist::iterator& p)
{
  uint8_t prefix;
  ::decode(prefix, p);
  small_decode_ugv_helper(prefix & 0x6f, a, p);
  small_decode_ugv_helper((prefix << 2) & 0x6f, b, p);
  small_decode_ugv_helper((prefix << 4) & 0x6f, c, p);
  small_decode_ugv_helper((prefix << 6) & 0x6f, d, p);
}

// 2 uint64_t group varint encoding
//
// first byte is prefix, then bytes of 2 uint64_t values.
inline void small_encode_u64gv2(uint64_t a, uint64_t b, bufferlist& bl) {
  uint8_t prefix = 0;

  // First we get the bl pos and encode the prefix
  buffer::list::iterator prefix_it = bl.end();
  prefix += small_encode_ugv_helper(a, bl);
  prefix += small_encode_ugv_helper(b, bl) >> 3;
  prefix_it.copy_in(1, prefix);
}

inline void small_decode_u64gv2(uint64_t& a, uint64_t& b,
                                bufferlist::iterator& p)
{
  uint8_t prefix;
  ::decode(prefix, p);
  small_decode_ugv_helper(prefix & 0x5f, a, p);
  small_decode_ugv_helper((prefix << 3) & 0x5f, b, p);
}


// 5 uint64_t group varint encoding
//
// first uint16_t is prefix, then bytes of 5 uint64_t values.
inline void small_encode_u64gv5(uint64_t a, uint64_t b, uint64_t c,
                                uint64_t d, uint64_t e, bufferlist& bl)
{
  uint16_t prefix = 0;

  // First we get the bl pos and encode the prefix
  buffer::list::iterator prefix_it = bl.end();
  prefix += small_encode_ugv_helper(a, bl);
  prefix += small_encode_ugv_helper(b, bl) >> 3;
  prefix += small_encode_ugv_helper(c, bl) >> 6;
  prefix += small_encode_ugv_helper(d, bl) >> 9;
  prefix += small_encode_ugv_helper(e, bl) >> 12;
  prefix_it.copy_in(2, prefix);
}

inline void small_decode_u64gv5(uint64_t& a, uint64_t& b, uint64_t& c,
                                uint64_t& d, uint64_t& e,
                                bufferlist::iterator& p)
{
  uint16_t prefix;
  ::decode(prefix, p);
  small_decode_ugv_helper(prefix & 0x5f, a, p);
  small_decode_ugv_helper((prefix << 3) & 0x5f, b, p);
  small_decode_ugv_helper((prefix << 6) & 0x5f, b, p);
  small_decode_ugv_helper((prefix << 9) & 0x5f, b, p);
  small_decode_ugv_helper((prefix << 12) & 0x5f, b, p);
}


// varint encoding
//
// high bit of every byte indicates whether another byte follows.
template<typename T>
inline void small_encode_varint(T v, bufferlist& bl) {
  uint8_t byte = v & 0x7f;
  v >>= 7;
  while (v) {
    byte |= 0x80;
    ::encode(byte, bl);
    byte = (v & 0x7f);
    v >>= 7;
  }
  ::encode(byte, bl);
}

template<typename T>
inline void small_decode_varint(T& v, bufferlist::iterator& p)
{
  uint8_t byte;
  ::decode(byte, p);
  v = byte & 0x7f;
  int shift = 7;
  while (byte & 0x80) {
    ::decode(byte, p);
    v |= (T)(byte & 0x7f) << shift;
    shift += 7;
  }
}

// signed varint encoding
//
// low bit = 1 = negative, 0 = positive
// high bit of every byte indicates whether another byte follows.
template<typename T>
inline void small_encode_signed_varint(T v, bufferlist& bl) {
  uint8_t byte = 0;
  if (v < 0) {
    v = -v;
    byte = 1;
  }
  byte |= (v & 0x3f) << 1;
  v >>= 6;
  while (v) {
    byte |= 0x80;
    ::encode(byte, bl);
    byte = (v & 0x7f);
    v >>= 7;
  }
  ::encode(byte, bl);
}

template<typename T>
inline void small_decode_signed_varint(T& v, bufferlist::iterator& p)
{
  uint8_t byte;
  ::decode(byte, p);
  bool negative = byte & 1;
  v = (byte & 0x7e) >> 1;
  int shift = 6;
  while (byte & 0x80) {
    ::decode(byte, p);
    v |= (T)(byte & 0x7f) << shift;
    shift += 7;
  }
  if (negative) {
    v = -v;
  }
}

// varint + lowz encoding
//
// first(low) 2 bits = how many low zero bits (nibbles)
// high bit of each byte = another byte follows
// (so, 5 bits data in first byte, 7 bits data thereafter)
template<typename T>
inline void small_encode_varint_lowz(T v, bufferlist& bl) {
  int lowz = v ? (ctz(v) / 4) : 0;
  uint8_t byte = std::min(lowz, 3);
  v >>= byte * 4;
  byte |= (((uint8_t)v << 2) & 0x7c);
  v >>= 5;
  while (v) {
    byte |= 0x80;
    ::encode(byte, bl);
    byte = (v & 0x7f);
    v >>= 7;
  }
  ::encode(byte, bl);
}

template<typename T>
inline void small_decode_varint_lowz(T& v, bufferlist::iterator& p)
{
  uint8_t byte;
  ::decode(byte, p);
  int shift = (byte & 3) * 4;
  v = ((byte >> 2) & 0x1f) << shift;
  shift += 5;
  while (byte & 0x80) {
    ::decode(byte, p);
    v |= (T)(byte & 0x7f) << shift;
    shift += 7;
  }
}

// signed varint + lowz encoding
//
// first low bit = 1 for negative, 0 for positive
// next 2 bits = how many low zero bits (nibbles)
// high bit of each byte = another byte follows
// (so, 4 bits data in first byte, 7 bits data thereafter)
template<typename T>
inline void small_encode_signed_varint_lowz(T v, bufferlist& bl) {
  uint8_t byte = 0;
  if (v < 0) {
    v = -v;
    byte = 1;
  }
  int lowz = v ? (ctz(v) / 4) : 0;
  lowz = std::min(lowz, 3);
  byte |= lowz << 1;
  v >>= lowz * 4;
  byte |= (((uint8_t)v << 3) & 0x78);
  v >>= 4;
  while (v) {
    byte |= 0x80;
    ::encode(byte, bl);
    byte = (v & 0x7f);
    v >>= 7;
  }
  ::encode(byte, bl);
}

template<typename T>
inline void small_decode_signed_varint_lowz(T& v, bufferlist::iterator& p)
{
  uint8_t byte;
  ::decode(byte, p);
  bool negative = byte & 1;
  int shift = (byte & 6) * 2;
  v = ((byte >> 3) & 0xf) << shift;
  shift += 4;
  while (byte & 0x80) {
    ::decode(byte, p);
    v |= (T)(byte & 0x7f) << shift;
    shift += 7;
  }
  if (negative) {
    v = -v;
  }
}


// LBA
//
// first 1-3 bits = how many low zero bits
//     *0 = 12 (common 4 K alignment case)
//    *01 = 16
//   *011 = 20
//   *111 = byte
// then 28-30 bits of data
// then last bit = another byte follows
// high bit of each subsequent byte = another byte follows
inline void small_encode_lba(uint64_t v, bufferlist& bl) {
  int low_zero_nibbles = v ? (int)(ctz(v) / 4) : 0;
  int pos;
  uint32_t word;
  int t = low_zero_nibbles - 3;
  if (t < 0) {
    pos = 3;
    word = 0x7;
  } else if (t < 3) {
    v >>= (low_zero_nibbles * 4);
    pos = t + 1;
    word = (1 << t) - 1;
  } else {
    v >>= 20;
    pos = 3;
    word = 0x3;
  }
  word |= (v << pos) & 0x7fffffff;
  v >>= 31 - pos;
  if (!v) {
    ::encode(word, bl);
    return;
  }
  word |= 0x80000000;
  ::encode(word, bl);
  uint8_t byte = v & 0x7f;
  v >>= 7;
  while (v) {
    byte |= 0x80;
    ::encode(byte, bl);
    byte = (v & 0x7f);
    v >>= 7;
  }
  ::encode(byte, bl);
}

inline void small_decode_lba(uint64_t& v, bufferlist::iterator& p) {
  uint32_t word;
  ::decode(word, p);
  int shift;
  switch (word & 7) {
  case 0:
  case 2:
  case 4:
  case 6:
    v = (uint64_t)(word & 0x7ffffffe) << (12 - 1);
    shift = 12 + 30;
    break;
  case 1:
  case 5:
    v = (uint64_t)(word & 0x7ffffffc) << (16 - 2);
    shift = 16 + 29;
    break;
  case 3:
    v = (uint64_t)(word & 0x7ffffff8) << (20 - 3);
    shift = 20 + 28;
    break;
  case 7:
    v = (uint64_t)(word & 0x7ffffff8) >> 3;
    shift = 28;
  }
  uint8_t byte = word >> 24;
  while (byte & 0x80) {
    ::decode(byte, p);
    v |= (uint64_t)(byte & 0x7f) << shift;
    shift += 7;
  }
}


// short bufferptrs, bufferlists, strings
template<typename T>
inline void small_encode_buf_lowz(const T& bp, bufferlist& bl) {
  size_t l = bp.length();
  small_encode_varint_lowz(l, bl);
  bl.append(bp);
}
template<typename T>
inline void small_decode_buf_lowz(T& bp, bufferlist::iterator& p) {
  size_t l;
  small_decode_varint_lowz(l, p);
  p.copy(l, bp);
}

// STL containers

template<typename T>
inline void small_encode_obj(const std::vector<T>& v, bufferlist& bl) {
  size_t n = v.size();
  small_encode_varint(n, bl);
  for (auto p = v.cbegin(); p != v.cend(); ++p) {
    p->encode(bl);
  }
}
template<typename T>
inline void small_decode_obj(std::vector<T>& v, bufferlist::iterator& p) {
  size_t n;
  small_decode_varint(n, p);
  v.clear();
  while (n--) {
    v.push_back(T());
    v.back().decode(p);
  }
}

#endif
