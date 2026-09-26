add_executable(ceph_test_objectcacher_stress
  object_cacher_stress.cc
  FakeWriteback.cc
  MemWriteback.cc
  )
target_link_libraries(ceph_test_objectcacher_stress
  osdc
  global
  ${EXTRALIBS}
  ${CMAKE_DL_LIBS}
  )
install(TARGETS ceph_test_objectcacher_stress
  DESTINATION ${CMAKE_INSTALL_BINDIR})

add_executable(ceph_test_objectcacher_misc
  object_cacher_misc.cc
  MemWriteback.cc
  )
target_link_libraries(ceph_test_objectcacher_misc
  osdc
  global
  ${EXTRALIBS}
  ${CMAKE_DL_LIBS}
  )
install(TARGETS ceph_test_objectcacher_misc
  DESTINATION ${CMAKE_INSTALL_BINDIR})

# unittest_reply_shape_probe: hermetic gtest for the benchmark-only
# Objecter reply output-shape census (ms_benchmark_direct_rx_probe).
add_executable(unittest_reply_shape_probe
  unittest_reply_shape_probe.cc
  $<TARGET_OBJECTS:unit-main>
  )
add_ceph_unittest(unittest_reply_shape_probe)
target_link_libraries(unittest_reply_shape_probe osdc global ${UNITTEST_LIBS})
