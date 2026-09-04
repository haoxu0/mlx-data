# coming soon

# A self-contained runtime check for ArrowStream: it builds its own
# ArrowArrayStream, so it needs neither Arrow nor a test framework. Off by
# default, like the python bindings.
option(MLX_BUILD_TESTS "Build mlx data tests" OFF)
if(MLX_BUILD_TESTS)
  enable_testing()
  add_executable(test_arrow_stream
                 ${CMAKE_CURRENT_LIST_DIR}/test_arrow_stream.cpp)
  target_link_libraries(test_arrow_stream PRIVATE mlxdata)
  target_include_directories(test_arrow_stream
                             PRIVATE ${CMAKE_CURRENT_LIST_DIR}/..)
  add_test(NAME arrow_stream COMMAND test_arrow_stream)
endif()
