lib_LTLIBRARIES = libreflex.la

AM_CPPFLAGS = -I@jdk@/include -I@jdk@/include/linux -I@ix@/deps/dpdk/build/include -I@ix@/inc -I@ix@/libix
libreflex_la_LIBADD =  -L/usr/local/lib -levent -levent_core -levent_pthreads -levent_extra -levent_openssl
libreflex_la_LDFLAGS = -Wl,--whole-archive @ix@/libix/libix.a -Wl,--no-whole-archive
include_HEADERS = src/stanford_mast_reflex_NativeDispatcher.h
libreflex_la_SOURCES = src/stanford_mast_reflex_NativeDispatcher.cpp

