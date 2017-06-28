lib_LTLIBRARIES = libreflex.la

AM_CPPFLAGS = -I@jdk@/include -I@jdk@/include/linux -I/kozyrakis/users/anakli/libevent/include
libreflex_la_LIBADD = -L/usr/local/lib -levent -levent_core -levent_pthreads -levent_extra -levent_openssl
include_HEADERS = src/stanford_mast_reflex_NativeDispatcher.h
libreflex_la_SOURCES = src/stanford_mast_reflex_NativeDispatcher.cpp

