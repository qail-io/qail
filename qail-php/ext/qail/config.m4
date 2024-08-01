dnl config.m4 for QAIL PHP extension

PHP_ARG_ENABLE(qail, whether to enable QAIL support,
[  --enable-qail           Enable QAIL support])

if test "$PHP_QAIL" != "no"; then
  dnl Path to Rust static library
  QAIL_LIB_DIR="/Users/orion/qail.rs/target/release"
  
  dnl Check for libqail_php.a
  if test ! -f "$QAIL_LIB_DIR/libqail_php.a"; then
    AC_MSG_ERROR([libqail_php.a not found in $QAIL_LIB_DIR. Run: cargo build --package qail-php --release])
  fi
  
  dnl Add include path for Rust headers if needed
  PHP_ADD_INCLUDE($QAIL_LIB_DIR)
  
  dnl Link the Rust static library
  PHP_ADD_LIBRARY_WITH_PATH(qail_php, $QAIL_LIB_DIR, QAIL_SHARED_LIBADD)
  
  dnl Also link system libraries required by Rust
  PHP_ADD_LIBRARY(resolv, , QAIL_SHARED_LIBADD)
  PHP_ADD_LIBRARY(c++, , QAIL_SHARED_LIBADD)
  
  PHP_SUBST(QAIL_SHARED_LIBADD)
  
  PHP_NEW_EXTENSION(qail, qail.c, $ext_shared)
fi
