module Main where

import Librados

main = do
  print "About to Initialize"
  radosInitialize []
  print "About to Open casdata"
  pool <- radosOpenPool "casdata"
  print "About to list objects:"
  radosListObjects pool >>= print
  print "About to write a string to object1"
  radosWriteString pool "object1" "Hello, I'm object 1"
  print "About to read a string from object1"
  radosReadString pool "object1" 1000 >>= print
  print "About to remove object1"
  radosRemove pool "object1"
  print "Closing pool"
  radosClosePool pool
  print "Deinitializing"
  radosDeinitialize
  print "Goodbye"
