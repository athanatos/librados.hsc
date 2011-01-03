{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE ExistentialQuantification #-}


module Librados ( radosInitialize
                , radosDeinitialize
                , radosOpenPool
                , radosClosePool
                , radosStatPool
                , radosSetSnap
                , radosSetSnapContext
                , radosSnapCreate
                , radosSnapRemove
                , radosSnapRollbackObject
                , radosSelfmanagedSnapCreate
                , radosSelfmanagedSnapRemove
                , radosSnapList
                , radosSnapLookup
                , radosSnapGetName
                , radosCreatePool
                , radosCreatePoolWithAuid
                , radosCreatePoolWithCrushRule
                , radosCreatePoolWithAll
                , radosChangePoolAuid
                , radosListObjectsOpen
                , radosListObjectsNext
                , radosListObjectsClose
                , radosListObjects
                , radosWrite
                , radosWriteString
                , radosWriteFull
                , radosWriteFullString
                , radosRead
                , radosReadString
                , radosRemove
                , radosTrunc
                ) where

import Foreign
import Foreign.C
import Foreign.Ptr (FunPtr, freeHaskellFunPtr)
import Data.Int (Int64)
import Data.Typeable
import Control.Exception
import Data.Word
import Data.Array.Storable

#include "/home/sam/projects/ceph/src/include/librados.h"
#let alignment t = "%lu", (unsigned long)offsetof(struct {char x__; t (y__); }, y__)

-- Types
data RadosPoolStat = RadosPoolStat { numBytes :: Int64
                                   , numKB :: Int64
                                   , numObjects :: Int64
                                   , numObjectClones :: Int64
                                   , numObjectCopies :: Int64
                                   , numObjectsMissingOnPrimary :: Int64
                                   , numObjectsDegraded :: Int64
                                   , numRD :: Int64
                                   , numRDKB :: Int64
                                   , numWR :: Int64
                                   , numWRKB :: Int64
                                   } deriving (Show)

instance Storable RadosPoolStat where
  alignment _ = #{alignment struct rados_pool_stat_t}
  sizeOf _ = #{size struct rados_pool_stat_t}
  peek ptr = do
    num_bytes <- #{peek struct rados_pool_stat_t, num_bytes} ptr
    num_kb <- #{peek struct rados_pool_stat_t, num_kb} ptr
    num_objects <- #{peek struct rados_pool_stat_t, num_objects} ptr
    num_object_clones <- #{peek struct rados_pool_stat_t, num_object_clones} ptr
    num_object_copies <- #{peek struct rados_pool_stat_t, num_object_copies} ptr
    num_objects_missing_on_primary <- #{peek struct rados_pool_stat_t, num_objects_missing_on_primary} ptr
    num_objects_degraded <- #{peek struct rados_pool_stat_t, num_objects_degraded} ptr
    num_rd <- #{peek struct rados_pool_stat_t, num_rd} ptr
    num_rd_kb <- #{peek struct rados_pool_stat_t, num_rd_kb} ptr
    num_wr <- #{peek struct rados_pool_stat_t, num_wr} ptr
    num_wr_kb <- #{peek struct rados_pool_stat_t, num_wr_kb} ptr
    return RadosPoolStat { numBytes = num_bytes
                         , numKB = num_kb
                         , numObjects = num_objects
                         , numObjectClones = num_object_clones
                         , numObjectCopies = num_object_copies
                         , numObjectsMissingOnPrimary = num_objects_missing_on_primary
                         , numObjectsDegraded = num_objects_degraded
                         , numRD = num_rd
                         , numRDKB = num_rd_kb
                         , numWR = num_wr
                         , numWRKB = num_wr_kb
                         }
  poke ptr stat = do
    #{poke struct rados_pool_stat_t, num_bytes} ptr (numBytes stat)
    #{poke struct rados_pool_stat_t, num_kb} ptr (numKB stat)
    #{poke struct rados_pool_stat_t, num_objects} ptr (numObjects stat)
    #{poke struct rados_pool_stat_t, num_object_clones} ptr (numObjectClones stat)
    #{poke struct rados_pool_stat_t, num_object_copies} ptr (numObjectCopies stat)
    #{poke struct rados_pool_stat_t, num_objects_missing_on_primary} ptr (numObjectsMissingOnPrimary stat)
    #{poke struct rados_pool_stat_t, num_objects_degraded} ptr (numObjectsDegraded stat)
    #{poke struct rados_pool_stat_t, num_rd} ptr (numRD stat)
    #{poke struct rados_pool_stat_t, num_rd_kb} ptr (numRDKB stat)
    #{poke struct rados_pool_stat_t, num_wr} ptr (numWR stat)
    #{poke struct rados_pool_stat_t, num_wr_kb} ptr (numWRKB stat)

type RadosPool = Ptr ()
type RadosListCtx = Ptr ()
type RadosSnap = CULong

arrayCopy :: Storable a => [a] -> IO (Ptr a)
arrayCopy inlist = do
  ptr <- mallocArray (length inlist)
  pokeArray ptr inlist
  return ptr

wrapErrorCode :: IO a -> CInt -> IO a
wrapErrorCode successAction code = do
  if (code < 0)
    then throw (MiscRadosException (fromIntegral code))
    else successAction

-- Exceptions
data SomeRadosException = forall e . Exception e => SomeRadosException e
                          deriving (Typeable)
instance Show SomeRadosException where
  show (SomeRadosException e) = show e
instance Exception SomeRadosException

radosExceptionToException :: Exception e => e -> SomeException
radosExceptionToException = toException . SomeRadosException

radosExceptionFromException :: Exception e => SomeException -> Maybe e
radosExceptionFromException x = do
  SomeRadosException a <- fromException x
  cast a

data MiscRadosException = MiscRadosException Int
                          deriving (Typeable, Show)

instance Exception MiscRadosException where
  toException = radosExceptionToException
  fromException = radosExceptionFromException

-- Initialize/Deinitialize
foreign import ccall "rados_initialize" c_radosInitialize :: CInt -> (Ptr (Ptr CChar)) -> IO CInt
radosInitialize :: [String] -> IO ()
radosInitialize argv = do
  storage <- sequence $ map newCString argv
  ptr <- (arrayCopy storage) :: IO (Ptr (Ptr CChar))
  result <- c_radosInitialize (fromIntegral (length storage)) ptr
  sequence_ (map free storage)
  free ptr
  if (result /= 0)
     then throwIO (MiscRadosException (fromIntegral result))
     else return ()

foreign import ccall "rados_deinitialize" radosDeinitialize :: IO ()

-- Pool manipulation
foreign import ccall "rados_open_pool" c_radosOpenPool :: Ptr CChar -> Ptr RadosPool -> IO CInt
radosOpenPool :: String -> IO RadosPool
radosOpenPool poolName = do
  stringPtr <- newCString poolName
  poolPtr <- (malloc :: IO (Ptr RadosPool))
  result <- c_radosOpenPool stringPtr poolPtr
  free stringPtr
  pool <- peek poolPtr -- ONLY DEREFERENCE IF RESULT == 0
  free poolPtr
  wrapErrorCode (return pool) result
       
foreign import ccall "rados_close_pool" c_radosClosePool :: RadosPool -> IO CInt
radosClosePool :: RadosPool -> IO ()
radosClosePool pool = c_radosClosePool pool >>= (wrapErrorCode $ return ())

foreign import ccall "rados_lookup_pool" c_radosLookupPool :: Ptr CChar -> IO CInt
radosLookupPool :: String -> IO Int
radosLookupPool poolName = do
  stringPtr <- newCString poolName
  result <- c_radosLookupPool stringPtr
  free stringPtr
  return (fromIntegral result)

foreign import ccall "rados_stat_pool" c_radosStatPool :: RadosPool -> Ptr RadosPoolStat -> IO CInt
radosStatPool :: RadosPool -> IO RadosPoolStat
radosStatPool pool = do
  statPtr <- (malloc :: (IO (Ptr RadosPoolStat)))
  result <- c_radosStatPool pool statPtr
  if (result /= 0)
    then throwIO (MiscRadosException (fromIntegral result))
    else return ()
  poolStat <- peek statPtr
  free statPtr
  return poolStat

foreign import ccall "rados_create_pool" c_radosCreatePool :: Ptr CChar -> IO CInt
radosCreatePool :: String -> IO ()
radosCreatePool poolName = do
  stringPtr <- newCString poolName
  result <- c_radosCreatePool stringPtr
  free stringPtr
  wrapErrorCode (return ()) result

foreign import ccall "rados_create_pool_with_auid" c_radosCreatePoolWithAuid :: Ptr CChar -> CULong -> IO CInt
radosCreatePoolWithAuid :: String -> Int64 -> IO ()
radosCreatePoolWithAuid poolName auid = do
  stringPtr <- newCString poolName
  result <- c_radosCreatePoolWithAuid stringPtr (fromIntegral auid)
  free stringPtr
  wrapErrorCode (return ()) result

foreign import ccall "rados_create_pool_with_crush_rule" c_radosCreatePoolWithCrushRule :: Ptr CChar -> CUChar -> IO CInt
radosCreatePoolWithCrushRule :: String -> Word8 -> IO ()
radosCreatePoolWithCrushRule poolName rule = do
  stringPtr <- newCString poolName
  result <- c_radosCreatePoolWithCrushRule stringPtr (fromIntegral rule)
  free stringPtr
  wrapErrorCode (return ()) result

foreign import ccall "rados_create_pool_with_all" c_radosCreatePoolWithAll :: Ptr CChar -> CULong -> CUChar -> IO CInt
radosCreatePoolWithAll:: String -> Word64 -> Word8 -> IO ()
radosCreatePoolWithAll poolName auid rule = do
  stringPtr <- newCString poolName
  result <- c_radosCreatePoolWithAll stringPtr (fromIntegral auid) (fromIntegral rule)
  free stringPtr
  wrapErrorCode (return ()) result

foreign import ccall "rados_delete_pool" c_radosDeletePool :: RadosPool -> IO CInt
radosDeletePool :: RadosPool -> IO ()
radosDeletePool pool = c_radosDeletePool pool >>= (wrapErrorCode $ return ())

foreign import ccall "rados_change_pool_auid" c_radosChangePoolAuid :: RadosPool -> CULong -> IO CInt
radosChangePoolAuid :: RadosPool -> Word64 -> IO ()
radosChangePoolAuid pool auid = c_radosChangePoolAuid pool (fromIntegral auid) >>= (wrapErrorCode $ return ())

-- Snaps
foreign import ccall "rados_set_snap" c_radosSetSnap :: RadosPool -> RadosSnap -> IO CInt
radosSetSnap :: RadosPool -> RadosSnap -> IO ()
radosSetSnap pool snap = c_radosSetSnap pool snap >>= (wrapErrorCode $ return ())

foreign import ccall "rados_set_snap_context" 
  c_radosSetSnapContext :: RadosPool -> RadosSnap -> Ptr RadosSnap -> CInt -> IO CInt
radosSetSnapContext :: RadosPool -> RadosSnap -> [RadosSnap] -> IO ()
radosSetSnapContext pool seq snaps = do
  ptr <- (arrayCopy snaps) :: IO (Ptr RadosSnap)
  result <- c_radosSetSnapContext pool seq ptr (fromIntegral (length snaps))
  free ptr
  wrapErrorCode (return ()) result

foreign import ccall "rados_snap_create" c_radosSnapCreate :: RadosPool -> Ptr CChar -> IO CInt
foreign import ccall "rados_snap_remove" c_radosSnapRemove :: RadosPool -> Ptr CChar -> IO CInt
_wrap :: (RadosPool -> Ptr CChar -> IO CInt) -> RadosPool -> String -> IO ()
_wrap fun pool snapName = do
  strPtr <- newCString snapName
  result <- fun pool strPtr
  free strPtr
  wrapErrorCode (return ()) result

radosSnapCreate = _wrap c_radosSnapCreate
radosSnapRemove = _wrap c_radosSnapRemove

foreign import ccall "rados_snap_rollback_object" 
  c_radosSnapRollbackObject :: RadosPool -> Ptr CChar -> Ptr CChar -> IO CInt
radosSnapRollbackObject :: RadosPool -> String -> String -> IO ()
radosSnapRollbackObject pool oid snapname = do
  strptroid <- newCString oid
  strptrsnap <- newCString snapname
  result <- c_radosSnapRollbackObject pool strptroid strptrsnap
  free strptroid
  free strptrsnap
  wrapErrorCode (return ()) result

foreign import ccall "rados_selfmanaged_snap_create" 
  c_radosSelfmanagedSnapCreate :: RadosPool -> Ptr CULong -> IO CInt
radosSelfmanagedSnapCreate :: RadosPool -> IO Int64
radosSelfmanagedSnapCreate pool = do
  ptr <- (malloc :: (IO (Ptr CULong)))
  result <- c_radosSelfmanagedSnapCreate pool ptr
  retval <- peek ptr
  wrapErrorCode (return (fromIntegral retval)) result

foreign import ccall "rados_selfmanaged_snap_remove"
  c_radosSelfmanagedSnapRemove :: RadosPool -> CULong -> IO CInt
radosSelfmanagedSnapRemove :: RadosPool -> Int64 -> IO ()
radosSelfmanagedSnapRemove pool snapid = 
  c_radosSelfmanagedSnapRemove pool (fromIntegral snapid) >>= (wrapErrorCode (return ()))

foreign import ccall "rados_snap_list" 
  c_radosSnapList :: RadosPool -> Ptr RadosSnap -> CInt -> IO CInt
radosSnapList :: RadosPool -> Int64 -> IO [RadosSnap]
radosSnapList pool max = do
  ptr <- ((mallocArray (fromIntegral max)) :: IO (Ptr RadosSnap))
  result <- c_radosSnapList pool ptr (fromIntegral max)
  if (result < 0)
    then throw (MiscRadosException (fromIntegral result))
    else return ()
  retval <- peekArray (fromIntegral result) ptr
  return retval

foreign import ccall "rados_snap_lookup"
  c_radosSnapLookup :: RadosPool -> Ptr CChar -> Ptr RadosSnap -> IO CInt
radosSnapLookup :: RadosPool -> String -> IO RadosSnap
radosSnapLookup pool name = do
  strptr <- newCString name
  retptr <- (malloc :: (IO (Ptr RadosSnap)))
  result <- c_radosSnapLookup pool strptr retptr
  retval <- peek retptr
  free strptr
  free retptr
  wrapErrorCode (return retval) result

foreign import ccall "rados_snap_get_name"
  c_radosSnapGetName :: RadosPool -> RadosSnap -> Ptr CChar -> CInt -> IO CInt
radosSnapGetName :: RadosPool -> RadosSnap -> Int -> IO String
radosSnapGetName pool snap max = do
  strptr <- ((mallocArray max) :: IO (Ptr CChar))
  result <- c_radosSnapGetName pool snap strptr (fromIntegral max) 
  -- If this is one of those rude libraries, strptr may not be null terminated, test later TODO
  wrapErrorCode (peekCString strptr) result

-- Object Listing
foreign import ccall "rados_list_objects_open" c_radosListObjectsOpen :: RadosPool -> Ptr RadosListCtx -> IO CInt
radosListObjectsOpen :: RadosPool -> IO RadosListCtx
radosListObjectsOpen pool = do
  ctxPtr <- (malloc :: IO (Ptr RadosListCtx))
  result <- c_radosListObjectsOpen pool ctxPtr
  retval <- peek ctxPtr
  free ctxPtr
  wrapErrorCode (return retval) result

foreign import ccall "rados_list_objects_next" c_radosListObjectsNext :: RadosListCtx -> Ptr (Ptr CChar) -> IO CInt
radosListObjectsNext :: RadosListCtx -> IO (Maybe String)
radosListObjectsNext ctx = do
  strPtrPtr <- (malloc :: IO (Ptr (Ptr CChar)))
  result <- c_radosListObjectsNext ctx strPtrPtr
  strPtr <- peek strPtrPtr
  free strPtrPtr
  case result of
    0 -> do
      retval <- peekCString strPtr
      return $ Just retval
    -2 -> return Nothing
    _ -> throw (MiscRadosException (fromIntegral result))

foreign import ccall "rados_list_objects_close" c_radosListObjectsClose :: RadosListCtx -> IO CInt
radosListObjectsClose :: RadosListCtx -> IO ()
radosListObjectsClose ctx = c_radosListObjectsClose ctx >>= wrapErrorCode (return ())

genToList :: [IO (Maybe a)] -> IO [a]
genToList [] = return []
genToList (x:xs) = do
  mx <- x
  case mx of
    Nothing -> return []
    Just ax -> do
      axs <- genToList xs
      return (ax : axs)

radosListObjects :: RadosPool -> IO [String]
radosListObjects pool = do
  ctx <- radosListObjectsOpen pool
  retval <- genToList (inf ctx)
  radosListObjectsClose ctx
  return retval
    where
      inf ctx = (radosListObjectsNext ctx) : (inf ctx)
  

-- Sync IO
foreign import ccall "rados_write" 
  c_radosWrite :: RadosPool -> Ptr CChar -> CInt -> Ptr CChar -> CInt -> IO CInt
foreign import ccall "rados_write_full" 
  c_radosWriteFull :: RadosPool -> Ptr CChar -> CInt -> Ptr CChar -> CInt -> IO CInt
foreign import ccall "rados_read" 
  c_radosRead :: RadosPool -> Ptr CChar -> CInt -> Ptr CChar -> CInt -> IO CInt

_wrapper func pool oid offset size array = withStorableArray array (\ptr -> do
  strptr <- newCString oid
  result <- func pool strptr (fromIntegral offset) ptr (fromIntegral size)
  wrapErrorCode (return (fromIntegral result)) result)
_wrapper_string func pool oid contents = do
  oidptr <- newCString oid
  contptr <- newCString contents
  result <- func pool oidptr 0 contptr (fromIntegral $ (length contents) + 1)
  wrapErrorCode (return (fromIntegral result)) result
  

radosWrite :: RadosPool -> String -> Int -> Int -> (StorableArray Int CChar) -> IO Int
radosWrite = _wrapper c_radosWrite
radosWriteString :: RadosPool -> String -> String -> IO Int
radosWriteString = _wrapper_string c_radosWrite
radosWriteFull :: RadosPool -> String -> Int -> Int -> (StorableArray Int CChar) -> IO Int
radosWriteFull = _wrapper c_radosWriteFull
radosWriteFullString :: RadosPool -> String -> String -> IO Int
radosWriteFullString = _wrapper_string c_radosWriteFull
radosRead :: RadosPool -> String -> Int -> Int -> (StorableArray Int CChar) -> IO Int
radosRead = _wrapper c_radosRead
radosReadString :: RadosPool -> String -> Int -> IO String
radosReadString pool oid max = do
  strptr <- ((mallocArray max) :: IO (Ptr CChar))
  oidptr <- newCString oid
  result <- c_radosRead pool oidptr 0 strptr (fromIntegral max)
  if (result >= 0)
    then do
      retval <- peekCString strptr
      free strptr >> free oidptr
      return retval
    else do
      free strptr
      free oidptr
      throw (MiscRadosException (fromIntegral result))

foreign import ccall "rados_remove" c_radosRemove :: RadosPool -> Ptr CChar -> IO CInt
radosRemove :: RadosPool -> String -> IO ()
radosRemove  = _wrap c_radosRemove

foreign import ccall "rados_trunc" c_radosTrunc :: RadosPool -> Ptr CChar -> CInt -> IO CInt
radosTrunc :: RadosPool -> String -> Int -> IO ()
radosTrunc pool oid size = do
  strptr <- newCString oid
  result <- c_radosTrunc pool strptr (fromIntegral size)
  free strptr
  wrapErrorCode (return ()) result

