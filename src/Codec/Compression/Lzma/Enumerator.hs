module Codec.Compression.Lzma.Enumerator
  ( compress
  , decompress
  ) where

import qualified Data.ByteString as B
import qualified Data.ByteString.Unsafe as B
import Control.Monad (forM_, liftM)
import Control.Monad.Trans
import qualified Data.Enumerator as E
import qualified Data.Enumerator.List as EL
import Foreign
import Foreign.C.Types (CSize)
import Bindings.Lzma

prettyRet :: C'lzma_ret -> String
prettyRet r
  | r == c'LZMA_OK                 = "Operation completed successfully"
  | r == c'LZMA_STREAM_END         = "End of stream was reached"
  | r == c'LZMA_NO_CHECK           = "Input stream has no integrity check"
  | r == c'LZMA_UNSUPPORTED_CHECK  = "Cannot calculate the integrity check"
  | r == c'LZMA_GET_CHECK          = "Integrity check type is now available"
  | r == c'LZMA_MEM_ERROR          = "Cannot allocate memory"
  | r == c'LZMA_MEMLIMIT_ERROR     = "Memory usage limit was reached"
  | r == c'LZMA_FORMAT_ERROR       = "File format not recognized"
  | r == c'LZMA_OPTIONS_ERROR      = "Invalid or unsupported options"
  | r == c'LZMA_DATA_ERROR         = "Data is corrupt"
  | r == c'LZMA_BUF_ERROR          = "No progress is possible"
  | r == c'LZMA_PROG_ERROR         = "Programming error"
  | otherwise                      = "Unknown LZMA error: "++show r

bufferSize :: Num a => a
bufferSize = 4096

memset :: Storable a => Ptr a -> Word8 -> IO ()
memset ptr val = memset' ptr undefined where
  memset' :: Storable a => Ptr a -> a -> IO ()
  memset' _ a = 
    forM_ [0..sizeOf a - 1] $ \ i ->
      pokeByteOff ptr i val

initStream :: String -> (Ptr C'lzma_stream -> IO C'lzma_ret) -> IO (Ptr C'lzma_stream)
initStream name fun = do
  buffer <- mallocBytes bufferSize
  streamPtr <- malloc
  memset streamPtr 0
  poke streamPtr C'lzma_stream
    { c'lzma_stream'next_in   = nullPtr
    , c'lzma_stream'avail_in  = 0
    , c'lzma_stream'total_in  = 0
    , c'lzma_stream'next_out  = buffer
    , c'lzma_stream'avail_out = bufferSize 
    , c'lzma_stream'total_out = 0 }
  ret <- fun streamPtr
  if ret == c'LZMA_OK
    then return streamPtr
    else fail $ name ++ " failed: " ++ prettyRet ret

easyEncoder :: Maybe Int -> Ptr C'lzma_stream -> IO C'lzma_ret
easyEncoder level ptr = c'lzma_easy_encoder ptr (maybe c'LZMA_PRESET_DEFAULT fromIntegral level) c'LZMA_CHECK_CRC64

autoDecoder :: Maybe Word64 -> Ptr C'lzma_stream -> IO C'lzma_ret
autoDecoder memlimit ptr = c'lzma_auto_decoder ptr (maybe maxBound fromIntegral memlimit) 0

-- | Compress a 'B.ByteString' 'E.Enumerator' into an xz container stream.
compress :: MonadIO m
         => Maybe Int -- ^ Compression level from [0..9], defaults to 6.
         -> E.Enumeratee B.ByteString B.ByteString m b
compress level step = do
  streamPtr <- E.tryIO $ initStream "lzma_easy_encoder" (easyEncoder level)
  codeEnum streamPtr step

-- | Decompress a 'B.ByteString' 'E.Enumerator' from an lzma or xz container stream.
decompress :: MonadIO m
           => Maybe Word64 -- ^ Memory limit, in bytes.
           -> E.Enumeratee B.ByteString B.ByteString m b
decompress memlimit step = do
  streamPtr <- E.tryIO $ initStream "lzma_auto_decoder" (autoDecoder memlimit)
  codeEnum streamPtr step

codeEnum :: MonadIO m
         => Ptr C'lzma_stream
         -> E.Enumeratee B.ByteString B.ByteString m b
codeEnum streamPtr (E.Continue k) = do
  availOut <- E.tryIO $ peekAvailOut streamPtr
  chunks <- if availOut < bufferSize
    -- always try to drain the output buffer before reading any data
    then E.tryIO $ buildChunk streamPtr availOut
    else do
      chunk <- EL.head
      E.tryIO $ case chunk of
        -- pin the bytestring and make sure it's been fully
        -- loaded into the native lzma_stream
        Just chunk' -> do
          chunks <- B.unsafeUseAsCStringLen chunk' $ \ (ptr, len) -> do
            pokeNextIn streamPtr ptr
            pokeAvailIn streamPtr $ fromIntegral len
            buildChunks streamPtr c'LZMA_RUN c'LZMA_OK
          -- sanity check, make sure it's actually been read
          availIn <- peekAvailIn streamPtr
          if availIn /= 0
            then fail $ "c'lzma_stream'avail_in == " ++ show availIn ++ ", should be 0" 
            else return chunks
        -- once we're at the end of the enumerator flush the lzma_stream
        Nothing -> do
            pokeNextIn streamPtr nullPtr
            pokeAvailIn streamPtr 0
            buildChunks streamPtr c'LZMA_FINISH c'LZMA_OK
  step <- lift $ E.runIteratee (k chunks)
  codeEnum streamPtr step

codeEnum streamPtr step = do
  E.tryIO $ do
    free =<< peekNextOut streamPtr
    pokeNextOut streamPtr nullPtr
    c'lzma_end streamPtr
    free streamPtr
  return step

buildChunks :: Ptr C'lzma_stream
            -> C'lzma_action
            -> C'lzma_ret
            -> IO (E.Stream B.ByteString)
buildChunks streamPtr action status = do
  availIn <- peekAvailIn streamPtr
  availOut <- peekAvailOut streamPtr
  codeStep streamPtr action status availIn availOut

codeStep :: Ptr C'lzma_stream
         -> C'lzma_action
         -> C'lzma_ret
         -> CSize
         -> CSize
         -> IO (E.Stream B.ByteString)
codeStep streamPtr action status availIn availOut
  -- the inner enumerator has finished and we're done flushing the coder
  | availOut == bufferSize && status == c'LZMA_STREAM_END =
      return E.EOF

  -- the normal case, we have some results..
  | availOut < bufferSize = do
      x <- getChunk streamPtr availOut
      if availIn == 0 && action /= c'LZMA_FINISH -- no more input, stop processing
        then return $ E.Chunks [x]
        else do
          -- run lzma_code forward just far enough to read all the input buffer
          xs <- buildChunks streamPtr action status
          case xs of
            E.Chunks xs' -> return $ E.Chunks (x:xs')
            E.EOF        -> return $ E.Chunks [x]

  -- the input buffer points into a pinned bytestring, so we need to make sure it's been
  -- fully loaded (availIn == 0) before returning
  | availIn > 0 || action == c'LZMA_FINISH = do
      ret <- c'lzma_code streamPtr action
      if ret == c'LZMA_OK || ret == c'LZMA_STREAM_END
        then buildChunks streamPtr action ret
        else fail $ "lzma_code failed: " ++ prettyRet ret

  -- nothing to do here 
  | otherwise = return $ E.Chunks []

buildChunk :: Ptr C'lzma_stream
           -> CSize
           -> IO (E.Stream B.ByteString)
buildChunk streamPtr availOut =
  liftM E.Chunks $ if availOut < bufferSize
    then fmap (:[]) $ getChunk streamPtr availOut
    else return []

getChunk :: Ptr C'lzma_stream
         -> CSize
         -> IO B.ByteString
getChunk streamPtr availOut
  | availOut < bufferSize = do
      nextOut <- peekNextOut streamPtr
      let avail = bufferSize - fromIntegral availOut
          baseBuffer = nextOut `plusPtr` (-avail)
      bs <- B.packCStringLen (baseBuffer, avail)
      pokeAvailOut streamPtr bufferSize
      -- B.pack* copies the buffer, so reuse it
      pokeNextOut streamPtr baseBuffer
      return bs
  | otherwise = return B.empty

