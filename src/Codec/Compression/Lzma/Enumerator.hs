module Codec.Compression.Lzma.Enumerator
  ( compress
  , decompress
  ) where

import qualified Data.ByteString as B
import qualified Data.ByteString.Unsafe as B
import Control.Monad
import Control.Monad.Trans
import qualified Data.Enumerator as E
import qualified Data.Enumerator.List as EL
import Foreign
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

initStream :: String -> (Ptr C'lzma_stream -> IO C'lzma_ret) -> IO (Ptr C'lzma_stream)
initStream name fun = do
  buffer <- mallocBytes bufferSize
  streamPtr <- new C'lzma_stream
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
  chunk <- EL.head
  chunks <- E.tryIO $ case chunk of
    Just chunk' ->
      B.unsafeUseAsCStringLen chunk' $ \ (ptr, len) -> do
        pokeNextIn streamPtr ptr
        pokeAvailIn streamPtr $ fromIntegral len
        buildChunks streamPtr c'LZMA_RUN
    Nothing -> buildChunks streamPtr c'LZMA_FINISH
  step <- lift $ E.runIteratee (k chunks)
  codeEnum streamPtr step

codeEnum streamPtr step = do
  E.tryIO $ do
    free =<< peekNextOut streamPtr
    c'lzma_end streamPtr
  return step

buildChunks :: Ptr C'lzma_stream
            -> C'lzma_action
            -> IO (E.Stream B.ByteString)
buildChunks streamPtr action = go where
  go = do
    ret <- c'lzma_code streamPtr action 
    if ret /= c'LZMA_OK && ret /= c'LZMA_STREAM_END
      then fail $ "c'lzma_code failed: " ++ prettyRet ret
      else do
        availIn <- peekAvailIn streamPtr
        availOut <- peekAvailOut streamPtr
        case () of
          -- the normal case, the coder has provided some data...
          -- it'd be more efficient to return a single buffer but this is easier to implement.
          _ | availOut < bufferSize -> do
                x <- getOutChunk streamPtr
                xs <- go
                return $ E.Chunks $ case xs of
                  E.Chunks xs' -> x:xs'
                  E.EOF        -> x:[] 
          -- the inner enumerator has finished, we need to flush the results out of the lzma_stream
            | action == c'LZMA_FINISH -> if ret /= c'LZMA_STREAM_END
                                           then go
                                           else return E.EOF
          -- the input buffer points into a pinned bytestring, so we need to make sure it's been
          -- fully loaded (availIn == 0) before returning
            | availIn > 0 -> go
          -- filling the lzma_stream buffer, nothing to return yet
            | otherwise -> return $ E.Chunks []

getOutChunk :: Ptr C'lzma_stream
            -> IO B.ByteString
getOutChunk streamPtr = do
  availOut <- peekAvailOut streamPtr
  if availOut < bufferSize
    then do
      nextOut <- peekNextOut streamPtr
      let avail = bufferSize - fromIntegral availOut
          baseBuffer = nextOut `plusPtr` (-avail)
      bs <- B.packCStringLen (baseBuffer, avail)
      pokeAvailOut streamPtr bufferSize
      -- B.pack* copies the buffer, so reuse it
      pokeNextOut streamPtr baseBuffer
      return bs
    else return B.empty

