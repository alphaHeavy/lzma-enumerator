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
import Debug.Trace (trace)
import Foreign
import Foreign.C.String
import Foreign.C.Types
import Foreign.Storable
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

easyEncoder :: Ptr C'lzma_stream -> IO C'lzma_ret
easyEncoder ptr = c'lzma_easy_encoder ptr c'LZMA_PRESET_DEFAULT c'LZMA_CHECK_CRC64

autoDecoder :: Maybe Word64 -> Ptr C'lzma_stream -> IO C'lzma_ret
autoDecoder memlimit ptr = c'lzma_auto_decoder ptr (maybe maxBound fromIntegral memlimit) 0

compress :: MonadIO m
         => E.Enumeratee B.ByteString B.ByteString m b
compress step = do
  streamPtr <- E.tryIO $ initStream "lzma_easy_encoder" easyEncoder
  codeEnum streamPtr step

decompress :: MonadIO m
           => Maybe Word64 
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
    Nothing -> do
      availIn <- peekAvailIn streamPtr
      availOut <- peekAvailOut streamPtr
      if availIn > 0 || availOut < bufferSize
        then buildChunks streamPtr c'LZMA_FINISH
        else return E.EOF
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
buildChunks streamPtr action = liftM E.Chunks go where
  go = do
    ret <- c'lzma_code streamPtr action 
    if ret /= c'LZMA_OK && ret /= c'LZMA_STREAM_END
      then fail $ "c'lzma_code failed: " ++ prettyRet ret
      else do
        availIn <- peekAvailIn streamPtr
        availOut <- peekAvailOut streamPtr
        case () of
          _ | availIn > 0 && availOut > 0 -> go
            | (availIn > 0 && availOut == 0) ||
              (action == c'LZMA_FINISH && availOut < bufferSize) -> do
                chunk <- getOutChunk streamPtr
                tail <- go
                return $ chunk:tail
            | otherwise -> return []

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

