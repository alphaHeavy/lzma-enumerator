import Test.Framework (defaultMain, testGroup)
import Test.Framework.Providers.HUnit
import Test.Framework.Providers.QuickCheck2 (testProperty)
import Test.QuickCheck
import Test.QuickCheck.Monadic

import qualified Data.ByteString as B
import qualified Data.Enumerator as E
import qualified Data.Enumerator.List as EL
import Data.List
import Data.Word
import Codec.Compression.Lzma.Enumerator

main = defaultMain tests

tests =
  [ testGroup "Compress" compressTests
  , testGroup "Decompress" decompressTests
  , testGroup "Chained" chainedTests
  ]

compressTests =
  [ testProperty "compressAndDiscard" prop_compressAndDiscard
  , testProperty "compressAndCheckLength" prop_compressAndCheckLength
  ]

decompressTests =
  [ testProperty "decompressRandom" prop_decompressRandom
  , testProperty "decompressCorrupt" prop_decompressCorrupt
  ]

chainedTests =
  [ testProperty "chain" prop_chain
  , testProperty "compressThenDecompress" prop_compressThenDecompress
  ]

someString :: Gen B.ByteString
someString = do
  val <- listOf $ elements [0..255::Word8] 
  return $ B.pack val

someBigString :: Gen B.ByteString
someBigString = resize (8*1024) someString

dropAll :: Monad m => E.Iteratee a m ()
dropAll = EL.dropWhile (const True)

prop_compressAndDiscard :: Property
prop_compressAndDiscard = monadicIO $ forAllM someBigString $ \ str -> do
  run $ E.run_ $ E.enumList 2 [str] E.$$ E.joinI (compress Nothing E.$$ dropAll)

prop_compressAndCheckLength :: Property
prop_compressAndCheckLength = monadicIO $ forAllM someBigString $ \ str -> do
  len <- run $ E.run_ $ E.enumList 2 [str] E.$$ E.joinI (compress Nothing E.$$ EL.fold (\ acc el -> acc + B.length el) 0)
  assert (len - 32 > B.length str `div` 2) -- random strings don't compress very well

prop_chain :: Property
prop_chain = monadicIO $ forAllM someBigString $ \ str -> do
  str' <- run $ E.run_ $ E.enumList 2 [str] E.$$ E.joinI (compress Nothing E.$$ E.joinI (decompress Nothing E.$$ EL.consume))
  return $ str == B.concat str'

prop_compressThenDecompress :: Property
prop_compressThenDecompress = monadicIO $ forAllM someBigString $ \ str -> do
  blob <- run $ E.run_ $ E.enumList 2 [str] E.$$ E.joinI (compress Nothing E.$$ EL.consume)
  let blob' = B.concat blob
  randIdx <- pick $ elements [0..B.length blob'-1]
  let resplit = let (x,y) = B.splitAt randIdx blob' in [x,y]
  str' <- run $ E.run_ $ E.enumList 2 resplit E.$$ E.joinI (decompress Nothing E.$$ EL.consume)
  return $ str == B.concat str'

prop_decompressRandom :: Property
prop_decompressRandom = monadicIO $ forAllM someBigString $ \ str -> do
  header <- run $ E.run_ $ E.enumList 2 [] E.$$ E.joinI (compress Nothing E.$$ EL.consume)
  let blob = header ++ [str]
  run $ E.run_ $ E.enumList 2 blob E.$$ E.joinI (decompress Nothing E.$$ dropAll)

prop_decompressCorrupt :: Property
prop_decompressCorrupt = expectFailure $ monadicIO $ forAllM someBigString $ \ str -> do
  header <- run $ E.run_ $ E.enumList 2 [] E.$$ E.joinI (compress Nothing E.$$ EL.consume)
  let header' = B.concat header
  randVal <- pick $ elements [0..255::Word8]
  randIdx <- pick $ elements [0..B.length header'-1]
  let (left, right) = B.splitAt randIdx header'
      updated = left `B.append` (randVal `B.cons` B.tail right)
      blob = [updated, str]
  run $ E.run_ $ E.enumList 2 blob E.$$ E.joinI (decompress Nothing E.$$ dropAll)

