{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
-- | This test suite for the `python-pickle` package uses Python to dump
-- various pickled objects to a temporary file, and for each dump checks
-- that `Language.Python.Pickle` can unpickle the object, and repickle it
-- exactly as the original dump.
module Main (main) where

import Control.Arrow ((&&&))
import Control.Monad (when)
import qualified Data.ByteString as S
import Data.ByteString.Builder (toLazyByteString)
import Data.ByteString.Lazy (toStrict)
import qualified Data.ByteString.Char8 as C
import qualified Data.Text as T
import Data.String (IsString)
import qualified Data.Map as M
import System.Directory (removeFile)
import System.Process (rawSystem)
import Test.HUnit (assertEqual, assertFailure)
import Test.Framework (defaultMain, testGroup, Test)
import Test.Framework.Providers.HUnit (testCase)
import Test.Framework.Providers.QuickCheck2
import Test.QuickCheck
import Test.Feat
import Test.Feat.Modifiers (nat, Infinite)
import Numeric.Natural
import Language.Python.Pickle

deriveEnumerable ''OpCode
instance Enumerable S.ByteString where
  enumerate = S.pack <$> enumerate
instance Enumerable T.Text where
  enumerate = T.pack <$> enumerate
instance Infinite Natural where
instance Enumerable Natural where
  enumerate = nat <$> enumerate
instance Arbitrary OpCode where
  arbitrary = sized uniform `suchThat` noFunnyBusiness
    where
      noFunnyBusiness (GLOBAL s1 s2) = C.notElem '\n' s1 && C.notElem '\n' s2
      noFunnyBusiness (INST s1 s2) = C.notElem '\n' s1 && C.notElem '\n' s2
      noFunnyBusiness (PERSID s) = C.notElem '\n' s
      noFunnyBusiness _ = True
main :: IO ()

main = defaultMain tests

prop_OpsRoundtrip (NonEmpty x) = parse (toStrict . toLazyByteString . mconcat $ map serialize x) == Right x

tests :: [Test]
tests =
  [ testGroup "PickledValues protocol 0" $
      map (\(a, b) -> testCase a $ testAgainstPython 0 b a) expressions
  , testGroup "PickledValues protocol 1" $
      map (\(a, b) -> testCase a $ testAgainstPython 1 b a) expressions
  , testGroup "PickledValues protocol 2" $
      map (\(a, b) -> testCase a $ testAgainstPython 2 b a) expressions
  , testGroup "Roundtrip ops" [testProperty "Ops roundtrip"  prop_OpsRoundtrip]
  ]

-- The round-tripping (unpickling/pickling and comparing the original
-- with the result) is not enough. For instance incorrectly casting ints to
-- ints can be hidden (the unpickled value is incorrect but when pickled again, it
-- is the same as the original).
-- So we can provide an expected value.
testAgainstPython :: Int -> Value -> String -> IO ()
testAgainstPython protocol expected s = do
  let filename = "python-pickle-test-pickled-values.pickle"
  _ <- rawSystem "./tests/pickle-dump.py" ["--output", filename,
    "--protocol", show protocol, "--", s]
  content <- S.readFile filename
  let value = unpickle content

  case value of
    Left err -> assertFailure $ "Can't unpickle " ++ s
      ++ show content ++ ".\nUnpickling error:\n  " ++ err
    Right v -> do
      assertEqual "Unpickled value against expected value" expected v
      when (protocol == 2) $
        assertEqual "Pickled value against original file content"
          content (pickle v)
  removeFile filename

expressions :: [(String, Value)]
expressions =
  [ ("{}", PyDict M.empty)
  , ("{'type': 'cache-query'}",
      PyDict $ M.fromList [(PyString "type", PyString "cache-query")])
  , ("{'type': 'cache-query', 'metric': 'some.metric.name'}",
      PyDict $ M.fromList [(PyString "type", PyString "cache-query"),
        (PyString "metric", PyString "some.metric.name")])

  , ("[]", List [])
  , ("[1]", List [PyInt 1])
  , ("[1, 2]", List [PyInt 1, PyInt 2])
  , ("[True, 0, 1, False]", List [PyBool True, PyInt 0, PyInt 1, PyBool False])
  , ("()", Tuple [])
  , ("(1,)", Tuple [PyInt 1])
  , ("(1, 2)", Tuple [PyInt 1, PyInt 2])
  , ("(1, 2, 3)", Tuple [PyInt 1, PyInt 2, PyInt 3])
  , ("(1, 2, 3, 4)", Tuple [PyInt 1, PyInt 2, PyInt 3, PyInt 4])
  , ("((), [], [3, 4], {})",
    Tuple [Tuple [], List [], List [PyInt 3, PyInt 4], PyDict M.empty])

  , ("None", None)
  , ("True", PyBool True)
  , ("False", PyBool False)

  , ("{'datapoints': [(1, 2)]}",
      PyDict $ M.fromList [(PyString "datapoints",
        List [Tuple [PyInt 1, PyInt 2]])])
  , ("{'datapoints': [(1, 2)], (2,): 'foo'}",
        PyDict $ M.fromList
          [ ( PyString "datapoints"
            , List [Tuple [PyInt 1, PyInt 2]])
          , ( Tuple [PyInt 2]
            , PyString "foo")
          ])
  , ("[(1, 2)]", List [Tuple [PyInt 1, PyInt 2]])
  , ("('twice', 'twice')", Tuple [PyString "twice", PyString "twice"])
  ]
  ++ map (show &&& PyInt) ints
  ++ map (show &&& PyFloat) doubles
  ++ map (quote . C.unpack &&& PyString) strings
  ++ map ((++ "L") . show &&& PyLong . toInteger) ints

ints :: [Int]
ints =
  [0, 10..100] ++ [100, 150..1000] ++
  [1000, 1500..10000] ++ [10000, 50000..1000000] ++
  map negate ([1,126,127,128,129] ++ [10000, 50000..1000000])

doubles :: [Double]
doubles =
  [0.1, 10.1..100] ++
  map negate [0.1, 10.1..100]

strings :: [C.ByteString]
strings =
  ["cache-query"]

quote :: IsString [a] => [a] -> [a]
quote s = concat ["'", s, "'"]

